"""Unit and integration tests for the retail orders pipeline.

Run these from inside the web-based Lakeflow Editor on the PREVIEW channel,
triggered mode, with Owner permission on the pipeline (unit testing is Beta).
Tests are Python even though the pipeline could be SQL. Verified 2026-08-16:
  https://docs.databricks.com/aws/en/ldp/unit-testing

The test_spark fixture redirects any table referenced BY NAME to an isolated
per-run schema, so mocking a source is a plain CREATE TABLE by name.
"""

import pytest
from pyspark.pipelines.testing import TestPipeline, test_spark
from pyspark.testing import assertDataFrameEqual

test_pipeline = TestPipeline.active()


# ---------------------------------------------------------------------------
# Mock builders. Each seeds a source table by name into the isolated schema.
# ---------------------------------------------------------------------------
def mock_orders_source(session):
    session.sql(
        """
        CREATE OR REPLACE TABLE lingesh_fe_sa_workspace_catalog.demo_sdp_unit_testing.orders_source AS
        SELECT * FROM VALUES
            (1001, 'C1', 2,  50.00),
            (1003, 'C2', 3,  NULL),
            (1004, NULL, 1,  10.00)
        AS t(order_id, customer_id, quantity, unit_price)
        """
    )


def mock_customers_cdf(session):
    # Three change events for one customer, deliberately out of sequence:
    # seq 1 creates the row, seq 3 is the latest, seq 2 arrives late.
    session.sql(
        """
        CREATE OR REPLACE TABLE lingesh_fe_sa_workspace_catalog.demo_sdp_unit_testing.customers_cdf AS
        SELECT * FROM VALUES
            ('C1', 'Silver', 'London',     'INSERT', 1),
            ('C1', 'Gold',   'Manchester', 'UPDATE', 3),
            ('C1', 'Silver', 'Birmingham', 'UPDATE', 2)
        AS t(customer_id, tier, city, operation, sequence_num)
        """
    )


def mock_curated_inputs(session):
    # orders_clean: two orders for C1, already through the silver transform.
    session.sql(
        """
        CREATE OR REPLACE TABLE lingesh_fe_sa_workspace_catalog.demo_sdp_unit_testing.orders_clean AS
        SELECT * FROM VALUES
            (5001, 'C1', 2, 50.00, 100.00),
            (5002, 'C1', 1, 30.00,  30.00)
        AS t(order_id, customer_id, quantity, unit_price, line_total)
        """
    )
    # customers_history: one closed SCD2 version and one current version for C1.
    # The current row (Gold) has a null __END_AT. The gold join must pick only it.
    session.sql(
        """
        CREATE OR REPLACE TABLE lingesh_fe_sa_workspace_catalog.demo_sdp_unit_testing.customers_history AS
        SELECT * FROM VALUES
            ('C1', 'Silver', 'London',     TIMESTAMP '2024-01-01', TIMESTAMP '2024-06-01'),
            ('C1', 'Gold',   'Manchester', TIMESTAMP '2024-06-01', CAST(NULL AS TIMESTAMP))
        AS t(customer_id, tier, city, `__START_AT`, `__END_AT`)
        """
    )


# ---------------------------------------------------------------------------
# Unit tests: transform logic in orders_clean. One class of assertion each.
# ---------------------------------------------------------------------------
def test_clean_happy_path_row_count(test_spark):
    # Of the three seeded rows, 1001 and 1003 survive. 1004 is dropped for a
    # null customer_id. There is no quantity-0 row here: expect_or_fail would
    # halt the run, so that case lives only in the fail test below.
    mock_orders_source(test_spark)
    test_pipeline.run(test_spark, {"orders_clean"})
    result = test_spark.table("orders_clean")
    assert result.count() == 2


def test_clean_computes_line_total(test_spark):
    mock_orders_source(test_spark)
    test_pipeline.run(test_spark, {"orders_clean"})
    row = test_spark.table("orders_clean").filter("order_id = 1001").first()
    assert row["line_total"] == 100.0  # 2 * 50.00


def test_clean_null_price_yields_null_total(test_spark):
    # order 1003 has a null unit_price, so line_total must be null, not 0.
    mock_orders_source(test_spark)
    test_pipeline.run(test_spark, {"orders_clean"})
    row = test_spark.table("orders_clean").filter("order_id = 1003").first()
    assert row["line_total"] is None


def test_clean_drops_rows_missing_keys(test_spark):
    # order 1004 has a null customer_id and must not appear.
    mock_orders_source(test_spark)
    test_pipeline.run(test_spark, {"orders_clean"})
    ids = [r["order_id"] for r in test_spark.table("orders_clean").collect()]
    assert 1004 not in ids


def test_clean_output_schema(test_spark):
    mock_orders_source(test_spark)
    test_pipeline.run(test_spark, {"orders_clean"})
    cols = set(test_spark.table("orders_clean").columns)
    assert cols == {"order_id", "customer_id", "quantity", "unit_price", "line_total"}


# ---------------------------------------------------------------------------
# Integration test: AUTO CDC stateful behaviour. This is what a plain transform
# unit test cannot cover, the out-of-order sequencing and SCD2 history.
# ---------------------------------------------------------------------------
def test_cdc_scd2_current_state_is_latest_by_sequence(test_spark):
    # Despite the late-arriving seq 2 event, the current row must reflect seq 3.
    mock_customers_cdf(test_spark)
    test_pipeline.run(test_spark, {"customers_history"})
    current = (
        test_spark.table("customers_history")
        .filter("__END_AT IS NULL AND customer_id = 'C1'")
        .first()
    )
    assert current["tier"] == "Gold"
    assert current["city"] == "Manchester"


def test_cdc_scd2_keeps_full_history(test_spark):
    # SCD2 keeps one row per version. Three change events for C1 means three
    # history rows.
    mock_customers_cdf(test_spark)
    test_pipeline.run(test_spark, {"customers_history"})
    versions = test_spark.table("customers_history").filter("customer_id = 'C1'").count()
    assert versions == 3


# ---------------------------------------------------------------------------
# Expectations: assert the declared data-quality policy actually fires.
# ---------------------------------------------------------------------------
def test_expectation_drops_row_missing_order_id(test_spark):
    # A row with a null order_id must be dropped by @dp.expect_or_drop, so a
    # clean input of one valid and one null-order_id row yields a single row.
    test_spark.sql(
        """
        CREATE OR REPLACE TABLE lingesh_fe_sa_workspace_catalog.demo_sdp_unit_testing.orders_source AS
        SELECT * FROM VALUES
            (2001, 'C9', 1, 5.00),
            (NULL,  'C9', 1, 5.00)
        AS t(order_id, customer_id, quantity, unit_price)
        """
    )
    test_pipeline.run(test_spark, {"orders_clean"})
    assert test_spark.table("orders_clean").count() == 1


def test_expectation_fails_update_on_non_positive_quantity(test_spark):
    # quantity 0 violates @dp.expect_or_fail("positive_quantity", ...), so the
    # update must fail rather than pass with bad data.
    test_spark.sql(
        """
        CREATE OR REPLACE TABLE lingesh_fe_sa_workspace_catalog.demo_sdp_unit_testing.orders_source AS
        SELECT * FROM VALUES (3001, 'C9', 0, 5.00)
        AS t(order_id, customer_id, quantity, unit_price)
        """
    )
    with pytest.raises(Exception):
        test_pipeline.run(test_spark, {"orders_clean"})


# ---------------------------------------------------------------------------
# Gold: orders_curated must attribute each order to the CURRENT customer tier,
# the SCD2 row with a null __END_AT. This is the bug the blog centres on.
# ---------------------------------------------------------------------------
def test_curated_attributes_to_current_tier(test_spark):
    mock_curated_inputs(test_spark)
    test_pipeline.run(test_spark, {"orders_curated"})
    rows = {r["tier"]: r for r in test_spark.table("orders_curated").collect()}
    # Only the current tier appears. A missing __END_AT filter would fan the
    # join across the closed Silver version too, so both tiers would show up.
    assert set(rows) == {"Gold"}
    assert rows["Gold"]["order_count"] == 2
    assert rows["Gold"]["revenue"] == 130.00
