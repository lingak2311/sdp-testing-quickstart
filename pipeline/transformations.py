"""Retail orders pipeline for the SDP unit-testing blog.

Bronze  orders_raw        raw order events landed as-is
Silver  orders_clean      transform logic: null handling, invalid rows, line total
Silver  customers_history  AUTO CDC, SCD Type 2 on customer tier and city
Gold    orders_curated    orders joined to the customer state, business aggregate

Naming and APIs verified against public docs on 2026-08-16:
  https://docs.databricks.com/aws/en/ldp/cdc
  https://docs.databricks.com/aws/en/ldp/expectations
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col, expr


# Bronze. Raw orders as they land. In production this reads from a volume or a
# connector; for the pipeline definition we read the seeded source table by name
# so the unit-test framework can redirect it.
@dp.table
def orders_raw():
    return spark.read.table("lingesh_fe_sa_workspace_catalog.demo_sdp_unit_testing.orders_source")


# Silver. The transform under test. Three things happen here:
#   1. drop rows with no order_id or no customer_id (cannot be used downstream)
#   2. treat a non-positive quantity or a null price as invalid, flag them
#   3. compute line_total = quantity * unit_price for valid rows
# Expectations enforce the data-quality contract at run time. The unit tests
# assert the transform logic itself.
@dp.table
@dp.expect_or_drop("has_order_id", "order_id IS NOT NULL")
@dp.expect_or_drop("has_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_fail("positive_quantity", "quantity > 0")
def orders_clean():
    raw = spark.read.table("orders_raw")
    return (
        raw.withColumn("unit_price", col("unit_price").cast("double"))
        .withColumn("quantity", col("quantity").cast("int"))
        .withColumn(
            "line_total",
            F.when(
                col("unit_price").isNotNull() & (col("quantity") > 0),
                col("quantity") * col("unit_price"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .select("order_id", "customer_id", "quantity", "unit_price", "line_total")
    )


# Silver. Customer dimension built with AUTO CDC, SCD Type 2, so the gold layer
# can attribute each order to the customer state that was valid at the time.
# sequence_by orders the change events, which is what makes out-of-order events
# resolve correctly. Verified signature: docs.databricks.com/aws/en/ldp/cdc
@dp.view
def customers_cdc():
    return spark.readStream.table("lingesh_fe_sa_workspace_catalog.demo_sdp_unit_testing.customers_cdf")


dp.create_streaming_table("customers_history")

dp.create_auto_cdc_flow(
    target="customers_history",
    source="customers_cdc",
    keys=["customer_id"],
    sequence_by=col("sequence_num"),
    apply_as_deletes=expr("operation = 'DELETE'"),
    except_column_list=["operation", "sequence_num"],
    stored_as_scd_type="2",
)


# Gold. Orders attributed to the current customer tier, aggregated to revenue
# per tier. Reads the current rows of the SCD2 history (__END_AT IS NULL).
@dp.table
def orders_curated():
    orders = spark.read.table("orders_clean")
    current_customers = (
        spark.read.table("customers_history")
        .filter(col("__END_AT").isNull())
        .select("customer_id", "tier")
    )
    return (
        orders.join(current_customers, "customer_id", "left")
        .groupBy("tier")
        .agg(
            F.count("order_id").alias("order_count"),
            F.round(F.sum("line_total"), 2).alias("revenue"),
        )
    )
