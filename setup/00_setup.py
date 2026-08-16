# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SDP unit-testing demo: one-click setup
# MAGIC
# MAGIC Creates the schema and seeds the two source tables the pipeline reads:
# MAGIC `orders_source` (bronze input) and `customers_cdf` (change events for AUTO CDC).
# MAGIC
# MAGIC You provide a catalog you can write to. The schema is fixed to
# MAGIC `demo_sdp_unit_testing` so the pipeline and test files work unchanged.
# MAGIC
# MAGIC Run all cells, then create the pipeline pointing at `transformations.py`.
# MAGIC The tests seed their own data, so they do not need these tables. These are
# MAGIC for running the pipeline end to end and exploring the gold table.

# COMMAND ----------

dbutils.widgets.text("catalog", "default", "Catalog (must be writable)")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = "demo_sdp_unit_testing"
FQ = f"{CATALOG}.{SCHEMA}"

assert CATALOG, "Set a catalog name in the widget above."
print(f"Target: {FQ}")

# COMMAND ----------

# MAGIC %md ## 1. Create the schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")
print(f"Schema ready: {FQ}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Seed the bronze source: orders_source
# MAGIC
# MAGIC A realistic spread with the edge cases the transform and expectations handle:
# MAGIC valid rows, a zero quantity, a null price, and a null customer id.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {FQ}.orders_source AS
SELECT * FROM VALUES
    (1001, 'C1', 2,  50.00),
    (1002, 'C1', 0,  25.00),
    (1003, 'C2', 3,  NULL),
    (1004, NULL, 1,  10.00),
    (1005, 'C2', 1,  19.99),
    (1006, 'C3', 4,  12.50),
    (1007, 'C3', 2,   8.00),
    (1008, 'C1', 5,   3.20),
    (1009, 'C4', 1, 249.00),
    (1010, 'C4', 3,  45.00),
    (1011, 'C2', 2,  15.00),
    (1012, 'C5', 1,  99.99),
    (1013, 'C5', 0,  60.00),
    (1014, 'C3', 6,   5.00),
    (1015, 'C1', 2,  22.75)
AS t(order_id, customer_id, quantity, unit_price)
""")
print(f"Seeded {FQ}.orders_source")
display(spark.table(f"{FQ}.orders_source"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Seed the CDC source: customers_cdf
# MAGIC
# MAGIC Change events for AUTO CDC. Note C1: three events arriving out of order
# MAGIC (sequence 1, then 3, then a late 2). The pipeline must resolve the current
# MAGIC state to sequence 3 and keep the full SCD2 history.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {FQ}.customers_cdf AS
SELECT * FROM VALUES
    ('C1', 'Silver',   'London',     'INSERT', 1),
    ('C1', 'Gold',     'Manchester', 'UPDATE', 3),
    ('C1', 'Silver',   'Birmingham', 'UPDATE', 2),
    ('C2', 'Silver',   'Leeds',      'INSERT', 1),
    ('C2', 'Gold',     'Leeds',      'UPDATE', 2),
    ('C3', 'Bronze',   'Bristol',    'INSERT', 1),
    ('C4', 'Platinum', 'Edinburgh',  'INSERT', 1),
    ('C5', 'Silver',   'Cardiff',    'INSERT', 1),
    ('C5', 'Silver',   'Cardiff',    'DELETE', 2)
AS t(customer_id, tier, city, operation, sequence_num)
""")
print(f"Seeded {FQ}.customers_cdf")
display(spark.table(f"{FQ}.customers_cdf"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data is ready. Now create the pipeline end to end.
# MAGIC
# MAGIC The steps below have a few non-obvious points. Follow them in order.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Create the pipeline and set its defaults
# MAGIC
# MAGIC Create an ETL pipeline. Its **default catalog** and **default schema**
# MAGIC decide where every output table lands, so set them explicitly:
# MAGIC
# MAGIC - Default catalog: the catalog you used in the widget above.
# MAGIC - Default schema: `demo_sdp_unit_testing`.
# MAGIC
# MAGIC If you leave the schema as `default`, the pipeline writes its tables there
# MAGIC instead, and the gold table will not sit beside your seeded sources.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Set the channel to PREVIEW and mode to triggered
# MAGIC
# MAGIC Unit testing is Beta, so the pipeline must be on the **PREVIEW** channel in
# MAGIC **triggered** mode. The Settings UI does not always expose the channel, so
# MAGIC verify it in the JSON view: open Pipeline settings, switch the toggle from
# MAGIC **UI** to **JSON**, and confirm these fields:
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "sdp-unit-testing",
# MAGIC   "channel": "PREVIEW",
# MAGIC   "continuous": false,
# MAGIC   "serverless": true,
# MAGIC   "catalog": "<your catalog>",
# MAGIC   "schema": "demo_sdp_unit_testing",
# MAGIC   "root_path": "/Workspace/Users/<you>/sdp-unit-testing",
# MAGIC   "libraries": [
# MAGIC     { "glob": { "include": "/Workspace/Users/<you>/sdp-unit-testing/pipeline/transformations.py" } }
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC `"channel": "PREVIEW"` and `"continuous": false` are the two that gate unit
# MAGIC testing. If `channel` reads `CURRENT` or is absent, set it to `PREVIEW`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Link ONLY the pipeline source, not the tests
# MAGIC
# MAGIC In the editor file tree every file starts unlinked (a broken-chain icon).
# MAGIC You link exactly one file as pipeline source code:
# MAGIC
# MAGIC - `pipeline/transformations.py`  ->  right-click, **Include as pipeline source code**.
# MAGIC
# MAGIC Leave everything else unlinked:
# MAGIC
# MAGIC - `pipeline/tests/test_transformations.py`  ->  stays unlinked. It runs in the
# MAGIC   test harness, not the pipeline graph. Linking it makes SDP try to evaluate
# MAGIC   the test functions as pipeline definitions, which fails.
# MAGIC - `setup/00_setup`, `setup/golden-prompt.md`, `README.md`  ->  stay unlinked.
# MAGIC
# MAGIC A note on the `libraries` glob: point it at the single file
# MAGIC `pipeline/transformations.py`, not at `pipeline/**`. A folder glob would sweep
# MAGIC in `pipeline/tests/` and pull the test file into the pipeline, the exact thing
# MAGIC step 6 is avoiding.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Run the tests
# MAGIC
# MAGIC Open `pipeline/tests/test_transformations.py` and click **Run file** (not
# MAGIC "Run pipeline"). The nine tests appear in the results panel with pass or fail
# MAGIC per assertion. Run a single test with the play button in its gutter.
# MAGIC
# MAGIC The tests seed their own mock data, so they do not read the tables this
# MAGIC notebook created. Those seeded tables are for running the pipeline itself
# MAGIC (Run pipeline) and exploring the gold table and the SCD2 history.