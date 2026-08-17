# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SDP unit testing: find the bug with tests
# MAGIC
# MAGIC You ran `00_setup` and the pipeline went green. Every table built, no errors.
# MAGIC A green pipeline feels like a correct pipeline. It is not.
# MAGIC
# MAGIC This notebook is the lesson. The gold table `orders_curated` is quietly wrong.
# MAGIC You will see the wrong numbers, understand why a green run hid them, and use
# MAGIC the unit tests to find and fix the bug. Set the same catalog you used in
# MAGIC `00_setup`.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog (must be writable)")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = "demo_sdp_unit_testing"

assert CATALOG, "Set the same catalog you used in 00_setup, then run this cell again."

FQ = f"{CATALOG}.{SCHEMA}"
print(f"Target: {FQ}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: look at the gold table
# MAGIC
# MAGIC `orders_curated` aggregates revenue and order count per customer tier. Run the
# MAGIC cell and read the numbers.

# COMMAND ----------

display(spark.table(f"{FQ}.orders_curated").orderBy("tier"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: the numbers are wrong
# MAGIC
# MAGIC You seeded 15 orders, and one has a null customer id that the pipeline drops,
# MAGIC so 14 orders should be attributed. Add up `order_count` across the tiers. It
# MAGIC is higher than 14. Some orders are counted more than once, and revenue is
# MAGIC smeared across tiers a customer no longer belongs to.
# MAGIC
# MAGIC Nothing failed. No error, no red. In production this ships, and a dashboard
# MAGIC shows inflated revenue for weeks before anyone notices. This is the failure
# MAGIC mode SDP unit testing exists to catch: not a crash, a quietly wrong result.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: how SDP unit testing works
# MAGIC
# MAGIC The framework runs a subset of your pipeline against data you control, then
# MAGIC lets you assert on the output. Three pieces:
# MAGIC
# MAGIC - `test_spark`: a session fixture that redirects any table referenced BY NAME
# MAGIC   to an isolated per-run schema. That is why the tests can seed a source table
# MAGIC   with a plain `CREATE TABLE` and the pipeline reads the mock, not production.
# MAGIC - `TestPipeline.active()`: a handle to the pipeline being edited.
# MAGIC - `test_pipeline.run(test_spark, {"table_name"})`: runs a selective refresh of
# MAGIC   just the tables you name, synchronously, so you can read the result.
# MAGIC
# MAGIC Tests run only inside this Lakeflow editor, on PREVIEW channel, triggered mode,
# MAGIC with Owner permission. You cannot run them from a notebook. That is why this
# MAGIC notebook sends you to the editor for the next step.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: run the tests and watch one go red
# MAGIC
# MAGIC 1. In the pipeline editor file tree, open `pipeline/tests/test_transformations.py`.
# MAGIC 2. Click **Run file** (not Run pipeline).
# MAGIC 3. Watch the results panel. Nine tests pass. One fails:
# MAGIC    `test_curated_attributes_to_current_tier`.
# MAGIC
# MAGIC Read the assertion. The test seeds a customer with two SCD2 versions, a closed
# MAGIC Silver row and a current Gold row, and two orders. It expects both orders to
# MAGIC attribute to Gold only. Instead the output has both Silver and Gold, because
# MAGIC the join matched every version of the customer, not just the current one.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: fix the bug
# MAGIC
# MAGIC Open `pipeline/transformations.py` and find `orders_curated`. The comment says
# MAGIC it reads the current rows of the SCD2 history, but the code does not filter for
# MAGIC them. In SCD2 the current version of a row is the one whose `__END_AT` is null.
# MAGIC Older versions have an `__END_AT` timestamp. Without the filter, the join fans
# MAGIC out across every version, which is exactly the double-counting you saw.
# MAGIC
# MAGIC Add the filter back:
# MAGIC
# MAGIC ```
# MAGIC current_customers = (
# MAGIC     spark.read.table("customers_history")
# MAGIC     .filter(col("__END_AT").isNull())
# MAGIC     .select("customer_id", "tier")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC Run the test file again. All ten tests pass.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: re-run the pipeline and confirm the fix
# MAGIC
# MAGIC Click **Run pipeline**. When it finishes, run the cell below. The order counts
# MAGIC now sum to 14, revenue attributes to the current tier, and the numbers are
# MAGIC correct. The test that caught the bug now guards against it coming back.

# COMMAND ----------

display(spark.table(f"{FQ}.orders_curated").orderBy("tier"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: the loud failure, on purpose
# MAGIC
# MAGIC The silent bug was one kind of problem. Expectations are the other. SDP has
# MAGIC three policies:
# MAGIC
# MAGIC - warn (`@dp.expect`): record the violation, keep the row.
# MAGIC - drop (`@dp.expect_or_drop`): drop the violating row.
# MAGIC - fail (`@dp.expect_or_fail`): stop the update.
# MAGIC
# MAGIC `orders_clean` uses all three shapes. The test
# MAGIC `test_expectation_fails_update_on_non_positive_quantity` seeds a quantity-0 row
# MAGIC and asserts the run raises, using `pytest.raises`. That is how you prove a fail
# MAGIC policy actually halts, rather than trusting that it does. Look at it in the
# MAGIC test file.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you learned
# MAGIC
# MAGIC - A green pipeline can ship wrong data. Tests are how you catch it.
# MAGIC - `test_spark` redirects tables by name, so mocking a source is one `CREATE TABLE`.
# MAGIC - You can test stateful AUTO CDC and SCD2 logic, not just plain transforms.
# MAGIC - You can assert that an expectation fires, including a fail policy that halts.
# MAGIC
# MAGIC The pipeline now has a test guarding the exact bug you fixed. That is the point.
