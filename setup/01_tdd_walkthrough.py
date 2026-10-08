# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SDP unit testing: find the bug with tests
# MAGIC
# MAGIC Prerequisite: in `00_setup` you must have done BOTH Run Tests and Run pipeline
# MAGIC (Step 8). Run pipeline materializes `orders_curated`, which this notebook reads.
# MAGIC If you skipped it, Step 1 below fails with "table or view not found": go back to
# MAGIC `00_setup`, click Run pipeline, then return here.
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

# COMMAND ----------

assert CATALOG, "Set the same catalog you used in 00_setup, then run this cell again."

FQ = f"{CATALOG}.{SCHEMA}"
print(f"Target: {FQ}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Look at the gold table
# MAGIC
# MAGIC `orders_curated` aggregates revenue and order count per customer tier. Run the
# MAGIC cell and read the numbers. If it errors with "table or view not found", you
# MAGIC have not run the pipeline yet: go back to `00_setup` Step 8, click Run
# MAGIC pipeline, then run this cell again.

# COMMAND ----------

display(spark.table(f"{FQ}.orders_curated").orderBy("tier"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: The numbers are wrong
# MAGIC
# MAGIC You seeded 15 orders. One has a null customer id that the pipeline drops, so
# MAGIC exactly 14 orders should be attributed. Now add up `order_count` across the
# MAGIC tiers: 3 + 7 + 2 + 13 = 25. The table reports 25 attributed orders against 14
# MAGIC real ones. Orders are counted more than once, and revenue is smeared across
# MAGIC tiers a customer no longer belongs to. The Silver row is the tell: it shows 13
# MAGIC orders, yet no customer is currently Silver. Those orders matched customers
# MAGIC through their old, closed Silver versions.
# MAGIC
# MAGIC Nothing failed. No error, no red. In production this ships, and a dashboard
# MAGIC shows inflated revenue for weeks before anyone notices. This is the failure
# MAGIC mode SDP unit testing exists to catch: not a crash, a quietly wrong result.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: How SDP unit testing works
# MAGIC
# MAGIC The framework runs a subset of your pipeline against data you control, then
# MAGIC lets you assert on the output. Three pieces:
# MAGIC
# MAGIC - `test_spark`: a session fixture that redirects any table referenced BY NAME
# MAGIC   to an isolated per-run schema. That is why the tests can seed a source table
# MAGIC   with a plain `CREATE TABLE` and the pipeline reads the mock, not production.
# MAGIC - `TestPipeline.active()`: a handle to the pipeline being edited.
# MAGIC - `test_pipeline.run(test_spark, {"table_name"})`: runs a selective refresh of
# MAGIC   just the tables you name, synchronously, so you can read the result. It
# MAGIC   returns a status and does not raise when the update fails, so the tests
# MAGIC   check `status.is_success` first, through the `run_chain` helper.
# MAGIC
# MAGIC Tests run only inside this Lakeflow editor, on PREVIEW channel, triggered mode,
# MAGIC with Owner permission. You cannot run them from a notebook. That is why this
# MAGIC notebook sends you to the editor for the next step.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Run the tests and watch one go red
# MAGIC
# MAGIC 1. In the pipeline editor file tree, open `pipeline/tests/test_transformations.py`.
# MAGIC 2. Click **Run Tests** (not Run pipeline).
# MAGIC 3. Watch the results panel. Eight tests pass. One fails:
# MAGIC    `test_curated_attributes_to_current_tier`.
# MAGIC
# MAGIC Read the assertion. The test seeds a customer with two SCD2 versions, a closed
# MAGIC Silver row and a current Gold row, and two orders. It expects both orders to
# MAGIC attribute to Gold only. Instead the output has both Silver and Gold, because
# MAGIC the join matched every version of the customer, not just the current one.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Fix the bug
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
# MAGIC Click **Run Tests** again. All nine tests pass.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Re-run the pipeline and confirm the fix
# MAGIC
# MAGIC Click **Run pipeline**. When it finishes, run the cell below. Add up
# MAGIC `order_count` again: 2 + 3 + 7 + 2 = 14. That matches the 14 real orders
# MAGIC exactly, and revenue attributes to each customer's current tier. The null
# MAGIC tier holds two orders (159.99) from a customer deleted in the CDC stream, so
# MAGIC it has no current tier: those orders are still counted, just not misattributed.
# MAGIC The test that caught the bug now guards against it coming back.

# COMMAND ----------

display(spark.table(f"{FQ}.orders_curated").orderBy("tier"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: The loud failure, on purpose
# MAGIC
# MAGIC The silent bug was one kind of problem. Expectations are the other. SDP has
# MAGIC three policies:
# MAGIC
# MAGIC - warn (`@dp.expect`): record the violation, keep the row.
# MAGIC - drop (`@dp.expect_or_drop`): drop the violating row.
# MAGIC - fail (`@dp.expect_or_fail`): stop the update.
# MAGIC
# MAGIC `orders_clean` uses all three shapes. The test
# MAGIC `test_expectation_drops_row_missing_order_id` proves the drop policy: it seeds a
# MAGIC null-order_id row and asserts the row is gone from the output. The fail policy
# MAGIC halts the whole update, which you see when you Run pipeline on bad data, rather
# MAGIC than as a single unit assertion.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you learned
# MAGIC
# MAGIC - A green pipeline can ship wrong data. Tests are how you catch it.
# MAGIC - `test_spark` redirects tables by name, so mocking a source is one `CREATE TABLE`.
# MAGIC - You can test stateful AUTO CDC and SCD2 logic, not just plain transforms.
# MAGIC - You can assert that a drop expectation fires by checking the bad row is gone.
# MAGIC
# MAGIC The pipeline now has a test guarding the exact bug you fixed. That is the point.
