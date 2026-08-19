# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SDP unit-testing quickstart: one setup notebook
# MAGIC
# MAGIC You pulled this repo into your workspace as a Git folder. Run this notebook
# MAGIC top to bottom and it does everything for you:
# MAGIC
# MAGIC 1. works out where the repo lives (no hardcoded paths)
# MAGIC 2. creates the schema
# MAGIC 3. seeds the two source tables the pipeline reads
# MAGIC 4. creates (or updates) the SDP pipeline, pointed at the repo code
# MAGIC
# MAGIC You provide one thing: a catalog you can write to. Everything else is derived.
# MAGIC
# MAGIC The pipeline is created for you here on purpose. The "Create ETL pipeline"
# MAGIC wizard always starts a blank project and will not adopt the files you pulled
# MAGIC from Git, so we create the pipeline through the SDK instead and point it
# MAGIC straight at `pipeline/transformations.py`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Pick your catalog
# MAGIC
# MAGIC Set the catalog in the widget that appears at the top of the notebook after
# MAGIC you run this cell. It must be a catalog you can create schemas and tables in.
# MAGIC The schema is fixed to `demo_sdp_unit_testing` so the pipeline code, which
# MAGIC reads tables by name, works without edits.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog (must be writable)")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = "demo_sdp_unit_testing"

# COMMAND ----------

assert CATALOG, "Set a catalog in the widget at the top, then run this cell again."

FQ = f"{CATALOG}.{SCHEMA}"
print(f"Catalog: {CATALOG}")
print(f"Schema:  {SCHEMA}")
print(f"Target:  {FQ}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Work out where this repo lives
# MAGIC
# MAGIC This notebook sits at `<repo>/setup/00_setup`. We read the notebook's own
# MAGIC workspace path at runtime and step up two folders to get the repo root, so
# MAGIC this works in anyone's workspace with no path edits. From the root we build
# MAGIC the path to the pipeline source file.

# COMMAND ----------

notebook_path = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
# notebook_path is like /Workspace/Users/<you>/sdp-testing-quickstart/setup/00_setup
setup_dir = notebook_path.rsplit("/", 1)[0]      # .../sdp-testing-quickstart/setup
repo_root = setup_dir.rsplit("/", 1)[0]          # .../sdp-testing-quickstart

# The pipeline sources ONE file. Pointing at the single file, not the folder,
# is what keeps the test file and this notebook out of the pipeline.
pipeline_source = f"{repo_root}/pipeline/transformations.py"

print(f"Notebook path:   {notebook_path}")
print(f"Repo root:       {repo_root}")
print(f"Pipeline source: {pipeline_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create the schema
# MAGIC
# MAGIC Idempotent. Safe to run more than once.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")
print(f"Schema ready: {FQ}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Seed the bronze source, orders_source
# MAGIC
# MAGIC Fifteen orders with the edge cases the transform and expectations handle:
# MAGIC valid rows, a null price (line total stays null, not a silent zero), and a
# MAGIC null customer id (dropped by the expectation). Every quantity is positive so
# MAGIC the run is green: a non-positive quantity would trip the fail expectation and
# MAGIC halt the update. This table is here so you can run the pipeline for real
# MAGIC and see the gold table.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {FQ}.orders_source AS
SELECT * FROM VALUES
    (1001, 'C1', 2,  50.00),
    (1002, 'C1', 2,  25.00),
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
    (1013, 'C5', 1,  60.00),
    (1014, 'C3', 6,   5.00),
    (1015, 'C1', 2,  22.75)
AS t(order_id, customer_id, quantity, unit_price)
""")
print(f"Seeded {FQ}.orders_source")
display(spark.table(f"{FQ}.orders_source"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Seed the CDC source, customers_cdf
# MAGIC
# MAGIC Change events for AUTO CDC. Look at customer C1: three events arriving out of
# MAGIC order (sequence 1, then 3, then a late 2). The pipeline must resolve the
# MAGIC current state to sequence 3 (Gold, Manchester) and keep the full SCD Type 2
# MAGIC history.

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
# MAGIC ## Step 6: Create or update the SDP pipeline
# MAGIC
# MAGIC We build the pipeline spec in code and create it through the SDK. Key settings,
# MAGIC each of which matters for unit testing:
# MAGIC
# MAGIC - `channel = "PREVIEW"`: unit testing is Beta, only on PREVIEW.
# MAGIC - `continuous = False`: triggered mode is required.
# MAGIC - `serverless = True`, `photon = True`: serverless compute.
# MAGIC - `catalog` and `schema`: your catalog and the fixed demo schema, so output
# MAGIC   tables land beside the sources you just seeded.
# MAGIC - `libraries`: a single-file glob at `pipeline/transformations.py`. This is
# MAGIC   deliberate. It sources ONLY the pipeline definition, so the test file, this
# MAGIC   setup notebook, the README, and the golden prompt are all excluded from the
# MAGIC   pipeline graph. A folder glob (`pipeline/**`) would wrongly pull the test
# MAGIC   file in.
# MAGIC
# MAGIC The cell is idempotent: if a pipeline named `sdp-unit-testing` already exists
# MAGIC it is updated in place, otherwise it is created.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineLibrary, PathPattern

w = WorkspaceClient()

PIPELINE_NAME = "sdp-unit-testing"

libraries = [PipelineLibrary(glob=PathPattern(include=pipeline_source))]

# Is there already a pipeline with this name? If so, update it rather than
# create a duplicate.
existing_id = None
for p in w.pipelines.list_pipelines():
    if p.name == PIPELINE_NAME:
        existing_id = p.pipeline_id
        break

if existing_id:
    w.pipelines.update(
        pipeline_id=existing_id,
        name=PIPELINE_NAME,
        catalog=CATALOG,
        schema=SCHEMA,
        channel="PREVIEW",
        continuous=False,
        serverless=True,
        photon=True,
        root_path=repo_root,
        libraries=libraries,
    )
    pipeline_id = existing_id
    print(f"Updated existing pipeline {PIPELINE_NAME}: {pipeline_id}")
else:
    created = w.pipelines.create(
        name=PIPELINE_NAME,
        catalog=CATALOG,
        schema=SCHEMA,
        channel="PREVIEW",
        continuous=False,
        serverless=True,
        photon=True,
        root_path=repo_root,
        libraries=libraries,
    )
    pipeline_id = created.pipeline_id
    print(f"Created pipeline {PIPELINE_NAME}: {pipeline_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Open the pipeline and confirm

# COMMAND ----------

host = spark.conf.get("spark.databricks.workspaceUrl")
print("Pipeline is ready. Open it here:")
print(f"https://{host}/pipelines/{pipeline_id}")
print()
print("What you should see:")
print("  - source file: pipeline/transformations.py (only that file)")
print("  - channel PREVIEW, triggered mode, serverless")
print(f"  - default catalog {CATALOG}, schema {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Run the tests, then run the pipeline
# MAGIC
# MAGIC Do these in order. Steps 3 and 4 are both required before you open the
# MAGIC walkthrough notebook.
# MAGIC
# MAGIC 1. In the editor file tree, confirm `pipeline/transformations.py` is the
# MAGIC    source. The test file, this notebook, and the docs are not part of the
# MAGIC    pipeline, which is correct.
# MAGIC 2. Open `pipeline/tests/test_transformations.py`.
# MAGIC 3. Click **Run Tests** (not "Run pipeline"). Nine tests appear in the results
# MAGIC    panel. Eight pass and one fails: `test_curated_attributes_to_current_tier`.
# MAGIC    That failure is expected. It is the deliberate bug you find and fix in the
# MAGIC    walkthrough. Run a single test with the play button in its gutter.
# MAGIC 4. Now click **Run pipeline** (required). This materializes the tables,
# MAGIC    including `orders_curated`, so the walkthrough can read them. Skip this and
# MAGIC    the next notebook fails on "table or view not found". When it finishes you
# MAGIC    can explore `orders_curated` (revenue by tier) and `customers_history` (the
# MAGIC    SCD Type 2 history).
# MAGIC 5. Open `setup/01_tdd_walkthrough` and follow it. That is where the lesson is.
