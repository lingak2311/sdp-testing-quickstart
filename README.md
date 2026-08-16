# SDP unit-testing quick-start

A small retail orders pipeline that shows how to test Lakeflow Spark Declarative
Pipelines (SDP): unit tests for transform logic, an integration test for AUTO CDC
SCD Type 2, and tests that assert expectations fire as declared.

Companion to the blog. All APIs verified against public docs on 2026-08-16.

## Layout

```
setup/
  00_setup                    run this first: seeds data and creates the pipeline
  golden-prompt.md            optional: build the pipeline with Genie Code instead
pipeline/
  transformations.py          the pipeline: raw -> clean -> customers_history (SCD2) -> curated
  tests/
    test_transformations.py   unit tests, AUTO CDC integration test, expectations tests
```

## What you need

Unit testing is Beta. Before you run:

1. Owner permission to create a pipeline, plus `USE CATALOG` and `CREATE SCHEMA`
   on a catalog you can write to.
2. Serverless compute available in your workspace.

The setup notebook handles the PREVIEW channel, triggered mode, and the catalog
and schema for you. You only supply a catalog name.

## How to run

Do NOT use the "Create ETL pipeline" wizard. It always starts a blank project and
will not adopt the files you pulled from Git. The setup notebook creates the
pipeline for you, pointed at this repo's code.

1. Pull this repo into your workspace as a Git folder.
2. Open `setup/00_setup` and run it top to bottom. In the `catalog` widget, enter
   a catalog you can write to. The notebook works out the repo location, creates
   the schema, seeds the source tables, and creates the `sdp-unit-testing`
   pipeline pointed at `pipeline/transformations.py`.
3. The last cell prints a link to the pipeline. Open it.
4. Open `pipeline/tests/test_transformations.py` and click **Run file** (not "Run
   pipeline"). The nine tests appear in the results panel, pass or fail per
   assertion. Run a single test with the play button in its gutter.

The pipeline sources only `pipeline/transformations.py`. The test file, the setup
notebook, and these docs are not part of the pipeline, which is what you want.

To run the pipeline itself against the seeded data, click **Run pipeline**, then
explore `orders_curated` and `customers_history`.

## What each test proves

| Test | Layer | Proves |
|------|-------|--------|
| happy-path row count | unit | valid rows survive the transform |
| computes line total | unit | quantity times unit price is correct |
| null price yields null total | unit | null handling, not a silent zero |
| drops rows missing keys | unit | rows with no order or customer id are gone |
| output schema | unit | columns and shape are as declared |
| SCD2 current state | integration | out-of-order events resolve to the latest by sequence |
| SCD2 full history | integration | every version is kept |
| expectation drops row | expectations | the drop policy actually removes a bad row |
| expectation fails update | expectations | the fail policy stops the run |

Test runs execute on the pipeline compute and are billed as normal pipeline
updates.
