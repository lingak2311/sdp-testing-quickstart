# SDP unit-testing quick-start

A small retail orders pipeline that shows how to test Lakeflow Spark Declarative
Pipelines (SDP): unit tests for transform logic, an integration test for AUTO CDC
SCD Type 2, and tests that assert expectations fire as declared.

This is a fail-first lesson. The pipeline ships with a deliberate bug. You run it,
watch it go green, then find the bug is producing wrong results and fix it through
the tests. The bug is real and realistic, not a trick, and finding it is the point.

Companion to the blog. All APIs verified against public docs on 2026-08-16.

## Layout

```
setup/
  00_setup                    run this first: seeds data and creates the pipeline
  01_tdd_walkthrough          the lesson: find the bug with tests, then fix it
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
   a catalog you can write to. The notebook creates the schema, seeds the source
   tables, and creates the `sdp-unit-testing` pipeline pointed at
   `pipeline/transformations.py`.
3. The last cell prints a link to the pipeline. Open it and click **Run pipeline**.
   It succeeds.
4. Open `setup/01_tdd_walkthrough` and follow it. You will see the gold table is
   wrong, run the tests, watch one go red, fix the one-line bug, and re-run to
   green.

The pipeline sources only `pipeline/transformations.py`. The test file, the setup
notebooks, and these docs are not part of the pipeline, which is what you want.

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
| curated current-tier attribution | gold | orders join only the current SCD2 row, not every version |
| expectation drops row | expectations | the drop policy actually removes a bad row |

Test runs execute on the pipeline compute and are billed as normal pipeline
updates.
