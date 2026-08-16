# SDP unit-testing quick-start

A small retail orders pipeline that shows how to test Lakeflow Spark Declarative
Pipelines (SDP): unit tests for transform logic, an integration test for AUTO CDC
SCD Type 2, and tests that assert expectations fire as declared.

Companion to the blog. All APIs verified against public docs on 2026-08-16.

## Layout

```
pipeline/
  transformations.py          the pipeline: raw -> clean -> customers_history (SCD2) -> curated
  tests/
    test_transformations.py   unit tests, AUTO CDC integration test, expectations tests
```

## What you need

Unit testing is Beta. Before you run:

1. The pipeline is on the **PREVIEW** channel.
2. The pipeline runs in **triggered** mode (not continuous).
3. You have **Owner** permission on the pipeline, plus `USE CATALOG` and
   `CREATE SCHEMA` on the default catalog.
4. You are in the web-based **Lakeflow Editor**. Tests run there only, not
   locally and not over Spark Connect.

The pipeline and tests target `lingesh_fe_sa_workspace_catalog.demo_sdp_unit_testing`.
Create that schema once before the first run, or point the references at another
schema you can write to inside `lingesh_fe_sa_workspace_catalog`.

## How to run

1. Create a pipeline, set its default catalog and schema.
2. Add `transformations.py` as a transformation file.
3. Add `tests/test_transformations.py` as a test file (the editor: Add, then Test).
4. Run the whole test file, or a single test with the play button in the gutter.
   Results show in the editor panel, pass or fail per assertion.

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
