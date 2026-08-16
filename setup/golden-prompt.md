# Golden prompt: build the pipeline with Genie Code

The blog walks through this pipeline by hand. This is the other path: paste the
prompt below into Genie Code (the Generate button in the Lakeflow Editor, or
agent mode) and it scaffolds the same pipeline end to end.

Genie Code output is non-deterministic, so treat this as a strong starting point,
not a guarantee of identical output. The manual walkthrough in the blog is the
source of truth. Review what it generates against the four tables described below.

## Prompt

```
Build a Lakeflow Spark Declarative Pipelines (SDP) pipeline in Python for a
retail orders example, using the medallion pattern. Use `from pyspark import
pipelines as dp`. Read source tables by name so the unit-testing framework can
redirect them. Create four assets:

1. Bronze table `orders_raw`: read the table
   `<CATALOG>.demo_sdp_unit_testing.orders_source` as-is. Columns: order_id,
   customer_id, quantity, unit_price.

2. Silver table `orders_clean` from `orders_raw`. Cast unit_price to double and
   quantity to int. Compute line_total = quantity * unit_price, but only when
   unit_price is not null and quantity > 0, otherwise null. Select order_id,
   customer_id, quantity, unit_price, line_total. Add expectations: drop rows
   where order_id is null, drop rows where customer_id is null, and fail the
   update when quantity is not greater than 0.

3. A customer dimension using AUTO CDC, SCD Type 2. Create a streaming view
   `customers_cdc` that reads `<CATALOG>.demo_sdp_unit_testing.customers_cdf` as
   a stream. Create a streaming table `customers_history`. Create an AUTO CDC
   flow into `customers_history` from `customers_cdc`, keyed on customer_id,
   sequenced by sequence_num, applying deletes when operation = 'DELETE',
   excluding operation and sequence_num from the target, stored as SCD type 2.

4. Gold table `orders_curated`: join `orders_clean` to the current rows of
   `customers_history` (where __END_AT is null) on customer_id, then aggregate
   to order_count and revenue (rounded sum of line_total) per tier.

Replace <CATALOG> with the catalog I give you. Use the exact AUTO CDC function
`dp.create_auto_cdc_flow` and expectation decorators `@dp.expect_or_drop` and
`@dp.expect_or_fail`.
```

## After it generates

- Check it produced the four assets above.
- Confirm it used `dp.create_auto_cdc_flow` with `stored_as_scd_type="2"` and
  `sequence_by`, not a hand-rolled merge.
- Confirm the expectations are the drop/drop/fail set, not all warn.
- Then add the test file (Add, then Test) and run the tests as in the blog.
