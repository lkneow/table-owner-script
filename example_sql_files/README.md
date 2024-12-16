# Example sql files

These are just example sql files to show how the script works. Most of them are taken from [dbt's jaffle shop example](https://github.com/dbt-labs/jaffle_shop_duckdb/tree/duckdb).
Edited slightly. No expectation for the sql to work, basically just need a few `table.sql` and `table.yml` files with them referencing one another

## Lineage
lineage looks something like below.

```mermaid
flowchart LR
  stg_customers --> core_email_example
  stg_customers --> core_customers
  stg_orders --> core_customers
  stg_orders --> core_orders
  stg_payments --> core_customers
  stg_payments --> core_orders
  stg_product_info --> core_products
  stg_products --> core_products
  core_customers --> mart_customer_order
  core_orders --> mart_customer_order
  core_products --> mart_product_order
  core_orders --> mart_product_order
```