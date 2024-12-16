{{ config(materialized='table') }}

SELECT
    *
FROM {{ ref('core_products') }} AS p
JOIN {{ ref('core_orders') }} AS o 
  ON p.customer_id = o.customer_id