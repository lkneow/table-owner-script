{{ config(materialized='table') }}

SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.price
FROM {{ ref('stg_products') }} AS p
LEFT JOIN {{ ref('stg_product_info') }} AS p_i
       ON p.product_id = p_i.id