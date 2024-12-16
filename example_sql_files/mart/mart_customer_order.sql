{{ config(materialized='table') }}

SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    o.order_id,
    o.order_date,
    o.order_status
FROM {{ ref('core_customers') }} AS c
JOIN {{ ref('core_orders') }} AS o ON c.customer_id = o.customer_id