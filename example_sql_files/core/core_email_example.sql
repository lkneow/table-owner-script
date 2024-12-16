{{ config(materialized='table') }}

WITH customers AS (

    SELECT 
        *
    FROM {{ ref('stg_customers') }}

)

SELECT 
  *
FROM customers