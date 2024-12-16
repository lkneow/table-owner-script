WITH source AS (
    
    {#/*
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    */#}
    SELECT
      * 
    FROM {{ ref('raw_products') }}

),

renamed AS (
    SELECT
        id AS product_id,
        order_id,
        product_price
    FROM source
)

SELECT
  * 
FROM renamed