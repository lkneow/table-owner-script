WITH source AS (
    
    {#/*
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    */#}
    SELECT
      * 
    FROM {{ ref('raw_products_info') }}

),

renamed AS (
    SELECT
        *
    FROM source
)

SELECT
  * 
FROM renamed