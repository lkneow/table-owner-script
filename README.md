# Table Owner Script

Simple version of a script I wrote for my job where the idea is to get all affected tables and their owners of a change in an upstream table.

For example, an upstream table changes logic and we want to make an announcement on slack to notify all affected personnel.

# The idea behind the script

Essentially looking through all files under the `/models` folder and looking for the given table

Also needs to be simple to use with no installation since most end users are not tech-savvy. So no installation or virtual environment required.

# Context

The intention is to parse through a folder of dbt models which looks somewhat similar to the following

Each model is in its own .sql file, and each model has its own .yml file that shows the owner of the table

```
dbt_project
├── models
│   ├── stg
│   │   ├── stg_customers.sql
│   │   ├── stg_customerrs.yml
│   │   ├── stg_orders.sql
│   │   ├── stg_orders.yml
│   ├── core
│   │   ├── core_customers.sql
│   │   ├── core_customers.yml
│   │   ├── core_orders.sql
│   │   ├── core_orders.yml
│   │   ├── core_products.sql
│   │   ├── core_products.yml
│   └── mart
│       ├── mart_customer_orders.sql
│       ├── mart_customer_orders.yml
│       ├── mart_product_orders.sql
│       ├── mart_product_orders.yml
├── dbt_project.yml
└── profiles.yml
```

Example sql file

```sql
WITH source AS (

    {#/*
    Normally we would SELECT FROM the table here, but we are using seeds to load
    our data in this project
    */#}
    SELECT 
      * 
    FROM {{ ref('raw_customers') }}

),

renamed AS (

    SELECT
        id AS customer_id,
        first_name,
        lASt_name

    FROM source

)

SELECT 
  * 
FROM renamed
```

Example yml file

```yaml
version: 2
models:
  - name: stg_customers
    description: example stg customers
    meta:
      owner:
        slack_group: teamA-on-slack
        user_email: personA@companyemail.com

```

## How to use
- run `python downstream_user_script.py -p <some_path> -s <table_name> -m <max_depth>`
    - `<path_to_datahub-airflow>`: Where the `/models` folder is located
    - `table_name`: Enter the name of the table for which you need to identify downstream tables and owners 
    - `max_depth`: Depth of search. 1 means every table that uses `table_name`. Default is 100 to cover all downstream.

An example

```
python downstream_user_script.py -p ./example_sql_files -s stg_customers -m 2
```

## Output

- Two files will be made in `./downstream_jsons` with the following files names
    - `downstream_<table_name>_depth<n>_datetime.json`
    - `downstream_<table_name>_depth<n>_datetime_custom.json`

- A basic json output that looks like the following. The same thing will be printed out
```json
{
    "@personB": [
        "core_email_example"
    ],
    "@teamB-on-slack": [
        "core_customers"
    ],
    "@teamC-on-slack": [
        "mart_customer_order"
    ]
}
```
- A custom format without quotes. Also printed out in terminal
- custom format looks like so it can be pasted onto slack. The @ ensures the team name gets tagged
```
@personB: [
    core_email_example
]
@teamB-on-slack: [
    core_customers
]
@teamC-on-slack: [
    mart_customer_order
]
```

# Upstream user script

Same as downstream user script

An example

```
python upstream_user_script.py -p ./example_sql_files -s mart_customer_order -m 2
```

Which would give the following

- Two files in `./upstream_jsons` with the following files names
    - `upstream_<table_name>_depth<n>_datetime.json`
    - `upstream_<table_name>_depth<n>_datetime_custom.json`

```json
{
    "@teamB-on-slack": [
        "core_customers",
        "core_orders"
    ],
    "@teamA-on-slack": [
        "stg_customers",
        "stg_orders",
        "stg_payments"
    ]
}
```

```
@teamB-on-slack: [
    core_customers,
    core_orders
]
@teamA-on-slack: [
    stg_customers,
    stg_orders,
    stg_payments
]
```

# How I would improve this

- Add tests, though I'm not too sure what to test
- use a proper sql parsing library
- use a proper yml parsing library
- Error statements or a prompt when the output is empty