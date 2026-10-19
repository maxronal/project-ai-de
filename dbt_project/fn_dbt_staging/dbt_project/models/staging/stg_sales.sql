with source as (
    select * from {{ source('silver_dataset', 'sales_clean') }}
)
select
    sale_id, customer_id, product_id, sale_timestamp,
    quantity, unit_price, total_line, ingested_at, ingestion_date
from source