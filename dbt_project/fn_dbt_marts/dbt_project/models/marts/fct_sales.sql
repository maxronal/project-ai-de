with sales as (
    select * from {{ ref('stg_sales') }}
)
select
    sale_id,
    customer_id,
    product_id,
    sale_timestamp,
    quantity,
    unit_price,
    total_line,
    ingested_at,
    ingestion_date
from sales
