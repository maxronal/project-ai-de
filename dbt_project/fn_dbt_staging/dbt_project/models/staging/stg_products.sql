with source as (
    select * from {{ source('raw_dataset', 'products_raw') }}
)
select
    product_id,
    trim(category) as category,
    trim(brand) as brand,
    trim(description) as description,
    safe_cast(price as numeric) as price,
    ingested_at
from source