with products as (
    select * from {{ ref('stg_products') }}
)
select
    product_id,
    category,
    brand,
    description,
    price,
    ingested_at as updated_at
from products
