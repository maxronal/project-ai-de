with source as (
    select * from {{ source('raw_dataset', 'customers_raw') }}
)
select
    customer_id,
    safe_cast(signup_date as date) as signup_date,
    trim(city) as city,
    trim(segment) as segment,
    ingested_at
from source