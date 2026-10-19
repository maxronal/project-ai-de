with customers as (
    select * from {{ ref('stg_customers') }}
)
select
    customer_id,
    signup_date,
    city,
    segment,
    ingested_at as updated_at
from customers
