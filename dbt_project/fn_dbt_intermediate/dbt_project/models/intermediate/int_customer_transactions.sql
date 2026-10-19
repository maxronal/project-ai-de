with sales as (
    select
        sale_id,
        customer_id,
        product_id,
        sale_timestamp,
        quantity,
        unit_price,
        total_line,
        ingestion_date
    from {{ ref('stg_sales') }}
),

customers as (
    select
        customer_id,
        city,
        segment,
        signup_date
    from {{ ref('stg_customers') }}
)

select
    s.sale_id,
    s.customer_id,
    c.city,
    c.segment,
    s.product_id,
    s.sale_timestamp,
    s.quantity,
    s.unit_price,
    s.total_line,
    s.ingestion_date,
    date_diff(date(s.sale_timestamp), c.signup_date, day) as days_since_signup
from sales s
inner join customers c
    on s.customer_id = c.customer_id
