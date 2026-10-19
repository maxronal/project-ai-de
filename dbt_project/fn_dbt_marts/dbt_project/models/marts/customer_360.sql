with customers as (
    select * from {{ ref('dim_customer') }}
),
transactions as (
    select * from {{ ref('int_customer_transactions') }}
)
select
    c.customer_id,
    c.city,
    c.segment,
    c.signup_date,
    count(distinct t.sale_id) as total_orders,
    coalesce(sum(t.quantity), 0) as total_units_purchased,
    coalesce(sum(t.total_line), 0) as total_spent,
    coalesce(round(avg(t.total_line), 2), 0) as avg_order_value,
    min(t.sale_timestamp) as first_purchase_at,
    max(t.sale_timestamp) as last_purchase_at
from customers c
left join transactions t
    on c.customer_id = t.customer_id
group by
    c.customer_id,
    c.city,
    c.segment,
    c.signup_date
