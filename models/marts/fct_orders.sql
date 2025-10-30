with orders as (

    select * from {{ ref('stg_jaffe_shop__orders') }}

),

payments as (
    select * from {{ ref('stg_stripe__payments') }} 
),

customer_payments as (
    select 
    orders.order_id,
    customer_id,
        sum(sum_amount) as payment
        
    from orders
    left join payments using (order_id)
    group by all
)

select * from customer_payments