select orderid as order_id,sum(amount) as sum_amount from raw.stripe.payment group by 1
