select
    i.order_id,
    i.order_item_id,
    o.customer_id,
    i.product_id,
    i.seller_id,
    r.review_id,
    r.review_score,
    i.price,
    i.freight_value,
    p.payment_type,
    p.payment_installments,
    p.payment_value
from {{ ref('stg_order_items') }} i
join {{ ref('stg_orders') }} o
    on i.order_id = o.order_id
left join {{ ref('stg_order_payments') }} p
    on i.order_id = p.order_id and p.payment_sequential = 1
left join {{ ref('stg_order_reviews') }} r
    on i.order_id = r.order_id