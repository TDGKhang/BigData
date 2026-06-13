select
    md5(concat(coalesce(cast(order_id as varchar), ''), '-', coalesce(cast(payment_sequential as varchar), ''))) as payment_key,
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
from {{ ref('stg_order_payments') }}