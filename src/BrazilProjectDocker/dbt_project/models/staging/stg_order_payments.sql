with source as (
    select *,
        row_number() over (
            partition by order_id, payment_sequential 
            order by payment_value desc
        ) as rn
    from {{ source('raw', 'payments') }}
)
select
    order_id,
    cast(payment_sequential as integer) as payment_sequential,
    payment_type,
    cast(payment_installments as integer) as payment_installments,
    cast(payment_value as numeric) as payment_value
from source
where rn = 1