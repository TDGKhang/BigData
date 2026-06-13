select
    order_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    -- Extract YYYYMMDD date keys
    cast(to_char(order_purchase_timestamp, 'YYYYMMDD') as integer) as purchase_date_key,
    cast(to_char(order_approved_at, 'YYYYMMDD') as integer) as approved_date_key,
    cast(to_char(order_delivered_carrier_date, 'YYYYMMDD') as integer) as delivered_carrier_date_key,
    cast(to_char(order_delivered_customer_date, 'YYYYMMDD') as integer) as delivered_customer_date_key,
    cast(to_char(order_estimated_delivery_date, 'YYYYMMDD') as integer) as estimated_delivery_date_key
from {{ ref('stg_orders') }}