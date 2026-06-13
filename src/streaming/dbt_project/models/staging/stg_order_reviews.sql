with source as (
    select *,
        row_number() over (
            partition by review_id 
            order by review_creation_date desc
        ) as rn
    from {{ source('raw', 'reviews') }}
)
select
    review_id,
    order_id,
    cast(review_score as integer) as review_score,
    review_comment_title,
    review_comment_message,
    cast(review_creation_date as timestamp) as review_creation_date,
    cast(review_answer_timestamp as timestamp) as review_answer_timestamp
from source
where rn = 1