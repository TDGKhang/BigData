with grouped_geo as (
    select
        geolocation_zip_code_prefix,
        avg(geolocation_lat) as geolocation_lat,
        avg(geolocation_lng) as geolocation_lng,
        -- Take the first non-null city and state values for simplicity
        min(geolocation_city) as geolocation_city,
        min(geolocation_state) as geolocation_state
    from {{ ref('stg_geolocation') }}
    group by geolocation_zip_code_prefix
)
select
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state
from grouped_geo