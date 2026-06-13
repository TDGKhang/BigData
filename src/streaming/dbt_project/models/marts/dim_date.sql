select
    cast(to_char(datum, 'YYYYMMDD') as integer) as date_key,
    datum as date_actual,
    to_char(datum, 'Day') as day_name,
    extract(isodow from datum) as day_of_week,
    extract(day from datum) as day_of_month,
    extract(doy from datum) as day_of_year,
    extract(week from datum) as week_of_year,
    extract(month from datum) as month_actual,
    to_char(datum, 'Month') as month_name,
    extract(quarter from datum) as quarter_actual,
    extract(year from datum) as year_actual
from generate_series(
    '2016-01-01'::date,
    '2030-12-31'::date,
    '1 day'::interval
) as datum