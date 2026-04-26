--dimenion table for time attributes like day of week, publish date

{{config(
    materialized = 'table'
)}}

with staging as (
    select * from {{ref('staging')}}
    where publish_date is not null
)

select 
    publish_date,
    extract(dayofweek from publish_date) as day_of_week, --can be used for analysis of which day of the week has more views
    year,
    month
from staging
order by publish_date desc
