--clean and prepare raw comic in big query

{{ config(
    materialized='ephemeral',
    description='Staging model, temp table'
) }}

with source as (
    select num, title, safe_title, transcript, publish_date, fetched_at, year, month
    from {{source('xkcd', 'raw_comics')}}
),

cleaned as (
    select 
    num as comic_id, 
    title, 
    safe_title,
    transcript, 
    publish_date, 
    fetched_at, 
    year, 
    month,
    --calculate cost
    (length(regexp_replace(title, '[^a-zA-Z]', ''))*5) as creation_cost,
    --Calculate random views 
    cast(floor(rand() * 10000) as int64) as views,
    --calculate random review
    round(1 + rand() * 9, 1) as review
    from source
)

select * from cleaned
