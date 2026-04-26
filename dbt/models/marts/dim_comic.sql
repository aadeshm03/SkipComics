--dimenion table model with comic details and attributes

{{config(
    materialized = 'table'
)}}

with staging as (
    select * from {{ref('staging')}}
)   

select 
    comic_id, 
    title,
    safe_title,
    transcript,
    creation_cost
from staging
