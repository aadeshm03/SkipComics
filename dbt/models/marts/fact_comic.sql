--model for comic performance metric 

{{config(
    materialized = 'table'
)}}

with staging as (
    select * from {{ref('staging')}}
),

fact_table as (
    select 
    
    --foreign keys to dimensions
    s.comic_id,
    s.publish_date,

    --metrics
    s.views as view_count,
    s.review as average_review,

    case 
        when d.creation_cost > 0 
        then round(s.views / d.creation_cost, 2)
        else 0
    end as cost_to_views_ratio
    from staging s
    join {{ ref('dim_comic') }} d on s.comic_id = d.comic_id
)

select * from fact_table
