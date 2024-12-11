{# if there is any change on up stream schema, make this model to fail #}
{{
    config(
        materialized = 'incremental',
        on_schema_changes = 'fail'
    )
}}

WITH src_reviews AS (
    SELECT * FROM {{ ref('scr_reviews') }}
)

SELECT 
{{ dbt_utils.generate_surrogate_key(['listing_id', 'review_date', 'reviewer_name', 'review_text']) }} AS review_id,
* FROM src_reviews
WHERE review_text is not null

{# this - makes reference to this model #}
{# select all records that have review data greater than max review date from 'this' model #}
{% if is_incremental() %}
    {% if var("start_date", False) and var("end_date", False) %}
        {{log('Loading ' ~ this ~ 'incrementally (start_date: ' ~ var("start_date") ~ ', end_date: ' ~ var("end_date"))}}
        AND review_date >= '{{ var("start_date") }}'
        AND review_date < '{{ var("end_date") }}'
    {% else %}  
        AND review_date > (select max(review_date) from {{ this }})
        {{ log('Loading ' ~ this ~ ' incrementally (all missing dates)', info=True)}}
    {% endif %}    
{% endif %}