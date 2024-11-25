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

SELECT * FROM src_reviews
WHERE review_text is not null

{# this - makes reference to this model #}
{# select all records that have review data greater than max review date from 'this' model #}
{% if is_incremental() %}
    AND review_date > (select max(review_date) from {{ this }})
{% endif %}