WITH src_reviews AS (
    SELECT * FROM {{ ref('scr_reviews') }}
)

SELECT * FROM src_reviews
WHERE review_text is not null