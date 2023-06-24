WITH src_user_variant AS (
    SELECT * FROM {{ ref('src_user_variant') }}
)
SELECT
    user_id,
    variant_id
FROM
    src_user_variant
