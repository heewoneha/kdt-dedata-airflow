WITH um AS (
    SELECT * FROM {{ ref("dim_user_metadata") }}
), uv AS (
    SELECT * FROM {{ ref("dim_user_variant") }}
)
SELECT
    uv.user_id,
    uv.variant_id,
    um.age,
    um.gender
FROM uv
LEFT JOIN um ON uv.user_id = um.user_id
