WITH src_user_variant AS (
    SELECT * FROM {{ source("jhjmo0719h", "variant") }}
)
SELECT
    user_id,
    variant_id
FROM
    src_user_variant
