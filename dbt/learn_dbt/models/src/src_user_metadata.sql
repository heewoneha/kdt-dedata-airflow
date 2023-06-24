WITH src_user_metadata AS (
    SELECT * FROM {{ source("jhjmo0719h", "metadata") }}
)
SELECT
    user_id,
    age,
    gender,
    updated_at
FROM
    src_user_metadata
