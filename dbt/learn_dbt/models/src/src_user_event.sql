WITH src_user_event AS (
    SELECT * FROM {{ source("jhjmo0719h", "event") }}
)
SELECT
    user_id,
    datestamp,
    item_id,
    clicked,
    purchased,
    paidamount
FROM
    src_user_event
