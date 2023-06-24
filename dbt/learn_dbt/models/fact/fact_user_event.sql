{{
  config(
    materialized = 'incremental',
    on_schema_change='fail'
  )
}}
WITH src_user_event AS (
    SELECT * FROM {{ ref("src_user_event") }}
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
WHERE datestamp is not NULL
{% if is_incremental() %}
 AND datestamp > (SELECT max(datestamp) from {{ this }})
{% endif %}
