SELECT
    *
FROM (
    SELECT
    user_id, COUNT(1) cnt
    FROM
      {{ ref("dim_user_metadata") }}
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 1
)
WHERE cnt > 1
