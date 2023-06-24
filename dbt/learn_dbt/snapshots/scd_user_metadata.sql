{% snapshot scd_user_metadata %}
{{
 config(
 target_schema='jhjmo0719h',
 unique_key='user_id',
 strategy='timestamp',
 updated_at='updated_at',
 invalidate_hard_deletes=True
 )
}}
SELECT * FROM {{ source('jhjmo0719h', 'metadata') }}
{% endsnapshot %}
