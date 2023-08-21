{{ config(
    materialized = 'incremental',
    unique_key = "_id",
    post_hook = [ "load_json_tbl(file_name)" ]
) }}

WITH json_config AS (

    SELECT
        *
    FROM
        crosschain.bronze.json_config_tbl

{% if is_incremental() %}
WHERE
    _inserted_timestamp > (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    *
FROM
    json_config
