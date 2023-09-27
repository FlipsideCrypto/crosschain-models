{{ config(
    materialized = 'incremental',
    unique_key = ['_res_id'],
    full_refresh = false
) }}

SELECT
    repo_owner,
    repo_name,
    endpoint_name,
    data,
    provider,
    endpoint_github,
    _inserted_timestamp,
    _res_id
FROM
    {{ source(
        'bronze_api',
        'github_repo_data'
    ) }}
{% if is_incremental() %}
WHERE
    _inserted_timestamp > (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}