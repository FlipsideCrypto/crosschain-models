{{ config(
    materialized = 'incremental',
    unique_key = ['_res_id'],
    cluster_by = ['_inserted_timestamp::DATE'],
    merge_exclude_columns = ["inserted_timestamp"],
) }}

SELECT
    project_name,
    repo_owner,
    repo_name,
    endpoint_name,
    data,
    provider,
    endpoint_github,
    _inserted_timestamp,
    _res_id,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['_res_id']) }} AS github_activity_id,
    '{{ invocation_id }}' as _invocation_id
FROM
    {{ source(
        'github',
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