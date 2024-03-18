{{ config(
    materialized = 'incremental',
    unique_key = "cmc_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"]
) }}

WITH base AS (

    SELECT
        SPLIT_PART(
            REPLACE(
                REPLACE(
                    DATA :data :status :error_message :: STRING,
                    '"'
                ),
                ' '
            ),
            ':',
            2
        ) AS cmc_id_raw,
        _inserted_timestamp
    FROM
        {{ ref('bronze_api__coin_market_cap_cryptocurrency_info') }}
    WHERE
        DATA :data :status :error_message LIKE 'Invalid value% for "id"%'

{% if is_incremental() %}
AND _inserted_timestamp > (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    DISTINCT VALUE :: INT cmc_id,
    _inserted_timestamp,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['cmc_id']) }} as coin_market_cap_cryptocurrency_info_failures_id,
    '{{ invocation_id }}' as _invocation_id
FROM
    base,
    LATERAL SPLIT_TO_TABLE(cmc_id_raw, ',') qualify(ROW_NUMBER() over (PARTITION BY cmc_id
ORDER BY
    _inserted_timestamp DESC)) = 1
