{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', id, token_address)",
    incremental_strategy = 'delete+insert'
) }}

SELECT
    id,
    i.value :: STRING AS token_address,
    NAME,
    symbol,
    i.key AS platform,
    _inserted_timestamp
FROM
    {{ ref('bronze__asset_metadata_coin_gecko') }} A,
    TABLE(FLATTEN(A.value :platforms)) i

{% if is_incremental() %}
WHERE
    _inserted_date >= (
        SELECT
            MAX(
                _inserted_timestamp :: DATE
            )
        FROM
            {{ this }}
    )
    AND _inserted_timestamp > (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY id, token_address
ORDER BY
    _inserted_timestamp DESC)) = 1
