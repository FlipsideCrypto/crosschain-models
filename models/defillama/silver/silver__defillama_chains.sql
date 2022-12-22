{{ config(
    materialized = 'incremental',
    unique_key = 'chain'
) }}

WITH chain_base AS (

SELECT
    ethereum.streamline.udf_api(
        'GET','https://api.llama.fi/chains',{},{}
    ) AS read,
    SYSDATE() AS _inserted_timestamp
)

SELECT
    VALUE:name::STRING AS chain,
    VALUE:chainId::STRING AS chain_id,
    VALUE:tokenSymbol::STRING AS token_symbol,
    _inserted_timestamp
FROM chain_base,
    LATERAL FLATTEN (input=> read:data)

{% if is_incremental() %}
WHERE chain NOT IN (
    SELECT
        DISTINCT chain
    FROM
        {{ this }}
)
{% endif %}