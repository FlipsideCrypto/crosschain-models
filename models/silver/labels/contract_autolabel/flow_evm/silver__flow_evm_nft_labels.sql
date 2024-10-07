{{ config(
    materialized = 'incremental',
    unique_key = "address",
    incremental_strategy = 'delete+insert',
    tags = ['daily']
) }}

WITH raw AS (

    SELECT
        livequery.live.udf_api(
            'https://raw.githubusercontent.com/fixes-world/token-list-jsons/refs/heads/main/nftlist-jsons/mainnet/flow/default.json'
        ) AS rawoutput
),
format_output AS (
    SELECT
        VALUE :evmAddress :: STRING AS token_address,
        VALUE :chainId AS chainId,
        VALUE :contractName :: STRING AS project_name,
        VALUE :description :: STRING AS description,
        VALUE :name :: STRING AS NAME
    FROM
        raw,
        LATERAL FLATTEN(input => PARSE_JSON(rawoutput :data :tokens))
),
put_together AS (
    SELECT
        LOWER(token_address) AS address,
        CONCAT(
            NAME,
            ': NFT address'
        ) AS address_name,
        NAME AS project_name,
        'nf_token_contract' AS l2_label
    FROM
        format_output
)
SELECT
    SYSDATE() AS system_created_at,
    SYSDATE() :: DATE AS insert_date,
    'flow_evm' AS blockchain,
    address,
    'flipside' AS creator,
    'nft' AS l1_label,
    l2_label,
    LOWER(address_name) AS address_name,
    LOWER(project_name) AS project_name,
    SYSDATE() AS _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['address']) }} AS flow_evm_nft_labels_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    put_together
WHERE
    address IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY address
ORDER BY
    l2_label DESC)) = 1
