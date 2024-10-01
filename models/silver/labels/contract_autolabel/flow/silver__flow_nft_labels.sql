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
        VALUE :address :: STRING AS token_address,
        VALUE :chainId AS chainId,
        VALUE :contractName :: STRING AS project_name,
        VALUE :description :: STRING AS description,
        VALUE :name :: STRING AS NAME,
        SUBSTR(
            VALUE :address :: STRING,
            3
        ) AS token_address_trim
    FROM
        raw,
        LATERAL FLATTEN(input => PARSE_JSON(rawoutput :data :tokens))
),
put_together AS (
    SELECT
        DISTINCT LOWER(token_address) AS address,
        CONCAT(
            NAME,
            ': account address'
        ) AS address_name,
        NAME AS project_name,
        'contract_deployer' AS l2_label
    FROM
        format_output
    WHERE
        address != '0xe452a2f5665728f5'
    UNION ALL
    SELECT
        DISTINCT LOWER(CONCAT('A.', token_address_trim, project_name)) AS address,
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
    DISTINCT SYSDATE() AS system_created_at,
    SYSDATE() AS insert_date,
    'flow' AS blockchain,
    address,
    'flipside' AS creator,
    'nft' AS l1_label,
    l2_label,
    lower(address_name) as address_name,
    lower(project_name) as project_name,
    SYSDATE() AS _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['address']) }} AS flow_nft_labels_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    put_together 
where address is not null
qualify(ROW_NUMBER() over(PARTITION BY address
ORDER BY
    l2_label DESC)) = 1
