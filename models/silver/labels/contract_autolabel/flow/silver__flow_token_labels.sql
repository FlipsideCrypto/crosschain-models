{{ config(
    materialized = 'incremental',
    unique_key = "address",
    incremental_strategy = 'delete+insert',
    tags = ['daily']
) }}

WITH raw AS (

    SELECT
        live.udf_api('https://raw.githubusercontent.com/fixes-world/token-list-jsons/refs/heads/main/jsons/mainnet/flow/default.json') AS rawoutput
),
format_output AS (
    SELECT
        VALUE :address :: STRING AS token_address,
        VALUE :contractName :: STRING AS project_name,
        VALUE :description :: STRING AS description,
        VALUE :name :: STRING AS NAME,
        VALUE :symbol :: STRING AS symbol,
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
        LOWER(token_address) AS address,
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
        AND project_name NOT LIKE 'EVMVMBridgedToken%'
    UNION ALL
    SELECT
        LOWER(
            CONCAT(
                'A.',
                token_address_trim,
                '.',
                project_name
            )
        ) AS address,
        CONCAT(
            NAME,
            ': ',
            symbol,
            ' token'
        ) AS address_name,
        NAME AS project_name,
        'token_contract' AS l2_label
    FROM
        format_output
    WHERE
        address != '0xe452a2f5665728f5'
        AND project_name NOT LIKE 'EVMVMBridgedToken%'
)
SELECT
    SYSDATE() AS system_created_at,
    SYSDATE() :: DATE AS insert_date,
    'flow' AS blockchain,
    address,
    'flipside' AS creator,
    'token' AS l1_label,
    l2_label,
    LOWER(address_name) AS address_name,
    LOWER(project_name) AS project_name,
    SYSDATE() AS _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['address']) }} AS flow_token_labels_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    put_together
WHERE
    address IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY address
ORDER BY
    l2_label DESC)) = 1
