{{ config(
    materialized = 'incremental',
    unique_key = "address",
    incremental_strategy = 'delete+insert',
    tags = ['daily'],
    post_hook = "delete from {{this}} a using (select distinct address from {{ ref('silver__flow_evm_nft_labels') }} union select distinct address from {{ ref('silver__flow_evm_token_labels') }}) b where a.address = b.address ",
) }}

WITH raw AS (

    SELECT
        livequery.live.udf_api(
            'https://evm.flowscan.io/api/v2/tokens?items_count=1000'
        ) AS rawoutput
),
format_output AS (
    SELECT
        VALUE :address :: STRING AS token_address,
        VALUE :name :: STRING AS NAME,
        VALUE :symbol :: STRING AS symbol,
        VALUE :type :: STRING AS TYPE
    FROM
        raw,
        LATERAL FLATTEN(input => PARSE_JSON(rawoutput :data :items))
),
put_together AS (
    SELECT
        DISTINCT LOWER(token_address) AS address,
        CONCAT(
            NAME,
            ': ',
            symbol,
            ' token'
        ) AS address_name,
        NAME AS project_name,
        'token' AS l1_label,
        'token_contract' AS l2_label
    FROM
        format_output
    WHERE
        TYPE = 'ERC-20'
    UNION
    SELECT
        DISTINCT LOWER(token_address) AS address,
        CONCAT(
            NAME,
            ': NFT address'
        ) AS address_name,
        NAME AS project_name,
        'nft' AS l1_label,
        'nf_token_contract' AS l2_label
    FROM
        format_output
    WHERE
        TYPE IN (
            'ERC-721',
            'ERC-1155'
        )
)
SELECT
    DISTINCT SYSDATE() AS system_created_at,
    SYSDATE() AS insert_date,
    'flow_evm' AS blockchain,
    address,
    'flipside' AS creator,
    l1_label,
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
