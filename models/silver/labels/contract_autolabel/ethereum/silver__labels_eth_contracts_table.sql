{{ config(
    materialized = 'incremental',
    unique_key = "address",
    incremental_strategy = 'delete+insert',
    tags = ['daily'],
    post_hook = "delete from {{this}} a using (select distinct blockchain, address from {{ ref('silver__address_labels') }} where delete_flag is null union select distinct blockchain, address from {{ ref('silver__deposit_wallets') }} union select distinct blockchain, address from {{ ref('silver__contract_autolabels') }}) b where a.blockchain = b.blockchain and a.address = b.address ",
) }}

WITH tokens AS (

    SELECT
        DISTINCT contract_address :: STRING AS address,
        'token' AS l1_label,
        'token_contract' AS l2_label
    FROM
        {{ source(
            'ethereum_core',
            'fact_event_logs'
        ) }}
    WHERE
        utils.udf_hex_to_int(SUBSTR(DATA, 3, 64)) :: FLOAT IS NOT NULL
        AND topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        AND tx_succeeded
),
nfts AS (
    SELECT
        DISTINCT contract_address :: STRING AS address,
        'nft' AS l1_label,
        'nf_token_contract' AS l2_label
    FROM
        {{ source(
            'ethereum_core',
            'fact_event_logs'
        ) }}
    WHERE
        utils.udf_hex_to_int(SUBSTR(DATA, 3, 64)) :: FLOAT IS NULL
        AND topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        AND tx_succeeded
)
SELECT
    DISTINCT CURRENT_DATE :: TIMESTAMP AS system_created_at,
    CURRENT_DATE :: TIMESTAMP AS insert_date,
    'ethereum' AS blockchain,
    LOWER(
        A.address :: STRING
    ) AS address,
    'flipside' AS creator,
    COALESCE(
        n.l1_label,
        t.l1_label
    ) AS label_type,
    COALESCE(
        n.l2_label,
        t.l2_label
    ) AS label_subtype,
    NAME AS project_name,
    NAME AS address_name,
    NULL AS delete_flag,
    _inserted_timestamp,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['A.address']) }} AS labels_eth_contracts_table_id,
    '{{ invocation_id }}' as _invocation_id
FROM
    {{ source(
        'ethereum_silver',
        'contracts'
    ) }} A
    LEFT JOIN tokens t
    ON A.address = t.address
    LEFT JOIN nfts n
    ON A.address = n.address
WHERE
    label_type IS NOT NULL
    AND project_name IS NOT NULL
    and project_name != ''
    and project_name != ' '

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
