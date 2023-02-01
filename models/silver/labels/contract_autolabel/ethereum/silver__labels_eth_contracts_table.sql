{{ config(
    materialized = 'incremental',
    unique_key = "address",
    incremental_strategy = 'delete+insert',
    tags = ['snowflake', 'crosschain', 'labels'],
    post_hook = ["delete from {{this}} a using {{ ref('silver__address_labels') }} b where a.blockchain = b.blockchain and a.address = b.address ", "delete from {{this}} a using {{ ref('silver__contract_autolabels') }} b where a.blockchain = b.blockchain and a.address = b.address "]
) }}

WITH tokens AS (

    SELECT
        DISTINCT contract_address :: STRING AS address,
        'token' AS l1_label,
        'token_contract' AS l2_label
    FROM
        {{ source(
            'ethereum_silver',
            'logs'
        ) }}
    WHERE
        PUBLIC.udf_hex_to_int(SUBSTR(DATA, 3, 64)) :: FLOAT IS NOT NULL
        AND topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        AND tx_status = 'SUCCESS'
),
nfts AS (
    SELECT
        DISTINCT contract_address :: STRING AS address,
        'nft' AS l1_label,
        'nf_token_contract' AS l2_label
    FROM
        {{ source(
            'ethereum_silver',
            'logs'
        ) }}
    WHERE
        PUBLIC.udf_hex_to_int(SUBSTR(DATA, 3, 64)) :: FLOAT IS NULL
        AND topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        AND tx_status = 'SUCCESS'
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
    _inserted_timestamp
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
