{{ config(
    materialized = 'incremental',
    unique_key = ['blockchain', 'address'],
    cluster_by = ['blockchain','modified_timestamp::DATE'],
    merge_exclude_columns = ['inserted_timestamp'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address,symbol);",
    tags = ['daily']
) }}

{% if execute %}

{% if is_incremental() %}
{% set max_mod_query %}

SELECT
    MAX(modified_timestamp) :: DATE AS modified_timestamp
FROM
    {{ this }}

    {% endset %}
    {% set max_mod = run_query(max_mod_query) [0] [0] %}
{% endif %}
{% endif %}

WITH base AS (
    SELECT
        token_id AS address,
        symbol,
        token_name AS NAME,
        decimals,
        block_id_created AS created_block_number,
        block_timestamp_created AS created_block_timestamp,
        tx_id_created AS created_tx_hash,
        NULL AS creator_address,
        'aleo' AS blockchain
    FROM
        {{ source(
            'aleo_core',
            'dim_token_registrations'
        ) }}
    WHERE
        decimals IS NOT NULL

{% if is_incremental() %}
AND modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}
UNION ALL
SELECT
    token_address AS address,
    symbol,
    NAME,
    decimals,
    NULL created_block_number,
    transaction_created_timestamp AS created_block_timestamp,
    NULL created_tx_hash,
    creator_address,
    'aptos' AS blockchain
FROM
    {{ source(
        'aptos_core',
        'dim_tokens'
    ) }}

{% if is_incremental() %}
WHERE
    modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}
UNION ALL
SELECT
    address,
    symbol,
    NAME,
    decimals,
    created_block_number,
    created_block_timestamp,
    created_tx_hash,
    creator_address,
    'arbitrum' AS blockchain
FROM
    {{ source(
        'arbitrum_core',
        'dim_contracts'
    ) }}
WHERE
    decimals IS NOT NULL

{% if is_incremental() %}
AND modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}
UNION ALL
SELECT
    address,
    symbol,
    NAME,
    decimals,
    created_block_number,
    created_block_timestamp,
    created_tx_hash,
    creator_address,
    'avalanche' AS blockchain
FROM
    {{ source(
        'avalanche_core',
        'dim_contracts'
    ) }}
WHERE
    decimals IS NOT NULL

{% if is_incremental() %}
AND modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}
UNION ALL
SELECT
    A.currency AS address,
    b.project_name AS symbol,
    b.label AS NAME,
    b.decimal AS decimals,
    NULL AS created_block_number,
    NULL AS created_block_timestamp,
    NULL AS created_tx_hash,
    NULL AS creator_address,
    'axelar' AS blockchain
FROM
    {{ source(
        'axelar_core',
        'fact_transfers'
    ) }} A
    LEFT JOIN {{ source(
        'axelar_core',
        'dim_tokens'
    ) }}
    b
    ON A.currency = b.address

{% if is_incremental() %}
WHERE
    A.modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY A.currency
    ORDER BY
        A.block_timestamp DESC
) = 1
UNION ALL
SELECT
    address,
    symbol,
    NAME,
    decimals,
    created_block_number,
    created_block_timestamp,
    created_tx_hash,
    creator_address,
    'base' AS blockchain
FROM
    {{ source(
        'base_core',
        'dim_contracts'
    ) }}
WHERE
    decimals IS NOT NULL

{% if is_incremental() %}
AND modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}
UNION ALL
SELECT
    address,
    symbol,
    NAME,
    decimals,
    created_block_number,
    created_block_timestamp,
    created_tx_hash,
    creator_address,
    'berachain-bartio' AS blockchain
FROM
    {{ source(
        'berachain_bartio_testnet',
        'dim_contracts'
    ) }}
WHERE
    decimals IS NOT NULL

{% if is_incremental() %}
AND modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}
UNION ALL
SELECT
    address,
    symbol,
    NAME,
    decimals,
    created_block_number,
    created_block_timestamp,
    created_tx_hash,
    creator_address,
    'blast' AS blockchain
FROM
    {{ source(
        'blast_core',
        'dim_contracts'
    ) }}
WHERE
    decimals IS NOT NULL

{% if is_incremental() %}
AND modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}
UNION ALL
SELECT
    address,
    symbol,
    NAME,
    decimals,
    created_block_number,
    created_block_timestamp,
    created_tx_hash,
    creator_address,
    'bob' AS blockchain
FROM
    {{ source(
        'bob_core',
        'dim_contracts'
    ) }}
WHERE
    decimals IS NOT NULL

{% if is_incremental() %}
AND modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}
UNION ALL
SELECT
    address,
    symbol,
    NAME,
    decimals,
    created_block_number,
    created_block_timestamp,
    created_tx_hash,
    creator_address,
    'boba' AS blockchain
FROM
    {{ source(
        'boba_core',
        'dim_contracts'
    ) }}
WHERE
    decimals IS NOT NULL

{% if is_incremental() %}
AND modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}
UNION ALL
SELECT
    address,
    symbol,
    NAME,
    decimals,
    created_block_number,
    created_block_timestamp,
    created_tx_hash,
    creator_address,
    'bsc' AS blockchain
FROM
    {{ source(
        'bsc_core',
        'dim_contracts'
    ) }}
WHERE
    decimals IS NOT NULL

{% if is_incremental() %}
AND modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}
UNION ALL
SELECT
    A.currency AS address,
    b.project_name AS symbol,
    b.label AS NAME,
    b.decimal AS decimals,
    NULL AS created_block_number,
    NULL AS created_block_timestamp,
    NULL AS created_tx_hash,
    NULL AS creator_address,
    'cosmos' AS blockchain
FROM
    {{ source(
        'cosmos_core',
        'fact_transfers'
    ) }} A
    LEFT JOIN {{ source(
        'cosmos_core',
        'dim_tokens'
    ) }}
    b
    ON A.currency = b.address
WHERE
    TRY_TO_DECIMAL(currency) IS NULL
    AND currency NOT LIKE '%,%'

{% if is_incremental() %}
AND A.modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY A.currency
    ORDER BY
        A.block_timestamp DESC
) = 1
UNION ALL
SELECT
    address,
    symbol,
    NAME,
    decimals,
    created_block_number,
    created_block_timestamp,
    created_tx_hash,
    creator_address,
    'core' AS blockchain
FROM
    {{ source(
        'core_core',
        'dim_contracts'
    ) }}
WHERE
    decimals IS NOT NULL

{% if is_incremental() %}
AND modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}
UNION ALL
SELECT
    A.mint AS address,
    NULL AS symbol,
    NULL AS NAME,
    NULL AS decimals,
    NULL AS created_block_number,
    NULL AS created_block_timestamp,
    NULL AS created_tx_hash,
    NULL AS creator_address,
    'eclipse' AS blockchain
FROM
    {{ source(
        'eclipse_core',
        'fact_transfers'
    ) }} A
WHERE
    mint IS NOT NULL

{% if is_incremental() %}
AND modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY A.mint
    ORDER BY
        A.block_timestamp DESC
) = 1
UNION ALL
SELECT
    address,
    symbol,
    NAME,
    decimals,
    created_block_number,
    created_block_timestamp,
    created_tx_hash,
    creator_address,
    'ethereum' AS blockchain
FROM
    {{ source(
        'ethereum_core',
        'dim_contracts'
    ) }}
WHERE
    decimals IS NOT NULL

{% if is_incremental() %}
AND modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}
UNION ALL
SELECT
    address,
    symbol,
    NAME,
    decimals,
    created_block_number,
    created_block_timestamp,
    created_tx_hash,
    creator_address,
    'flow' AS blockchain
FROM
    {{ source(
        'flow_evm_core',
        'dim_contracts'
    ) }}
WHERE
    decimals IS NOT NULL

{% if is_incremental() %}
AND modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}
UNION ALL
SELECT
    A.token_contract AS address,
    NULL AS symbol,
    NULL AS NAME,
    NULL AS decimals,
    NULL AS created_block_number,
    NULL AS created_block_timestamp,
    NULL AS created_tx_hash,
    NULL AS creator_address,
    'flow' AS blockchain
FROM
    {{ source(
        'flow_core',
        'ez_token_transfers'
    ) }} A

{% if is_incremental() %}
WHERE
    modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY A.token_contract
    ORDER BY
        A.block_timestamp DESC
) = 1
UNION ALL
SELECT
    address,
    symbol,
    NAME,
    decimals,
    created_block_number,
    created_block_timestamp,
    created_tx_hash,
    creator_address,
    'gnosis' AS blockchain
FROM
    {{ source(
        'gnosis_core',
        'dim_contracts'
    ) }}
WHERE
    decimals IS NOT NULL

{% if is_incremental() %}
AND modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}
UNION ALL
SELECT
    address,
    symbol,
    NAME,
    decimals,
    created_block_number,
    created_block_timestamp,
    created_tx_hash,
    creator_address,
    'ink' AS blockchain
FROM
    {{ source(
        'ink_core',
        'dim_contracts'
    ) }}
WHERE
    decimals IS NOT NULL

{% if is_incremental() %}
AND modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}
UNION ALL
SELECT
    address,
    symbol,
    NAME,
    decimals,
    created_block_number,
    created_block_timestamp,
    created_tx_hash,
    creator_address,
    'kaia' AS blockchain
FROM
    {{ source(
        'kaia_core',
        'dim_contracts'
    ) }}
WHERE
    decimals IS NOT NULL

{% if is_incremental() %}
AND modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}
UNION ALL
SELECT
    address,
    symbol,
    NAME,
    decimals,
    created_block_number,
    created_block_timestamp,
    created_tx_hash,
    creator_address,
    'mantle' AS blockchain
FROM
    {{ source(
        'mantle_core',
        'dim_contracts'
    ) }}
WHERE
    decimals IS NOT NULL

{% if is_incremental() %}
AND modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}
UNION ALL
SELECT
    A.asset AS address,
    NULL AS symbol,
    NULL AS NAME,
    NULL AS decimals,
    NULL AS created_block_number,
    NULL AS created_block_timestamp,
    NULL AS created_tx_hash,
    NULL AS creator_address,
    'maya' AS blockchain
FROM
    {{ source(
        'maya_core',
        'fact_transfers'
    ) }} A

{% if is_incremental() %}
WHERE
    modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY A.asset
    ORDER BY
        A.block_timestamp DESC
) = 1 {# UNION ALL
SELECT
    address,
    symbol,
    NAME,
    decimals,
    created_block_number,
    created_block_timestamp,
    created_tx_hash,
    creator_address,
    'mezo' AS blockchain
FROM
    {{ source(
        'mezo_core',
        'dim_contracts'
    ) }}

{% if is_incremental() %}
WHERE
    modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}

#}
{# UNION ALL
SELECT
    address,
    symbol,
    NAME,
    decimals,
    created_block_number,
    created_block_timestamp,
    created_tx_hash,
    creator_address,
    'monad' AS blockchain
FROM
    {{ source(
        'monad_core',
        'dim_contracts'
    ) }}

{% if is_incremental() %}
WHERE
    modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}

#}
--TODO add movement when a transfer model is created
UNION ALL
SELECT
    contract_address AS address,
    symbol,
    NAME,
    decimals,
    NULL AS created_block_number,
    NULL AS created_block_timestamp,
    NULL AS created_tx_hash,
    NULL AS creator_address,
    'near' AS blockchain
FROM
    {{ source(
        'near_core',
        'dim_ft_contract_metadata'
    ) }}

{% if is_incremental() %}
WHERE
    modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}
UNION ALL
SELECT
    address,
    symbol,
    NAME,
    decimals,
    created_block_number,
    created_block_timestamp,
    created_tx_hash,
    creator_address,
    'optimism' AS blockchain
FROM
    {{ source(
        'optimism_core',
        'dim_contracts'
    ) }}
WHERE
    decimals IS NOT NULL

{% if is_incremental() %}
AND modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}
UNION ALL
SELECT
    address,
    symbol,
    NAME,
    decimals,
    created_block_number,
    created_block_timestamp,
    created_tx_hash,
    creator_address,
    'polygon' AS blockchain
FROM
    {{ source(
        'polygon_core',
        'dim_contracts'
    ) }}
WHERE
    decimals IS NOT NULL

{% if is_incremental() %}
AND modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}

{# UNION ALL
SELECT
    address,
    symbol,
    NAME,
    decimals,
    created_block_number,
    created_block_timestamp,
    created_tx_hash,
    creator_address,
    'rise' AS blockchain
FROM
    {{ source(
        'rise_core',
        'dim_contracts'
    ) }}

{% if is_incremental() %}
WHERE
    modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}

#}
UNION ALL
SELECT
    address,
    symbol,
    NAME,
    decimals,
    created_block_number,
    created_block_timestamp,
    created_tx_hash,
    creator_address,
    'ronin' AS blockchain
FROM
    {{ source(
        'ronin_core',
        'dim_contracts'
    ) }}
WHERE
    decimals IS NOT NULL

{% if is_incremental() %}
AND modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}
UNION ALL
SELECT
    address,
    symbol,
    NAME,
    decimals,
    created_block_number,
    created_block_timestamp,
    created_tx_hash,
    creator_address,
    'sei' AS blockchain
FROM
    {{ source(
        'sei_evm_core',
        'dim_contracts'
    ) }}
WHERE
    decimals IS NOT NULL

{% if is_incremental() %}
AND modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}
UNION ALL
SELECT
    currency AS address,
    symbol,
    token_name AS NAME,
    decimals,
    NULL AS created_block_number,
    NULL AS created_block_timestamp,
    NULL AS created_tx_hash,
    NULL AS creator_address,
    'sei' AS blockchain
FROM
    {{ source(
        'sei_core',
        'dim_tokens'
    ) }}

{% if is_incremental() %}
WHERE
    modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}
UNION ALL
SELECT
    token_address AS address,
    LOWER(symbol) AS symbol,
    token_name AS NAME,
    decimals,
    NULL AS created_block_number,
    NULL AS created_block_timestamp,
    NULL AS created_tx_hash,
    NULL AS creator_address,
    'solana' AS blockchain
FROM
    {{ source(
        'solana_core',
        'dim_tokens'
    ) }}

{% if is_incremental() %}
WHERE
    modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}
UNION ALL
SELECT
    asset_id :: STRING AS address,
    asset_code AS symbol,
    NULL AS NAME,
    NULL AS decimals,
    NULL AS created_block_number,
    NULL AS created_block_timestamp,
    NULL AS created_tx_hash,
    NULL AS creator_address,
    'stellar' AS blockchain
FROM
    {{ source(
        'stellar_core',
        'dim_assets'
    ) }}
WHERE
    asset_id IS NOT NULL

{% if is_incremental() %}
AND modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}
UNION ALL
SELECT
    address,
    symbol,
    NAME,
    decimals,
    created_block_number,
    created_block_timestamp,
    created_tx_hash,
    creator_address,
    'swell' AS blockchain
FROM
    {{ source(
        'swell_core',
        'dim_contracts'
    ) }}
WHERE
    decimals IS NOT NULL

{% if is_incremental() %}
AND modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}
UNION ALL
SELECT
    A.asset AS address,
    NULL AS symbol,
    NULL AS NAME,
    NULL AS decimals,
    NULL AS created_block_number,
    NULL AS created_block_timestamp,
    NULL AS created_tx_hash,
    NULL AS creator_address,
    'thorchain' AS blockchain
FROM
    {{ source(
        'thorchain_core',
        'fact_transfers'
    ) }} A

{% if is_incremental() %}
WHERE
    modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY A.asset
    ORDER BY
        A.block_timestamp DESC
) = 1
UNION ALL
SELECT
    address,
    symbol,
    description AS NAME,
    decimals,
    NULL AS created_block_number,
    NULL AS created_block_timestamp,
    NULL AS created_tx_hash,
    NULL AS creator_address,
    'ton' AS blockchain
FROM
    {{ source(
        'ton_core',
        'fact_jetton_metadata'
    ) }}

{% if is_incremental() %}
WHERE
    modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY address
    ORDER BY
        update_time_onchain DESC,
        update_time_metadata DESC
) = 1
)
SELECT
    address,
    symbol,
    NAME,
    decimals,
    created_block_number,
    created_block_timestamp,
    created_tx_hash,
    creator_address,
    blockchain,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['blockchain','address']) }} AS tokens_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base
