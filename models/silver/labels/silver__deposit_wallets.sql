{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', blockchain, address, creator)",
    incremental_strategy = 'delete+insert',
    tags = ['weekly_full_refresh'],
    post_hook = "delete from {{this}} a using {{ ref('silver__address_labels') }} b where a.blockchain = b.blockchain and a.address = b.address "
) }}

WITH pre_final AS (

    SELECT
        system_created_at,
        insert_date,
        blockchain,
        address,
        creator,
        l1_label AS label_type,
        l2_label AS label_subtype,
        address_name,
        project_name
    FROM
        {{ ref('silver__snowflake_Algorand_satellite') }}

{% if is_incremental() %}
WHERE
    insert_date >= (
        SELECT
            MAX(insert_date)
        FROM
            {{ this }}
        WHERE
            blockchain = 'algorand'
    )
{% endif %}
UNION ALL
SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name
FROM
    {{ ref('silver__snowflake_Aptos_satellites') }}

{% if is_incremental() %}
WHERE
    insert_date >= (
        SELECT
            MAX(insert_date)
        FROM
            {{ this }}
        WHERE
            blockchain = 'aptos'
    )
{% endif %}
UNION ALL
SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name
FROM
    {{ ref('silver__snowflake_Arbitrum_satellites') }}

{% if is_incremental() %}
WHERE
    insert_date >= (
        SELECT
            MAX(insert_date)
        FROM
            {{ this }}
        WHERE
            blockchain = 'arbitrum'
    )
{% endif %}
UNION ALL
SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name
FROM
    {{ ref('silver__snowflake_Avalanche_satellites') }}

{% if is_incremental() %}
WHERE
    insert_date >= (
        SELECT
            MAX(insert_date)
        FROM
            {{ this }}
        WHERE
            blockchain = 'avalanche'
    )
{% endif %}
UNION ALL
SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name
FROM
    {{ ref('silver__snowflake_Base_satellites') }}

{% if is_incremental() %}
WHERE
    insert_date >= (
        SELECT
            MAX(insert_date)
        FROM
            {{ this }}
        WHERE
            blockchain = 'base'
    )
{% endif %}
UNION ALL
SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name
FROM
    {{ ref('silver__snowflake_Blast_satellites') }}

{% if is_incremental() %}
WHERE
    insert_date >= (
        SELECT
            MAX(insert_date)
        FROM
            {{ this }}
        WHERE
            blockchain = 'blast'
    )
{% endif %}
UNION ALL
SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name
FROM
    {{ ref('silver__snowflake_BSC_satellites') }}

{% if is_incremental() %}
WHERE
    insert_date >= (
        SELECT
            MAX(insert_date)
        FROM
            {{ this }}
        WHERE
            blockchain = 'bsc'
    )
{% endif %}
UNION ALL
SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name
FROM
    {{ ref('silver__snowflake_ETH_satellites') }}

{% if is_incremental() %}
WHERE
    insert_date >= (
        SELECT
            MAX(insert_date)
        FROM
            {{ this }}
        WHERE
            blockchain = 'ethereum'
    )
{% endif %}
UNION ALL
SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name
FROM
    {{ ref('silver__snowflake_Flow_satellites') }}

{% if is_incremental() %}
WHERE
    insert_date >= (
        SELECT
            MAX(insert_date)
        FROM
            {{ this }}
        WHERE
            blockchain = 'flow'
    )
{% endif %}
UNION ALL
SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name
FROM
    {{ ref('silver__snowflake_Kaia_satellites') }}

{% if is_incremental() %}
WHERE
    insert_date >= (
        SELECT
            MAX(insert_date)
        FROM
            {{ this }}
        WHERE
            blockchain = 'kaia'
    )
{% endif %}
UNION ALL
SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name
FROM
    {{ ref('silver__snowflake_Near_satellite') }}

{% if is_incremental() %}
WHERE
    insert_date >= (
        SELECT
            MAX(insert_date)
        FROM
            {{ this }}
        WHERE
            blockchain = 'near'
    )
{% endif %}
UNION ALL
SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name
FROM
    {{ ref('silver__snowflake_Optimism_satellites') }}

{% if is_incremental() %}
WHERE
    insert_date >= (
        SELECT
            MAX(insert_date)
        FROM
            {{ this }}
        WHERE
            blockchain = 'optimism'
    )
{% endif %}
UNION ALL
SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name
FROM
    {{ ref('silver__snowflake_Osmosis_satellite') }}

{% if is_incremental() %}
WHERE
    insert_date >= (
        SELECT
            MAX(insert_date)
        FROM
            {{ this }}
        WHERE
            blockchain = 'osmosis'
    )
{% endif %}
UNION ALL
SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name
FROM
    {{ ref('silver__snowflake_Polygon_satellites') }}

{% if is_incremental() %}
WHERE
    insert_date >= (
        SELECT
            MAX(insert_date)
        FROM
            {{ this }}
        WHERE
            blockchain = 'polygon'
    )
{% endif %}
UNION ALL
SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name
FROM
    {{ ref('silver__snowflake_Ronin_satellites') }}

{% if is_incremental() %}
WHERE
    insert_date >= (
        SELECT
            MAX(insert_date)
        FROM
            {{ this }}
        WHERE
            blockchain = 'ronin'
    )
{% endif %}
UNION ALL
SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name
FROM
    {{ ref('silver__snowflake_Sei_Evm_satellites') }}

{% if is_incremental() %}
WHERE
    insert_date >= (
        SELECT
            MAX(insert_date)
        FROM
            {{ this }}
        WHERE
            blockchain = 'sei_evm'
    )
{% endif %}
UNION ALL
SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name
FROM
    {{ ref('silver__snowflake_SOL_satellites') }}

{% if is_incremental() %}
WHERE
    insert_date >= (
        SELECT
            MAX(insert_date)
        FROM
            {{ this }}
        WHERE
            blockchain = 'solana'
    )
{% endif %}
UNION ALL
SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name
FROM
    {{ ref('silver__snowflake_Thorchain_satellite') }}

{% if is_incremental() %}
WHERE
    insert_date >= (
        SELECT
            MAX(insert_date)
        FROM
            {{ this }}
        WHERE
            blockchain = 'thorchain'
    )
{% endif %}
UNION ALL
SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name
FROM
    {{ ref('silver__snowflake_Arbitrum_satellites_small') }}

{% if is_incremental() %}
WHERE
    insert_date >= (
        SELECT
            MAX(insert_date)
        FROM
            {{ this }}
        WHERE
            blockchain = 'arbitrum'
    )
{% endif %}
UNION ALL
SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name
FROM
    {{ ref('silver__snowflake_Avalanche_satellites_small') }}

{% if is_incremental() %}
WHERE
    insert_date >= (
        SELECT
            MAX(insert_date)
        FROM
            {{ this }}
        WHERE
            blockchain = 'avalanche'
    )
{% endif %}
UNION ALL
SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name
FROM
    {{ ref('silver__snowflake_Base_satellites_small') }}

{% if is_incremental() %}
WHERE
    insert_date >= (
        SELECT
            MAX(insert_date)
        FROM
            {{ this }}
        WHERE
            blockchain = 'base'
    )
{% endif %}
UNION ALL
SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name
FROM
    {{ ref('silver__snowflake_Blast_satellites_small') }}

{% if is_incremental() %}
WHERE
    insert_date >= (
        SELECT
            MAX(insert_date)
        FROM
            {{ this }}
        WHERE
            blockchain = 'blast'
    )
{% endif %}
UNION ALL
SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name
FROM
    {{ ref('silver__snowflake_BSC_satellites_small') }}

{% if is_incremental() %}
WHERE
    insert_date >= (
        SELECT
            MAX(insert_date)
        FROM
            {{ this }}
        WHERE
            blockchain = 'bsc'
    )
{% endif %}
UNION ALL
SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name
FROM
    {{ ref('silver__snowflake_ETH_satellites_small') }}

{% if is_incremental() %}
WHERE
    insert_date >= (
        SELECT
            MAX(insert_date)
        FROM
            {{ this }}
        WHERE
            blockchain = 'ethereum'
    )
{% endif %}
UNION ALL
SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name
FROM
    {{ ref('silver__snowflake_Optimism_satellites_small') }}

{% if is_incremental() %}
WHERE
    insert_date >= (
        SELECT
            MAX(insert_date)
        FROM
            {{ this }}
        WHERE
            blockchain = 'optimism'
    )
{% endif %}
UNION ALL
SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name
FROM
    {{ ref('silver__snowflake_Polygon_satellites_small') }}

{% if is_incremental() %}
WHERE
    insert_date >= (
        SELECT
            MAX(insert_date)
        FROM
            {{ this }}
        WHERE
            blockchain = 'polygon'
    )
{% endif %}
)
SELECT
    *,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['blockchain','creator','address']) }} AS deposit_wallets_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final
