{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', blockchain, address, creator)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'crosschain', 'labels', 'silver__contract_autolabels'],
  post_hook= "delete from {{this}} a using (select distinct blockchain, address from {{ ref('silver__address_labels') }} where delete_flag is null  union select distinct blockchain, address from {{ ref('silver__contract_autolabels') }} union select distinct blockchain, address from {{ ref('silver__labels_eth_contracts_table') }}) b where a.blockchain = b.blockchain and a.address = b.address ",
) }}

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