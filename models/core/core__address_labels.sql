{{ config(
    materialized = 'view',
    tags = ['snowflake', 'crosschain', 'labels', 'gold_address_labels'],
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
    {{ ref('silver_crosschain__address_labels') }}
    -- deposit wallet algos
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
    {{ ref('silver_crosschain__snowflake_Algorand_satellite') }}
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
    {{ ref('silver_crosschain__snowflake_Arbitrum_satellites') }}
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
    {{ ref('silver_crosschain__snowflake_Avalanche_satellites') }}
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
    {{ ref('silver_crosschain__snowflake_BSC_satellites') }}
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
    {{ ref('silver_crosschain__snowflake_ETH_satellites') }}
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
    {{ ref('silver_crosschain__snowflake_Flow_satellites') }}
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
    {{ ref('silver_crosschain__snowflake_Near_satellite') }}
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
    {{ ref('silver_crosschain__snowflake_Optimism_satellites') }}
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
    {{ ref('silver_crosschain__snowflake_Osmosis_satellite') }}
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
    {{ ref('silver_crosschain__snowflake_Polygon_satellites') }}
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
    {{ ref('silver_crosschain__snowflake_SOL_satellites') }}
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
    {{ ref('silver_crosschain__snowflake_Thorchain_satellite') }}
