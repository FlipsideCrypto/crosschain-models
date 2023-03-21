{{ config(
    materialized = 'view'
) }}

SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_contract_address_eth') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_contract_address_avalanche') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_contract_address_bsc') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_contract_address_optimism') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_contract_address_polygon') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_gnosis_safe_address') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_thor_dex_user') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_thor_liquidity_provider') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_active_arbitrum_last_7') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_active_avalanche_last_7') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_active_bsc_last_7') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_active_eth_last_7') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_active_optimism_last_7') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_active_polygon_last_7') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_nft_larva_labs_user') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_nft_looksrare_user') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_nft_nftx_user') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_nft_opensea_user') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_nft_rarible_user') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_nft_x2y2_user') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_wallet_ETH_value') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_wallet_TOKEN_value') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_wallet_value') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date :: date,
    null as end_date,
    '2022-09-06' :: TIMESTAMP AS tag_created_at
FROM
    {{ source(
        'crosschain_silver',
        'godmode_nft_minters'
    ) }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date :: date,
    null as end_date,
    '2022-12-13' :: TIMESTAMP AS tag_created_at
FROM
    {{ source(
        'crosschain_silver',
        'osmosis_developer_vesting_receivers'
    ) }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_cex_user_ETH') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_token_vesting_ETH') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_nft_transactor_ETH') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_airdrop_master_ETH') }}
UNION ALL
SELECT 
    blockchain, 
    creator, 
    address, 
    tag_name, 
    tag_type, 
    to_date(start_date) AS start_date, 
    NULL AS end_date, 
    NULL as tag_created_at
FROM 
    {{ source(
        'crosschain_silver',
        'optimism_delegates'
    )}} 
UNION  ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_chainlink_oracle') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_chainlink_oracle_arbitrum') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_chainlink_oracle_avalanche') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_chainlink_oracle_polygon') }}
UNION ALL
SELECT
    blockchain,
    creator,
    LOWER(address) as address,
    tag_name,
    tag_type,
    start_date :: date,
    null as end_date,
    '2022-01-18' :: TIMESTAMP as tag_created_at
FROM
    {{ ref('silver__aave_balancer_addresses') }}
UNION ALL
SELECT 
    blockchain, 
    creator, 
    address, 
    tag_name, 
    tag_type, 
    start_date, 
    end_date,
    tag_created_at
FROM  
    {{ ref('silver__tags_icns') }}
UNION ALL
SELECT 
    blockchain, 
    creator, 
    address, 
    tag_name, 
    tag_type, 
    '2023-02-09' :: TIMESTAMP as start_date,
    null as end_date,
    '2023-02-09' :: TIMESTAMP as tag_created_at
FROM  
    {{ ref('silver__optimism_airdrop2_tags') }}
UNION ALL
SELECT 
    blockchain, 
    creator, 
    address, 
    tag_name, 
    tag_type, 
    '2022-06-01' :: TIMESTAMP as start_date,
    null as end_date,
    '2022-06-01' :: TIMESTAMP as tag_created_at
FROM  
    {{ ref('silver__optimism_airdrop1_tags') }}