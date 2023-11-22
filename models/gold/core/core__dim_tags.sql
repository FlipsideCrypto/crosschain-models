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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_contract_address_eth_id,{{ dbt_utils.generate_surrogate_key(['address']) }}) AS dim_tags_id
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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_contract_address_avalanche_id,{{ dbt_utils.generate_surrogate_key(['address']) }}) AS dim_tags_id
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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_contract_address_bsc_id,{{ dbt_utils.generate_surrogate_key(['address']) }}) AS dim_tags_id
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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_contract_address_optimism_id,{{ dbt_utils.generate_surrogate_key(['address']) }}) AS dim_tags_id
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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_contract_address_polygon_id,{{ dbt_utils.generate_surrogate_key(['address']) }}) AS dim_tags_id
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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_gnosis_safe_address_id,{{ dbt_utils.generate_surrogate_key(['address']) }}) AS dim_tags_id
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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_thor_dex_user_id,{{ dbt_utils.generate_surrogate_key(['address']) }}) AS dim_tags_id
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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_thor_liquidity_provider_id,{{ dbt_utils.generate_surrogate_key(['address']) }}) AS dim_tags_id
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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_active_arbitrum_last_7_id,{{ dbt_utils.generate_surrogate_key(['address','start_date']) }}) AS dim_tags_id
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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_active_avalanche_last_7_id,{{ dbt_utils.generate_surrogate_key(['address','start_date']) }}) AS dim_tags_id
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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_active_bsc_last_7_id,{{ dbt_utils.generate_surrogate_key(['address','start_date']) }}) AS dim_tags_id
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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_active_eth_last_7_id,{{ dbt_utils.generate_surrogate_key(['address','start_date']) }}) AS dim_tags_id
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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_active_optimism_last_7_id,{{ dbt_utils.generate_surrogate_key(['address','start_date']) }}) AS dim_tags_id
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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_active_polygon_last_7_id,{{ dbt_utils.generate_surrogate_key(['address','start_date']) }}) AS dim_tags_id
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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_nft_larva_labs_user_id,{{ dbt_utils.generate_surrogate_key(['address']) }}) AS dim_tags_id
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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_nft_looksrare_user_id,{{ dbt_utils.generate_surrogate_key(['address']) }}) AS dim_tags_id
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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_nft_nftx_user_id,{{ dbt_utils.generate_surrogate_key(['address']) }}) AS dim_tags_id
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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_nft_opensea_user_id,{{ dbt_utils.generate_surrogate_key(['address']) }}) AS dim_tags_id
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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_nft_rarible_user_id,{{ dbt_utils.generate_surrogate_key(['address']) }}) AS dim_tags_id
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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_nft_x2y2_user_id,{{ dbt_utils.generate_surrogate_key(['address']) }}) AS dim_tags_id
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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_nft_blur_user_id,{{ dbt_utils.generate_surrogate_key(['address']) }}) AS dim_tags_id
FROM
    {{ ref('silver__tags_nft_blur_user') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_nft_sudoswap_user_id,{{ dbt_utils.generate_surrogate_key(['address']) }}) AS dim_tags_id
FROM
    {{ ref('silver__tags_nft_sudoswap_user') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_wallet_eth_value_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_wallet_token_value_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_wallet_value_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
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
    '2022-09-06' :: TIMESTAMP AS tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['address']) }} AS dim_tags_id
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
    '2022-12-13' :: TIMESTAMP AS tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['address']) }} AS dim_tags_id
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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_cex_user_eth_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name']) }}) AS dim_tags_id
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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_token_vesting_eth_id,{{ dbt_utils.generate_surrogate_key(['address']) }}) AS dim_tags_id
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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_nft_transactor_eth_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_airdrop_master_eth_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
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
    NULL as tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }} AS dim_tags_id
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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_chainlink_oracle_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_chainlink_oracle_arbitrum_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_chainlink_oracle_avalanche_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_chainlink_oracle_polygon_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
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
    '2022-01-18' :: TIMESTAMP as tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }} AS dim_tags_id
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
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_icns_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
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
    '2023-02-09' :: TIMESTAMP as tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }} AS dim_tags_id
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
    '2022-06-01' :: TIMESTAMP as tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }} AS dim_tags_id
FROM  
    {{ ref('silver__optimism_airdrop1_tags') }}
UNION ALL
SELECT 
    blockchain, 
    creator, 
    lower(address), 
    tag_name, 
    tag_type, 
    start_date :: date as start_date,
    null as end_date,
    '2023-05-18' :: TIMESTAMP as tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }} AS dim_tags_id
FROM  
    {{ ref('silver__dydx_delegates') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_token_standard_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
FROM
    {{ ref('silver__tags_token_standard') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_token_standard_erc6551_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
FROM
    {{ ref('silver__tags_token_standard_erc6551') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_erc6551_owner_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
FROM
    {{ ref('silver__tags_erc6551_owner') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_token_standard_erc4626_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
FROM
    {{ ref('silver__tags_token_standard_erc4626') }}
UNION ALL
SELECT 
    blockchain, 
    creator, 
    lower(address), 
    tag_name, 
    tag_type, 
    start_date :: date as start_date,
    null as end_date,
    '2023-06-13' :: TIMESTAMP as tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }} AS dim_tags_id
FROM  
    {{ ref('silver__fund_address_tags') }}
UNION ALL
SELECT 
    blockchain, 
    creator, 
    lower(address), 
    tag_name, 
    tag_type, 
    start_date :: date as start_date,
    null as end_date,
    '2023-06-13' :: TIMESTAMP as tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }} AS dim_tags_id
FROM  
    {{ ref('silver__influencer_address_tags') }}
UNION ALL
SELECT 
    blockchain, 
    creator, 
    lower(address), 
    tag_name, 
    tag_type, 
    start_date :: date as start_date,
    null as end_date,
    '2023-08-22' :: TIMESTAMP as tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['blockchain','address','tag_name','start_date']) }} AS dim_tags_id
FROM  
    {{ ref('silver__uniswap_verified_token_tags') }}
UNION ALL
SELECT 
    blockchain, 
    creator, 
    lower(address), 
    tag_name, 
    tag_type, 
    start_date :: date as start_date,
    null as end_date,
    '2023-09-11' :: TIMESTAMP as tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }} AS dim_tags_id
FROM  
    {{ ref('silver__opensea_names') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_token_standard_arbitrum_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
FROM
    {{ ref('silver__tags_token_standard_arbitrum') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_token_standard_erc4626_arbitrum_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
FROM
    {{ ref('silver__tags_token_standard_erc4626_arbitrum') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_token_standard_erc6551_arbitrum_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
FROM
    {{ ref('silver__tags_token_standard_erc6551_arbitrum') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_token_standard_erc6551_owner_arbitrum_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
FROM
    {{ ref('silver__tags_token_standard_erc6551_owner_arbitrum') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_token_standard_avalanche_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
FROM
    {{ ref('silver__tags_token_standard_avalanche') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_token_standard_erc4626_avalanche_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
FROM
    {{ ref('silver__tags_token_standard_erc4626_avalanche') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_token_standard_erc6551_avalanche_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
FROM
    {{ ref('silver__tags_token_standard_erc6551_avalanche') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_token_standard_erc6551_owner_avalanche_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
FROM
    {{ ref('silver__tags_token_standard_erc6551_owner_avalanche') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_token_standard_base_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
FROM
    {{ ref('silver__tags_token_standard_base') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_token_standard_erc4626_base_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
FROM
    {{ ref('silver__tags_token_standard_erc4626_base') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_token_standard_erc6551_base_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
FROM
    {{ ref('silver__tags_token_standard_erc6551_base') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_token_standard_erc6551_owner_base_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
FROM
    {{ ref('silver__tags_token_standard_erc6551_owner_base') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_token_standard_bsc_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
FROM
    {{ ref('silver__tags_token_standard_bsc') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_token_standard_erc4626_bsc_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
FROM
    {{ ref('silver__tags_token_standard_erc4626_bsc') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_token_standard_erc6551_bsc_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
FROM
    {{ ref('silver__tags_token_standard_erc6551_bsc') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_token_standard_erc6551_owner_bsc_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
FROM
    {{ ref('silver__tags_token_standard_erc6551_owner_bsc') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_token_standard_erc4626_optimism_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
FROM
    {{ ref('silver__tags_token_standard_erc4626_optimism') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_token_standard_erc6551_optimism_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
FROM
    {{ ref('silver__tags_token_standard_erc6551_optimism') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_token_standard_erc6551_owner_optimism_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
FROM
    {{ ref('silver__tags_token_standard_erc6551_owner_optimism') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_token_standard_optimism_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
FROM
    {{ ref('silver__tags_token_standard_optimism') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_token_standard_erc4626_polygon_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
FROM
    {{ ref('silver__tags_token_standard_erc4626_polygon') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_token_standard_erc6551_owner_polygon_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
FROM
    {{ ref('silver__tags_token_standard_erc6551_owner_polygon') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_token_standard_erc6551_polygon_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
FROM
    {{ ref('silver__tags_token_standard_erc6551_polygon') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_token_standard_polygon_id,{{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }}) AS dim_tags_id
FROM
    {{ ref('silver__tags_token_standard_polygon') }}
UNION ALL
SELECT 
    blockchain, 
    creator, 
    regexp_replace(address, '[^[:ascii:]]', '') as address, 
    regexp_replace(tag_name, '[^[:ascii:]]', '') as tag_name, 
    regexp_replace(tag_type, '[^[:ascii:]]', '') as tag_type, 
    start_date :: date as start_date,
    null as end_date,
    '2023-10-27' :: TIMESTAMP as tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['blockchain','address','tag_name','start_date']) }} AS dim_tags_id
FROM  
    {{ ref('silver__Israel_sanctioned_addresses_tags') }}
UNION ALL
SELECT 
    blockchain, 
    creator, 
    address, 
    tag_name, 
    tag_type, 
    start_date,
    end_date,
    tag_created_at,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(tags_sei_abassador_tags_id,{{ dbt_utils.generate_surrogate_key(['address','start_date']) }}) AS dim_tags_id
FROM  
    {{ ref('silver__tags_sei_abassador_tags') }}