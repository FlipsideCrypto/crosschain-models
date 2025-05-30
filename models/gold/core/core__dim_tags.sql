{{ config(
    materialized = 'view'
) }}

WITH pre_final AS (

    SELECT
        blockchain,
        creator,
        address,
        tag_name,
        tag_type,
        start_date,
        end_date,
        tag_created_at,
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
    FROM
        {{ ref('silver__tags_active_polygon_last_7') }}
        {# UNION ALL
    SELECT
        blockchain,
        creator,
        address,
        tag_name,
        tag_type,
        start_date,
        end_date,
        tag_created_at,
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
    FROM
        {{ ref('silver__tags_nft_larva_labs_user') }}
        #}
        {# UNION ALL
    SELECT
        blockchain,
        creator,
        address,
        tag_name,
        tag_type,
        start_date,
        end_date,
        tag_created_at,
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
    FROM
        {{ ref('silver__tags_nft_looksrare_user') }}
        #}
        {# UNION ALL
    SELECT
        blockchain,
        creator,
        address,
        tag_name,
        tag_type,
        start_date,
        end_date,
        tag_created_at,
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
    FROM
        {{ ref('silver__tags_nft_nftx_user') }}
        #}
        {# UNION ALL
    SELECT
        blockchain,
        creator,
        address,
        tag_name,
        tag_type,
        start_date,
        end_date,
        tag_created_at,
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
    FROM
        {{ ref('silver__tags_nft_opensea_user') }}
        #}
        {# UNION ALL
    SELECT
        blockchain,
        creator,
        address,
        tag_name,
        tag_type,
        start_date,
        end_date,
        tag_created_at,
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
    FROM
        {{ ref('silver__tags_nft_rarible_user') }}
        #}
        {# UNION ALL
    SELECT
        blockchain,
        creator,
        address,
        tag_name,
        tag_type,
        start_date,
        end_date,
        tag_created_at,
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
    FROM
        {{ ref('silver__tags_nft_x2y2_user') }}
        #}
        {# UNION ALL
    SELECT
        blockchain,
        creator,
        address,
        tag_name,
        tag_type,
        start_date,
        end_date,
        tag_created_at,
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
    FROM
        {{ ref('silver__tags_nft_blur_user') }}
        #}
        {# UNION ALL
    SELECT
        blockchain,
        creator,
        address,
        tag_name,
        tag_type,
        start_date,
        end_date,
        tag_created_at,
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
    FROM
        {{ ref('silver__tags_nft_sudoswap_user') }}
        #}
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
    FROM
        {{ ref('silver__tags_wallet_value') }}
    UNION ALL
    SELECT
        blockchain,
        creator,
        address,
        tag_name,
        tag_type,
        start_date :: DATE,
        NULL AS end_date,
        '2022-09-06' :: TIMESTAMP AS tag_created_at,
        '2000-01-01' :: timestamp_ntz AS inserted_timestamp,
        '2000-01-01' :: timestamp_ntz AS modified_timestamp
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
        start_date :: DATE,
        NULL AS end_date,
        '2022-12-13' :: TIMESTAMP AS tag_created_at,
        '2000-01-01' :: timestamp_ntz AS inserted_timestamp,
        '2000-01-01' :: timestamp_ntz AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
    FROM
        {{ ref('silver__tags_airdrop_master_ETH') }}
    UNION ALL
    SELECT
        blockchain,
        creator,
        address,
        tag_name,
        tag_type,
        TO_DATE(start_date) AS start_date,
        NULL AS end_date,
        NULL AS tag_created_at,
        '2000-01-01' :: timestamp_ntz AS inserted_timestamp,
        '2000-01-01' :: timestamp_ntz AS modified_timestamp
    FROM
        {{ source(
            'crosschain_silver',
            'optimism_delegates'
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
    FROM
        {{ ref('silver__tags_chainlink_oracle_polygon') }}
    UNION ALL
    SELECT
        blockchain,
        creator,
        LOWER(address) AS address,
        tag_name,
        tag_type,
        start_date :: DATE,
        NULL AS end_date,
        '2022-01-18' :: TIMESTAMP AS tag_created_at,
        '2000-01-01' :: timestamp_ntz AS inserted_timestamp,
        '2000-01-01' :: timestamp_ntz AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
    FROM
        {{ ref('silver__tags_icns') }}
    UNION ALL
    SELECT
        blockchain,
        creator,
        address,
        tag_name,
        tag_type,
        '2023-02-09' :: TIMESTAMP AS start_date,
        NULL AS end_date,
        '2023-02-09' :: TIMESTAMP AS tag_created_at,
        '2000-01-01' :: timestamp_ntz AS inserted_timestamp,
        '2000-01-01' :: timestamp_ntz AS modified_timestamp
    FROM
        {{ ref('silver__optimism_airdrop2_tags') }}
    UNION ALL
    SELECT
        blockchain,
        creator,
        address,
        tag_name,
        tag_type,
        '2022-06-01' :: TIMESTAMP AS start_date,
        NULL AS end_date,
        '2022-06-01' :: TIMESTAMP AS tag_created_at,
        '2000-01-01' :: timestamp_ntz AS inserted_timestamp,
        '2000-01-01' :: timestamp_ntz AS modified_timestamp
    FROM
        {{ ref('silver__optimism_airdrop1_tags') }}
    UNION ALL
    SELECT
        blockchain,
        creator,
        LOWER(address),
        tag_name,
        tag_type,
        start_date :: DATE AS start_date,
        NULL AS end_date,
        '2023-05-18' :: TIMESTAMP AS tag_created_at,
        '2000-01-01' :: timestamp_ntz AS inserted_timestamp,
        '2000-01-01' :: timestamp_ntz AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
    FROM
        {{ ref('silver__tags_token_standard_erc4626') }}
    UNION ALL
    SELECT
        blockchain,
        creator,
        LOWER(address),
        tag_name,
        tag_type,
        start_date :: DATE AS start_date,
        NULL AS end_date,
        '2023-06-13' :: TIMESTAMP AS tag_created_at,
        '2000-01-01' :: timestamp_ntz AS inserted_timestamp,
        '2000-01-01' :: timestamp_ntz AS modified_timestamp
    FROM
        {{ ref('silver__fund_address_tags') }}
    UNION ALL
    SELECT
        blockchain,
        creator,
        LOWER(address),
        tag_name,
        tag_type,
        start_date :: DATE AS start_date,
        NULL AS end_date,
        '2023-06-13' :: TIMESTAMP AS tag_created_at,
        '2000-01-01' :: timestamp_ntz AS inserted_timestamp,
        '2000-01-01' :: timestamp_ntz AS modified_timestamp
    FROM
        {{ ref('silver__influencer_address_tags') }}
    UNION ALL
    SELECT
        blockchain,
        creator,
        LOWER(address),
        tag_name,
        tag_type,
        start_date :: DATE AS start_date,
        NULL AS end_date,
        '2023-08-22' :: TIMESTAMP AS tag_created_at,
        '2000-01-01' :: timestamp_ntz AS inserted_timestamp,
        '2000-01-01' :: timestamp_ntz AS modified_timestamp
    FROM
        {{ ref('silver__uniswap_verified_token_tags') }}
    UNION ALL
    SELECT
        blockchain,
        creator,
        LOWER(address),
        tag_name,
        tag_type,
        start_date :: DATE AS start_date,
        NULL AS end_date,
        '2023-09-11' :: TIMESTAMP AS tag_created_at,
        '2000-01-01' :: timestamp_ntz AS inserted_timestamp,
        '2000-01-01' :: timestamp_ntz AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
    FROM
        {{ ref('silver__tags_token_standard_polygon') }}
    UNION ALL
    SELECT
        blockchain,
        creator,
        REGEXP_REPLACE(
            address,
            '[^[:ascii:]]',
            ''
        ) AS address,
        REGEXP_REPLACE(
            tag_name,
            '[^[:ascii:]]',
            ''
        ) AS tag_name,
        REGEXP_REPLACE(
            tag_type,
            '[^[:ascii:]]',
            ''
        ) AS tag_type,
        start_date :: DATE AS start_date,
        NULL AS end_date,
        '2023-10-27' :: TIMESTAMP AS tag_created_at,
        '2000-01-01' :: timestamp_ntz AS inserted_timestamp,
        '2000-01-01' :: timestamp_ntz AS modified_timestamp
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
    FROM
        {{ ref('silver__tags_sei_abassador_tags') }}
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
    FROM
        {{ ref('silver__libreplex_inscription') }}
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
    FROM
        {{ ref('silver__tags_active_kaia') }}
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
    FROM
        {{ ref('silver__tags_active_lp_kaia') }}
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
    FROM
        {{ ref('silver__tags_lst_holder_kaia') }}
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
    FROM
        {{ ref('silver__tags_token_swaps_kaia') }}
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
        COALESCE(
            inserted_timestamp,
            '2000-01-01'
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            '2000-01-01'
        ) AS modified_timestamp
    FROM
        {{ ref('silver__tags_transfers_kaia') }}
    UNION ALL
    SELECT
        blockchain,
        creator,
        REGEXP_REPLACE(
            address,
            '[^[:ascii:]]',
            ''
        ) AS address,
        REGEXP_REPLACE(
            tag_name,
            '[^[:ascii:]]',
            ''
        ) AS tag_name,
        REGEXP_REPLACE(
            tag_type,
            '[^[:ascii:]]',
            ''
        ) AS tag_type,
        start_date :: DATE AS start_date,
        NULL AS end_date,
        '2024-11-20' :: TIMESTAMP AS tag_created_at,
        '2024-11-20' :: timestamp_ntz AS inserted_timestamp,
        '2024-11-20' :: timestamp_ntz AS modified_timestamp
    FROM
        {{ ref('silver__flow_evm_contract_names') }}
    
    UNION ALL

    SELECT
        blockchain,
        creator,
        address,
        tag_name,
        tag_type,
        start_date :: DATE,
        NULL AS end_date,
        '2025-03-04' :: TIMESTAMP AS tag_created_at,
        '2000-01-01' :: timestamp_ntz AS inserted_timestamp,
        '2000-01-01' :: timestamp_ntz AS modified_timestamp
    FROM
        {{ ref('silver__bybit_hack_addresses') }}
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(['blockchain','address','tag_name','start_date',]) }} AS dim_tags_id
FROM
    pre_final
