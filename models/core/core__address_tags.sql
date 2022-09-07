{{ config(
    materialized = 'view',
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
UNION
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
UNION
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
UNION
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
UNION
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
UNION
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
UNION
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
UNION
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
UNION
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
UNION
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
UNION
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
UNION
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
UNION
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
UNION
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
UNION
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
UNION
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
UNION
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
UNION
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
UNION
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
UNION
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
UNION
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
UNION
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
UNION
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
UNION
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    '2022-09-06'::timestamp AS tag_created_at
FROM
    {{ ref('silver__godmode_nft_minters') }}
UNION
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