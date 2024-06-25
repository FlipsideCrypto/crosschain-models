{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'OLAS',
    'PURPOSE': 'AI, AGENT, SERVICES',
    } } }
) }}

SELECT
    'ethereum' AS blockchain,
    C.block_number,
    C.block_timestamp,
    C.tx_hash,
    C.origin_function_signature,
    C.origin_from_address,
    C.origin_to_address,
    C.contract_address,
    C.event_index,
    C.event_name,
    C.owner_address,
    C.token_address,
    C.olas_amount_unadj,
    C.olas_amount,
    C.olas_amount_usd,
    C.lp_token_address,
    C.lp_token_amount_unadj,
    C.lp_token_amount,
    C.product_id,
    C.bond_id,
    C.maturity_timestamp,
    CASE
        WHEN r.bond_id IS NULL THEN FALSE
        ELSE TRUE
    END AS is_redeemed,
    C.create_bond_id AS ez_olas_bonding_id,
    C.inserted_timestamp,
    GREATEST(
        COALESCE(
            C.modified_timestamp,
            '1970-01-01' :: TIMESTAMP
        ),
        COALESCE(
            r.modified_timestamp,
            '1970-01-01' :: TIMESTAMP
        )
    ) AS modified_timestamp
FROM
    {{ source(
        'ethereum_silver_olas',
        'create_bond'
    ) }} C
    LEFT JOIN {{ source(
        'ethereum_silver_olas',
        'redeem_bond'
    ) }}
    r USING(bond_id)
