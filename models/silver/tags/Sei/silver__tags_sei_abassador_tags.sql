{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', address, start_date)",
    incremental_strategy = 'delete+insert',
) }}

WITH roles AS (

    SELECT
        *
    FROM
        (
            VALUES
                (
                    '⭐️',
                    '238289',
                    'Crew 1',
                    'Crew 1 Foundation',
                    '3'
                ),
                (
                    '⭐️⭐️',
                    '205728',
                    'Crew 2',
                    'Engagement',
                    '8'
                ),
                (
                    '⭐️⭐️⭐️',
                    '205859',
                    'Crew 3',
                    'Exploration',
                    '13'
                ),
                (
                    '⭐️⭐️⭐️',
                    '205925',
                    'Officer 3',
                    'Ecosystem Expertise',
                    '28'
                ),
                (
                    '⭐️⭐️',
                    '205899',
                    'Officer 2',
                    'Sei Study',
                    '23'
                ),
                (
                    '⭐️⭐️⭐️',
                    '238232',
                    'Officer 1',
                    'Ecosystem Intro',
                    '18'
                ),
                (
                    '⭐️⭐️⭐️',
                    '206026',
                    'Captain 3',
                    'Community Connect',
                    '43'
                ),
                (
                    '⭐️⭐️',
                    '206025',
                    'Captain 2',
                    'Inspiring Community',
                    '38'
                ),
                (
                    '🌟',
                    '214906',
                    'Colonel 1',
                    'Leader',
                    '0'
                ),
                (
                    '🌟🌟🌟',
                    '206035',
                    'Major 3',
                    'Sei Showcase',
                    '58'
                ),
                (
                    '🌟',
                    '206031',
                    'Major 1',
                    'Content Creation',
                    '48'
                )
        ) AS A (
            badge,
            campaign_id,
            card_name,
            role_name,
            point
        )
),
glaxy_nft AS (
    SELECT
        DISTINCT tx_id
    FROM
        {{ source(
            'sei_core',
            'fact_msg_attributes'
        ) }}
    WHERE
        attribute_value = 'sei1fmgpz0frux02euxljm80mqa2j0j3078qlaf27yhgcc04vesfp77qcjtmqn'

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
    SELECT
        MAX(start_date)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2023-10-01'
{% endif %}
AND tx_succeeded = 'true'
AND attribute_key = '_contract_address'
),
minter AS (
    SELECT
        block_timestamp,
        tx_id,
        attribute_value AS minter
    FROM
        {{ source(
            'sei_core',
            'fact_msg_attributes'
        ) }}
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                glaxy_nft
        )

{% if is_incremental() %}
AND block_timestamp :: DATE >= (
    SELECT
        MAX(start_date)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2023-10-01'
{% endif %}
AND attribute_key = 'minter'
),
tokens AS (
    SELECT
        A.block_timestamp,
        A.tx_id,
        minter,
        attribute_value AS token_id
    FROM
        minter A
        JOIN {{ source(
            'sei_core',
            'fact_msg_attributes'
        ) }}
        b
        ON A.tx_id = b.tx_id
    WHERE
        attribute_key = 'token_id'

{% if is_incremental() %}
AND b.block_timestamp :: DATE >= (
    SELECT
        MAX(start_date)
    FROM
        {{ this }}
)
{% else %}
    AND b.block_timestamp :: DATE >= '2023-10-01'
{% endif %}
),
campaign AS (
    SELECT
        A.block_timestamp,
        A.tx_id,
        minter,
        token_id,
        attribute_value AS campaign
    FROM
        tokens A
        JOIN {{ source(
            'sei_core',
            'fact_msg_attributes'
        ) }}
        b
        ON A.tx_id = b.tx_id
    WHERE
        attribute_key = 'campaign_id'
)
SELECT
    'sei' AS blockchain,
    'flipside' AS creator,
    minter AS address,
    CONCAT(
        card_name,
        ': ',
        role_name
    ) AS tag_name,
    'sei ambassador' AS tag_type,
    A.block_timestamp AS start_date,
    NULL AS end_date,
    CURRENT_TIMESTAMP AS tag_created_at,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['address','start_date']) }} AS tags_sei_abassador_tags_id,
    '{{ invocation_id }}' as _invocation_id
FROM
    campaign A
    JOIN roles b
    ON A.campaign = b.campaign_id
