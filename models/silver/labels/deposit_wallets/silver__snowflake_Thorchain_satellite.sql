{{ config(
    materialized = 'incremental',
    unique_key = "address",
    incremental_strategy = 'delete+insert',
) }}

WITH distributor_cex AS (
    -- THIS STATEMENT FINDS KNOWN CEX LABELS WITHIN THE BRONZE ADDRESS LABELS TABLE

    SELECT
        system_created_at,
        insert_date,
        blockchain,
        address,
        creator,
        l1_label,
        l2_label,
        address_name,
        project_name
    FROM
        {{ ref('silver__address_labels') }}
    WHERE
        blockchain = 'thorchain'
        AND l1_label = 'cex'
        AND l2_label = 'hot_wallet'
),
possible_sats AS (
    -- THIS STATEMENT LOCATES POTENTIAL SATELLITE WALLETS BASED ON DEPOSIT BEHAVIOR
    SELECT
        DISTINCT *
    FROM
        (
            SELECT
                DISTINCT dc.system_created_at,
                dc.insert_date,
                dc.blockchain,
                xfer.from_address AS address,
                dc.creator,
                dc.address_name,
                dc.project_name,
                dc.l1_label,
                'deposit_wallet' AS l2_label,
                COUNT(
                    DISTINCT project_name
                ) over(
                    PARTITION BY dc.blockchain,
                    xfer.from_address
                ) AS project_count -- how many projects has each from address sent to
            FROM
                {{ source(
                    'thorchain',
                    'transfers'
                ) }}
                xfer
                JOIN distributor_cex dc
                ON dc.address = xfer.to_address
            WHERE
                rune_amount > 0

{% if is_incremental() %}
AND block_timestamp > CURRENT_DATE - 10
{% endif %}
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9
)
),
real_sats AS (
    SELECT
        from_address,
        COUNT(DISTINCT COALESCE(project_name, 'blunts')) AS project_count
    FROM
        {{ source(
            'thorchain',
            'transfers'
        ) }}
        xfer
        LEFT OUTER JOIN distributor_cex dc
        ON dc.address = xfer.to_address
    WHERE
        rune_amount > 0
        AND from_address IN (
            SELECT
                address
            FROM
                possible_sats
        )
        and to_address != 'thor1dheycdevq39qlkxs2a6wuuzyn4aqxhve4qxtxt'

{% if is_incremental() %}
AND block_timestamp > CURRENT_DATE - 10
{% endif %}
GROUP BY
    from_address
),
exclusive_sats AS (
    SELECT
        DISTINCT from_address AS address
    FROM
        real_sats
    WHERE
        project_count = 1
    GROUP BY
        1
),
final_base AS(
    SELECT
        DISTINCT CURRENT_TIMESTAMP AS system_created_at,
        CURRENT_TIMESTAMP AS insert_date,
        blockchain,
        e.address,
        creator,
        l1_label,
        l2_label,
        project_name,
        CONCAT(
            project_name,
            ' deposit_wallet'
        ) AS address_name
    FROM
        exclusive_sats e
        JOIN possible_sats p
        ON e.address = p.address
)
SELECT
    DISTINCT system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label,
    l2_label,
    address_name,
    project_name
FROM
    final_base
WHERE
    address NOT IN (
        SELECT
            DISTINCT address
        FROM
            {{ ref('silver__address_labels') }}
        WHERE
            blockchain = 'thorchain'
    )
