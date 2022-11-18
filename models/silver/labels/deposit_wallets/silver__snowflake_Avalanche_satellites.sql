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
        label_type as l1_label,
        label_subtype as l2_label,
        address_name,
        project_name
    FROM
        {{ ref('silver__address_labels') }}
    WHERE
        blockchain = 'avalanche'
        AND l1_label = 'cex'
        AND l2_label = 'hot_wallet'
        and delete_flag is null
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
                    'avalanche_core',
                    'fact_token_transfers'
                ) }}
                xfer
                JOIN distributor_cex dc
                ON dc.address = xfer.to_address
            WHERE
                raw_amount > 0

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
UNION
SELECT
    DISTINCT dc.system_created_at,
    dc.insert_date,
    dc.blockchain,
    tr.from_address AS address,
    dc.creator,
    dc.address_name,
    dc.project_name,
    dc.l1_label,
    'deposit_wallet' AS l2_label,
    COUNT(
        DISTINCT project_name
    ) over(
        PARTITION BY dc.blockchain,
        tr.from_address
    ) AS project_count
FROM
    {{ source(
        'avalanche_core',
        'fact_traces'
    ) }}
    tr
    JOIN distributor_cex dc
    ON dc.address = tr.to_address
WHERE
    tx_status = 'SUCCESS'
    AND avax_value > 0

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
        COALESCE(project_name, 'blunts') AS project_names
    FROM
        {{ source(
            'avalanche_core',
            'fact_token_transfers'
        ) }}
        xfer
        LEFT OUTER JOIN distributor_cex dc
        ON dc.address = xfer.to_address
    WHERE
        from_address IN (
            SELECT
                address
            FROM
                possible_sats
        )
        AND raw_amount > 0

{% if is_incremental() %}
AND block_timestamp > CURRENT_DATE - 10
{% endif %}
UNION
SELECT
    from_address,
    COALESCE(project_name, 'blunts') AS project_names
FROM
    {{ source(
        'avalanche_core',
        'fact_traces'
    ) }}
    tr
    LEFT OUTER JOIN distributor_cex dc
    ON dc.address = tr.to_address
WHERE
    from_address IN (
        SELECT
            address
        FROM
            possible_sats
    )
    AND tx_status = 'SUCCESS'
    AND avax_value > 0

{% if is_incremental() %}
AND block_timestamp > CURRENT_DATE - 10
{% endif %}
),
project_counts as (
    select distinct from_address, 
    count(distinct project_names) as project_count
    from real_sats
    group by from_address
),
exclusive_sats AS (
    SELECT
        DISTINCT from_address AS address
    FROM
        project_counts
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
            blockchain = 'avalanche'
            and delete_flag is null
    )
