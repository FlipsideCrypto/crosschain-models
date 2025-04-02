{{ config(
    materialized = 'incremental',
    unique_key = "address",
    incremental_strategy = 'delete+insert',
    tags = ['weekly_full_refresh']
) }}

WITH distributor_cex AS (
    -- THIS STATEMENT FINDS KNOWN CEX LABELS WITHIN THE BRONZE ADDRESS LABELS TABLE
    SELECT
        system_created_at,
        insert_date,
        blockchain,
        address,
        creator,
        label_type AS l1_label,
        label_subtype AS l2_label,
        address_name,
        project_name
    FROM
        {{ ref('silver__address_labels') }}
    WHERE
        blockchain = 'sei_evm'
        AND l1_label = 'cex'
        AND l2_label = 'hot_wallet'
        AND delete_flag IS NULL
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
                    'sei_evm_core',
                    'ez_token_transfers'
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
                'sei_evm_core',
                'ez_native_transfers'
            ) }}
            xfer
            JOIN distributor_cex dc
            ON dc.address = xfer.to_address
        WHERE
            amount > 0

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
        COALESCE(
            project_name,
            'blunts'
        ) AS project_names
    FROM
        {{ source(
            'sei_evm_core',
            'ez_token_transfers'
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
    COALESCE(
        project_name,
        'blunts'
    ) AS project_names
FROM
    {{ source(
        'sei_evm_core',
        'ez_native_transfers'
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
    AND amount > 0

{% if is_incremental() %}
AND block_timestamp > CURRENT_DATE - 10
{% endif %}
),
project_counts AS (
    SELECT
        DISTINCT from_address,
        COUNT(
            DISTINCT project_names
        ) AS project_count
    FROM
        real_sats
    GROUP BY
        from_address
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
        LEFT JOIN possible_sats p
        ON e.address = p.address
)
SELECT
    DISTINCT f.system_created_at,
    f.insert_date,
    f.blockchain,
    f.address,
    f.creator,
    f.l1_label,
    f.l2_label,
    f.address_name,
    f.project_name,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['f.address']) }} AS snowflake_sei_evm_satellites_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    final_base f
    LEFT JOIN {{ ref('silver__address_labels') }} A
    ON f.address = A.address
    AND A.blockchain = 'sei_evm'
    AND A.delete_flag IS NULL
WHERE
    A.address IS NULL
