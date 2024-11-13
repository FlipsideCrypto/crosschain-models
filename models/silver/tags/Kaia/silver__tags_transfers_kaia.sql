{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', address, start_date)",
    incremental_strategy = 'delete+insert',
    tags = ['monthly']
) }}

{% if is_incremental() %}
WITH transfer_volume AS (

    SELECT
        DISTINCT from_address AS address,
        MIN(DATE_TRUNC('day', block_timestamp)) AS start_date
    FROM
        {{ source(
            'kaia_core',
            'ez_token_transfers'
        ) }}
    WHERE
        block_timestamp >= CURRENT_DATE - 30
        AND amount > 0
    GROUP BY
        from_address
),
current_tagged AS (
    SELECT
        *
    FROM
        {{ this }}
    WHERE
        end_date IS NULL
),
additions AS (
    SELECT
        DISTINCT 'kaia' AS blockchain,
        'flipside' AS creator,
        address,
        'token transfer volume > 0' AS tag_name,
        'activity' AS tag_type,
        start_date :: DATE AS start_date,
        NULL AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['address','start_date']) }} AS tags_token_transfer_volume_id,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        transfer_volume
    WHERE
        address NOT IN (
            SELECT
                DISTINCT address
            FROM
                current_tagged
        )
),
cap_end_date AS (
    SELECT
        DISTINCT blockchain,
        creator,
        address,
        tag_name,
        tag_type,
        start_date :: DATE AS start_date,
        DATE_TRUNC(
            'DAY',
            CURRENT_DATE
        ) :: DATE AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['address','start_date']) }} AS tags_token_transfer_volume_id,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        current_tagged
    WHERE
        address NOT IN (
            SELECT
                DISTINCT address
            FROM
                transfer_volume
        )
)
SELECT
    *
FROM
    additions
UNION
SELECT
    *
FROM
    cap_end_date
{% else %}
    WITH transfer_base AS (
        SELECT
            DISTINCT from_address,
            block_timestamp :: DATE AS bt
        FROM
            {{ source(
                'kaia_core',
                'ez_token_transfers'
            ) }}
        WHERE
            amount > 0
    ),
    next_date AS (
        SELECT
            *,
            LEAD(bt) over (
                PARTITION BY from_address
                ORDER BY
                    bt
            ) AS nt,
            DATEDIFF(
                'day',
                bt,
                nt
            ) AS days_between_activity
        FROM
            transfer_base
    ),
    conditional_group AS (
        SELECT
            *,
            conditional_true_event(
                days_between_activity > 30
            ) over (
                PARTITION BY from_address
                ORDER BY
                    bt
            ) AS e
        FROM
            next_date
    ),
    conditional_group_lagged AS (
        SELECT
            *,
            COALESCE(LAG(e) over (PARTITION BY from_address
        ORDER BY
            bt), 0) AS grouping_val
        FROM
            conditional_group
    ),
    final_base AS (
        SELECT
            from_address,
            grouping_val,
            MIN(bt) AS start_date,
            DATEADD('day', 30, MAX(bt)) AS end_date
        FROM
            conditional_group_lagged
        GROUP BY
            1,
            2)
        SELECT
            'kaia' AS blockchain,
            'flipside' AS creator,
            from_address AS address,
            'token transfer volume > 0' AS tag_name,
            'activity' AS tag_type,
            start_date,
            IFF(
                end_date > CURRENT_DATE,
                NULL,
                end_date
            ) AS end_date,
            CURRENT_TIMESTAMP AS tag_created_at,
            SYSDATE() AS inserted_timestamp,
            SYSDATE() AS modified_timestamp,
            {{ dbt_utils.generate_surrogate_key(['address','start_date']) }} AS tags_token_transfer_volume_id,
            '{{ invocation_id }}' AS _invocation_id
        FROM
            final_base
        {% endif %}
