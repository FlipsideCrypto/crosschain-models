{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', address, tag_name, start_date)",
    incremental_strategy = 'delete+insert',
) }}
-- We do not want to full refresh this model until we have a historical tags code set up.
-- to full-refresh either include the variable allow_full_refresh: True to command or comment out below code
-- DO NOT FORMAT will break the full refresh code if formatted copy from below
-- {% if execute %}
--   {% if flags.FULL_REFRESH and var('allow_full_refresh', False) != True %}
--       {{ exceptions.raise_compiler_error("Full refresh is not allowed for this model unless the argument \"- -vars 'allow_full_refresh: True'\" is included in the dbt run command.") }}
--   {% endif %}
-- {% endif %}
{% if execute %}
    {% if flags.full_refresh and var(
            'allow_full_refresh',
            False
        ) != True %}
        {{ exceptions.raise_compiler_error("Full refresh is not allowed for this model unless the argument \"- -vars 'allow_full_refresh: True'\" is included in the dbt run command.") }}
    {% endif %}
{% endif %}

WITH t1 AS (
    SELECT
        DISTINCT decoded_flat :to :: STRING AS wallets,
        x.block_timestamp,
        decoded_flat :value /(pow(10, 18)) AS tokens_claimed,
        price AS token_price_usd
    FROM
        {{ source(
            'ethereum_silver',
            'decoded_logs'
        ) }}
        x
        JOIN {{ source(
            'ethereum_price',
            'fact_hourly_token_prices'
        ) }}
        y
        ON x.contract_address = y.token_address
        AND TRUNC(
            block_timestamp,
            'hour'
        ) = y.hour
    WHERE
        origin_function_signature = '0x2e7ba6ef' -- Claim function
        AND event_name = 'Transfer'
        AND tx_status = 'SUCCESS'
),
t2 AS (
    SELECT
        wallets,
        block_timestamp,
        CASE
            WHEN token_price_usd IS NULL THEN 0
            ELSE token_price_usd
        END AS final_usd_price,
        final_usd_price * tokens_claimed AS airdrop_received_in_usd
    FROM
        t1
    WHERE
        wallets NOT IN (
            SELECT
                DISTINCT address
            FROM
                {{ source(
                    'crosschain_core',
                    'dim_labels'
                ) }}
            WHERE
                blockchain = 'ethereum'
        ) -- filtering treasury, funding wallets, etc.
),
t3 AS (
    SELECT
        COUNT (
            DISTINCT wallets
        ) AS total_airdroppers
    FROM
        t2
),
t4 AS (
    SELECT
        total_airdroppers * 0.1 AS top10
    FROM
        t3
),
t5 AS (
    SELECT
        DISTINCT wallets,
        MAX(block_timestamp) AS start_date,
        SUM(airdrop_received_in_usd) AS total_airdrop_received_in_usd,
        RANK() over (
            ORDER BY
                total_airdrop_received_in_usd DESC
        ) AS POSITION
    FROM
        t2
    GROUP BY
        1
),
base_table AS (
    SELECT
        wallets,
        start_date,
        'Airdrop Master' AS tag,
        total_airdrop_received_in_usd
    FROM
        t5,
        t4
    WHERE
        POSITION <= top10
    ORDER BY
        3 DESC
),
new_additions AS (
    SELECT
        DISTINCT 'ethereum' AS blockchain,
        'flipside' AS creator,
        wallets AS address,
        'airdrop master' AS tag_name,
        'wallet' AS tag_type,
        start_date,
        NULL AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at
    FROM
        base_table

{% if is_incremental() %}
WHERE
    address NOT IN (
        SELECT
            DISTINCT address
        FROM
            {{ this }}
    )
{% endif %}
)

{% if is_incremental() %},
cap_end_date AS (
    SELECT
        DISTINCT 'ethereum' AS blockchain,
        'flipside' AS creator,
        address,
        'airdrop master' AS tag_name,
        'wallet' AS tag_type,
        start_date,
        DATE_TRUNC(
            'DAY',
            CURRENT_DATE
        ) :: DATE AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at
    FROM
        {{ this }}
    WHERE
        address NOT IN (
            SELECT
                DISTINCT wallets
            FROM
                base_table
        )
)
{% endif %}
, pre_final AS (
    SELECT
        *
    FROM
        new_additions

    {% if is_incremental() %}
    UNION
    SELECT
        *
    FROM
        cap_end_date
    {% endif %}
)
SELECT 
    *,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }} AS tags_airdrop_master_eth_id,
    '{{ invocation_id }}' as _invocation_id
FROM 
    pre_final