{{ config(
    materialized = 'incremental',
    full_refresh = false
) }}

WITH base AS (

    SELECT
        groupid,
        LISTAGG(
            id :: INT,
            ','
        ) ids
    FROM
        (
            SELECT
                conditional_true_event(
                    CASE
                        WHEN rn_mod = 1 THEN TRUE
                        ELSE FALSE
                    END
                ) over (
                    ORDER BY
                        id
                ) groupid,
                id
            FROM
                (
                    SELECT
                        id,
                        MOD(ROW_NUMBER() over(
                    ORDER BY
                        id), 100) rn_mod
                    FROM
                        (
                            SELECT
                                DISTINCT id :: INT id,
                                CURRENT_DATE() :: DATE run_date
                            FROM
                                {{ ref('silver__asset_metadata_coin_market_cap') }}

{% if is_incremental() %}
WHERE
    id NOT IN (
        SELECT
            cmc_id :: INT
        FROM
            silver.coin_market_cap_cryptocurrency_info_failures
    )
{% endif %}

{% if is_incremental() %}
EXCEPT
SELECT
    cmc_id,
    _inserted_timestamp :: DATE
FROM
    silver.coin_market_cap_cryptocurrency_info
{% endif %}
)
ORDER BY
    id
)
)
GROUP BY
    groupID
)
SELECT
    ids,
    ethereum.streamline.udf_api(
        'GET',
        'https://pro-api.coinmarketcap.com/v2/cryptocurrency/info?id=' || ids || '&aux=urls,logo,description,tags,platform,date_added,notice,status',{ 'X-CMC_PRO_API_KEY':(
            SELECT
                key
            FROM
                crosschain._internal.api_keys
            WHERE
                provider = 'coinmarketcap'
        ) },{}
    ) AS DATA,
    SYSDATE() AS _inserted_timestamp
FROM
    base
