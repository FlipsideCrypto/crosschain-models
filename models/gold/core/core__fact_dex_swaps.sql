{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    *
FROM
    {{ ref('defi__fact_dex_swaps') }}
