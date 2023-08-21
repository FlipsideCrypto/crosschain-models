{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    *
FROM
    {{ ref('price__dim_asset_metadata') }}
