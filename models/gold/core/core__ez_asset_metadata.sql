{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    *
FROM
    {{ ref('price__ez_asset_metadata') }}
