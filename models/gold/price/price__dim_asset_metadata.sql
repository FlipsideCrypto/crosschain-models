{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    token_address,
    id,
    COALESCE(c.symbol,s.symbol) AS symbol,
    name,
    decimals,
    s.blockchain,
    provider,
    GREATEST(COALESCE(s.inserted_timestamp,'2000-01-01'), COALESCE(c.inserted_timestamp,'2000-01-01')) as inserted_timestamp,
    GREATEST(COALESCE(s.modified_timestamp,'2000-01-01'), COALESCE(c.modified_timestamp,'2000-01-01')) as modified_timestamp,
    COALESCE(asset_metadata_all_providers_id, {{ dbt_utils.generate_surrogate_key(['token_address','id','COALESCE(c.symbol,s.symbol)','s.blockchain','provider']) }}) AS dim_asset_metadata_id
FROM {{ ref('silver__asset_metadata_all_providers') }} s
LEFT JOIN {{ ref('core__dim_contracts') }} c 
    ON LOWER(c.address) = s.token_address AND c.blockchain = s.blockchain
QUALIFY(ROW_NUMBER() OVER (PARTITION BY token_address, id, COALESCE(c.symbol,s.symbol), s.blockchain, provider 
    ORDER BY _inserted_timestamp DESC)) = 1