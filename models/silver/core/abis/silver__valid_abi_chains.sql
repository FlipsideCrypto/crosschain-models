{{ config(
    materialized = 'table',
    tags = ['daily']
) }}

SELECT 
    DISTINCT blockchain as blockchain
FROM {{ ref('silver__abis') }}
order by blockchain asc