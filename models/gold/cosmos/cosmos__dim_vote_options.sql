{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL' :'COSMOS',
    'PURPOSE': 'GOVERNANCE',
    } } }
) }}

SELECT
    DISTINCT vote_option AS vote_id,
    CASE
        WHEN vote_option = 1 THEN 'YES'
        WHEN vote_option = 2 THEN 'ABSTAIN'
        WHEN vote_option = 3 THEN 'NO'
        WHEN vote_option = 4 THEN 'NO WITH VETO'
        ELSE 'NULL'
    END AS description,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['vote_id']) }} AS dim_vote_options_id
FROM
    {{ ref('cosmos__fact_governance_votes') }}
