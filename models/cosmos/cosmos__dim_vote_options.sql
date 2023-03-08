{{ config(
    materialized = 'view'
) }}

SELECT
    DISTINCT vote_option AS vote_id,
    CASE
        WHEN vote_option = 1 THEN 'YES'
        WHEN vote_option = 2 THEN 'ABSTAIN'
        WHEN vote_option = 3 THEN 'NO'
        WHEN vote_option = 4 THEN 'NO WITH VETO'
        ELSE 'NULL'
    END AS description
FROM
    {{ ref('cosmos__fact_governance_votes') }}
