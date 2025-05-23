{{ config(
    materialized = 'table',
    unique_key = ['blockchain']
) }}

WITH token_level_scores AS (
    SELECT
        address,
        blockchain,
        ROUND(AVG(legitimacy_score), 4) AS avg_score
    FROM {{ ref('silver__token_metadata') }}
    WHERE legitimacy_score IS NOT NULL
    GROUP BY address, blockchain
),

classified AS (
    SELECT
        blockchain,
        CASE 
            WHEN avg_score >= 0.9 THEN '0.9–1.0'
            WHEN avg_score >= 0.8 THEN '0.8–0.9'
            WHEN avg_score >= 0.7 THEN '0.7–0.8'
            WHEN avg_score >= 0.6 THEN '0.6–0.7'
            WHEN avg_score >= 0.5 THEN '0.5–0.6'
            WHEN avg_score >= 0.4 THEN '0.4–0.5'
            WHEN avg_score >= 0.3 THEN '0.3–0.4'
            WHEN avg_score >= 0.2 THEN '0.2–0.3'
            WHEN avg_score >= 0.1 THEN '0.1–0.2'
            ELSE '0.0–0.1'
        END AS score_bucket
    FROM token_level_scores
),

final AS (
    SELECT
        blockchain,
        COUNT_IF(score_bucket = '0.9–1.0') AS count_90_100,
        COUNT_IF(score_bucket = '0.8–0.9') AS count_80_90,
        COUNT_IF(score_bucket = '0.7–0.8') AS count_70_80,
        COUNT_IF(score_bucket = '0.6–0.7') AS count_60_70,
        COUNT_IF(score_bucket = '0.5–0.6') AS count_50_60,
        COUNT_IF(score_bucket = '0.4–0.5') AS count_40_50,
        COUNT_IF(score_bucket = '0.3–0.4') AS count_30_40,
        COUNT_IF(score_bucket = '0.2–0.3') AS count_20_30,
        COUNT_IF(score_bucket = '0.1–0.2') AS count_10_20,
        COUNT_IF(score_bucket = '0.0–0.1') AS count_00_10,
        COUNT(*) AS total_tokens
    FROM classified
    GROUP BY blockchain
)

SELECT *
FROM final