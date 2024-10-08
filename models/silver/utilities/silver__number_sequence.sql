{{ config(
    materialized = 'table',
    cluster_by = 'round(_id,-3)',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

SELECT
    ROW_NUMBER() over (
        ORDER BY
            SEQ4()
    ) :: INT AS _id
FROM
    TABLE(GENERATOR(rowcount => 1000000000))
