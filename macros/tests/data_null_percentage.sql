{% test data_null_percentage(
        model,
        column_name,
        threshold
) %}

WITH source_data AS (
    SELECT
        {{ column_name }} AS data_col
    FROM
        {{ model }}
    WHERE 
        _inserted_timestamp = (SELECT MAX(_inserted_timestamp) FROM {{ model }})
)

, null_counts AS (
    SELECT
        COUNT(*) AS total_rows,
        COUNT(CASE WHEN data_col IS NULL THEN 1 END) AS null_rows
    FROM
        source_data
)

SELECT
    null_rows,
    total_rows,
    1.0 * null_rows / total_rows AS null_percentage
FROM
    null_counts
WHERE
    1.0 * null_rows / total_rows > {{ threshold }}

{% endtest %}
