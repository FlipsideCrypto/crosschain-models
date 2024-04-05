{% test sequence_gaps(
    model,
    partition_by,
    column_name
) %}
{%- set partition_sql = partition_by | join(", ") -%}
{%- set previous_column = "prev_" ~ column_name -%}
WITH source AS (
    SELECT
        {{ partition_sql + "," if partition_sql }}
        {{ column_name }},
        LAG(
            {{ column_name }},
            1
        ) over (
            {{ "PARTITION BY " ~ partition_sql if partition_sql }}
            ORDER BY
                {{ column_name }} ASC
        ) AS {{ previous_column }}
    FROM
        {{ model }}
)
SELECT
    {{ partition_sql + "," if partition_sql }}
    {{ previous_column }},
    {{ column_name }},
    {{ column_name }} - {{ previous_column }}
    - 1 AS gap
FROM
    source
WHERE
    {{ column_name }} - {{ previous_column }} <> 1
ORDER BY
    gap DESC {% endtest %}

{% test hour_sequence_gaps(
            model,
            partition_by,
            column_name,
            filter
        ) %}
        {%- set partition_sql = partition_by | join(", ") -%}
        {%- set previous_column = "prev_" ~ column_name -%}
        WITH base_source AS (
            SELECT
                {{ partition_sql }},
                {{ column_name }},
                LAG(
                    {{ column_name }},
                    1
                ) over (
                    {{ "PARTITION BY " ~ partition_sql if partition_sql }}
                    ORDER BY
                        {{ column_name }} ASC
                ) AS {{ previous_column }}
            FROM
                {{ model }}
            {% if filter %}
                WHERE {{ filter }}
            {% endif %}
        )
    SELECT
        {{ partition_sql }},
        {{ previous_column }},
        {{ column_name }},
        DATEDIFF(
            HOUR,
            {{ previous_column }},
            {{ column_name }}
        ) - 1 AS gap
    FROM
        base_source
    WHERE
        gap > 0
    ORDER BY
        gap DESC
{% endtest %}

{% test price_hour_sequence_gaps(
            model,
            partition_by_1,
            partition_by_2,
            column_name,
            filter
        ) %}
        {%- set previous_column = "prev_" ~ column_name -%}
        WITH base_source AS (
            SELECT
                {{ partition_by_1 }}
                {% if partition_by_2 %}, {{ partition_by_2 }} {% endif %},
                {{ column_name }},
                LAG(
                    {{ column_name }},
                    1
                ) over (
                    PARTITION BY LOWER({{ partition_by_1 }}) {% if partition_by_2 %}, {{ partition_by_2 }} {% endif %}
                    ORDER BY
                        {{ column_name }} ASC
                ) AS {{ previous_column }}
            FROM
                {{ model }}
            {% if filter %}
                WHERE {{ filter }}
            {% endif %}
        )
    SELECT
        {{ partition_by_1 }}
        {% if partition_by_2 %}, {{ partition_by_2 }} {% endif %},
        {{ previous_column }},
        {{ column_name }},
        DATEDIFF(
            HOUR,
            {{ previous_column }},
            {{ column_name }}
        ) - 1 AS gap
    FROM
        base_source
    WHERE
        gap > 0
    ORDER BY
        gap DESC
{% endtest %}