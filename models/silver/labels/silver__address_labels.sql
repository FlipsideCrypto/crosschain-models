{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', blockchain, address, creator)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'crosschain', 'labels', 'silver__address_labels']
) }}

WITH subset_table AS (

  SELECT
    *,
    SPLIT(
      SUBSTR(
        record_metadata :key :: STRING,
        2,
        len(
          record_metadata :key :: STRING
        ) -2
      ),
      '-'
    ) [1] :: STRING AS blockchain,
    TO_TIMESTAMP(
      SPLIT(
        SUBSTR(
          record_metadata :key :: STRING,
          2,
          len(
            record_metadata :key :: STRING
          ) -2
        ),
        '-'
      ) [2] :: INT
    ) AS insert_date
  FROM
    {{ source(
      'bronze',
      'address_labels'
    ) }}
  WHERE
    ARRAY_SIZE(
      SPLIT(
        SUBSTR(
          record_metadata :key :: STRING,
          2,
          len(
            record_metadata :key :: STRING
          ) -2
        ),
        '-'
      )
    ) = 3
    AND SPLIT(
      SUBSTR(
        record_metadata :key :: STRING,
        2,
        len(
          record_metadata :key :: STRING
        ) -2
      ),
      '-'
    ) [0] = 'labels'

{% if is_incremental() %}
AND (
  record_metadata :CreateTime :: INT / 1000
) :: TIMESTAMP :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(system_created_at :: DATE))
  FROM
    {{ this }}
)
{% endif %}
),
clean_table AS (
  SELECT
    *,
    SPLIT(
      record_metadata :key :: STRING,
      '-'
    ) [1] :: STRING AS blockchain,
    TO_TIMESTAMP(
      SPLIT(
        record_metadata :key :: STRING,
        '-'
      ) [2] :: INT
    ) AS insert_date
  FROM
    {{ source(
      'bronze',
      'address_labels'
    ) }}
  WHERE
    ARRAY_SIZE(
      SPLIT(
        record_metadata :key :: STRING,
        '-'
      )
    ) = 3
    AND SPLIT(
      record_metadata :key :: STRING,
      '-'
    ) [0] = 'labels'

{% if is_incremental() %}
AND (
  record_metadata :CreateTime :: INT / 1000
) :: TIMESTAMP :: DATE >= (
  SELECT
    DATEADD('day', -1, MAX(system_created_at :: DATE))
  FROM
    {{ this }}
)
{% endif %}
),
base_tables AS (
  SELECT
    *
  FROM
    subset_table
  UNION
  SELECT
    *
  FROM
    clean_table
),
flat_table AS (
  SELECT
    (
      record_metadata :CreateTime :: INT / 1000
    ) :: TIMESTAMP AS system_created_at,
    insert_date,
    blockchain,
    CASE
      WHEN blockchain IN (
        'algorand',
        'solana'
      ) THEN t.value :address :: STRING
      ELSE LOWER(
        t.value :address :: STRING
      )
    END AS address,
    t.value :creator :: STRING AS creator,
    t.value :l1_label :: STRING AS l1_label,
    t.value :l2_label :: STRING AS l2_label,
    t.value :address_name :: STRING AS address_name,
    t.value :project_name :: STRING AS project_name,
    t.value :delete :: STRING AS delete_flag
  FROM
    base_tables,
    LATERAL FLATTEN(
      input => record_content
    ) t qualify(ROW_NUMBER() over(PARTITION BY blockchain, address, creator
  ORDER BY
    insert_date DESC)) = 1
)
SELECT
  system_created_at,
  insert_date,
  blockchain,
  address,
  creator,
  l1_label AS label_type,
  l2_label AS label_subtype,
  address_name,
  project_name,
  delete_flag
FROM
  flat_table
WHERE
  project_name IS NOT NULL
  AND address_name IS NOT NULL
  AND l1_label <> 'project' -- contract creations
