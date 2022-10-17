{{ config(
  materialized = 'incremental',
  unique_key = "address",
  incremental_strategy = 'delete+insert',
) }}

WITH distributor_cex AS (
  -- THIS STATEMENT FINDS KNOWN CEX LABELS WITHIN THE BRONZE ADDRESS LABELS TABLE

  SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    label_type AS l1_label,
    label_subtype AS l2_label,
    address_name,
    project_name
  FROM
    {{ source(
      'crosschain_core',
      'address_labels'
    ) }}
  WHERE
    blockchain = 'flow'
    AND l1_label = 'cex'
    AND l2_label = 'hot_wallet'
),
possible_sats AS (
  SELECT
    DISTINCT *
  FROM
    (
      SELECT
        DISTINCT dc.system_created_at,
        dc.insert_date,
        dc.blockchain,
        xfer.sender AS address,
        dc.creator,
        dc.address_name,
        dc.project_name,
        dc.l1_label,
        'deposit_wallet' AS l2_label,
        COUNT(
          DISTINCT project_name
        ) over(
          PARTITION BY dc.blockchain,
          xfer.sender
        ) AS project_count -- how many projects has each from address sent to
      FROM
        {{ source(
          'flow_core',
          'ez_token_transfers'
        ) }}
        xfer
        JOIN distributor_cex dc
        ON dc.address = xfer.recipient
      WHERE
        amount > 0

{% if is_incremental() %}
AND block_timestamp > CURRENT_DATE - 10
{% endif %}
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9
)
),
real_sats AS (
  SELECT
    sender,
    COUNT(DISTINCT COALESCE(project_name, 'blunts')) AS project_count
  FROM
    {{ source(
      'flow_core',
      'ez_token_transfers'
    ) }}
    xfer
    LEFT OUTER JOIN distributor_cex dc
    ON dc.address = xfer.recipient
  WHERE
    amount > 0
    AND sender IN (
      SELECT
        address
      FROM
        possible_sats
    )
    AND recipient != '0xf919ee77447b7497'

{% if is_incremental() %}
AND block_timestamp > CURRENT_DATE - 10
{% endif %}
GROUP BY
  sender
),
exclusive_sats AS (
  SELECT
    DISTINCT sender AS address
  FROM
    real_sats
  WHERE
    project_count = 1
  GROUP BY
    1
),
final_base AS(
  SELECT
    DISTINCT CURRENT_TIMESTAMP AS system_created_at,
    CURRENT_TIMESTAMP AS insert_date,
    blockchain,
    e.address,
    creator,
    l1_label,
    l2_label,
    project_name,
    CONCAT(
      project_name,
      ' deposit_wallet'
    ) AS address_name
  FROM
    exclusive_sats e
    JOIN possible_sats p
    ON e.address = p.address
)
SELECT
  DISTINCT system_created_at,
  insert_date,
  blockchain,
  address,
  creator,
  l1_label,
  l2_label,
  address_name,
  project_name
FROM
  final_base
WHERE
  address NOT IN (
    SELECT
      DISTINCT address
    FROM
      {{ ref('silver_crosschain__address_labels') }}
    WHERE
      blockchain = 'flow'
  )
