{{ config(
  materialized = 'incremental',
  unique_key = "address",
  incremental_strategy = 'merge',
  cluster_by = 'block_timestamp::DATE',
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

WITH axelar_base AS (

  SELECT
    tx_from AS address,
    block_timestamp
  FROM
    {{ source(
      'axelar_core',
      'fact_transactions'
    ) }}
  WHERE
    tx_succeeded = TRUE
    AND len(tx_from) > 32

{% if is_incremental() %}
AND block_timestamp >= (
  SELECT
    MAX(
      block_timestamp :: DATE -2
    )
  FROM
    {{ this }}
)
{% endif %}
UNION ALL
SELECT
  sender AS address,
  block_timestamp
FROM
  {{ source(
    'axelar_core',
    'fact_transfers'
  ) }}
WHERE
  tx_succeeded = TRUE
  AND len(sender) > 32
  AND REGEXP_LIKE(sender, '[a-z].*')

{% if is_incremental() %}
AND block_timestamp >= (
  SELECT
    MAX(
      block_timestamp :: DATE -2
    )
  FROM
    {{ this }}
)
{% endif %}
UNION ALL
SELECT
  receiver AS address,
  block_timestamp
FROM
  {{ source(
    'axelar_core',
    'fact_transfers'
  ) }}
WHERE
  tx_succeeded = TRUE
  AND len(receiver) > 32
  AND REGEXP_LIKE(receiver, '[a-z].*')

{% if is_incremental() %}
AND block_timestamp >= (
  SELECT
    MAX(
      block_timestamp :: DATE -2
    )
  FROM
    {{ this }}
)
{% endif %}
UNION ALL
SELECT
  foreign_address AS address,
  block_timestamp
FROM
  {{ source(
    'axelar_core',
    'fact_transfers'
  ) }}
WHERE
  tx_succeeded = TRUE
  AND len(foreign_address) > 32
  AND REGEXP_LIKE(
    foreign_address,
    '[a-z].*'
  )

{% if is_incremental() %}
AND block_timestamp >= (
  SELECT
    MAX(
      block_timestamp :: DATE -2
    )
  FROM
    {{ this }}
)
{% endif %}
),
osmo_base AS (
  SELECT
    tx_from AS address,
    block_timestamp
  FROM
    {{ source(
      'osmosis_core',
      'fact_transactions'
    ) }}
  WHERE
    tx_succeeded = TRUE
    AND len(tx_from) > 32

{% if is_incremental() %}
AND block_timestamp >= (
  SELECT
    MAX(
      block_timestamp :: DATE -2
    )
  FROM
    {{ this }}
)
{% endif %}
UNION ALL
SELECT
  sender AS address,
  block_timestamp
FROM
  {{ source(
    'osmosis_core',
    'fact_transfers'
  ) }}
WHERE
  tx_succeeded = TRUE
  AND len(sender) > 32
  AND REGEXP_LIKE(sender, '[a-z].*')

{% if is_incremental() %}
AND block_timestamp >= (
  SELECT
    MAX(
      block_timestamp :: DATE -2
    )
  FROM
    {{ this }}
)
{% endif %}
UNION ALL
SELECT
  receiver AS address,
  block_timestamp
FROM
  {{ source(
    'osmosis_core',
    'fact_transfers'
  ) }}
WHERE
  tx_succeeded = TRUE
  AND len(receiver) > 32
  AND REGEXP_LIKE(receiver, '[a-z].*')

{% if is_incremental() %}
AND block_timestamp >= (
  SELECT
    MAX(
      block_timestamp :: DATE -2
    )
  FROM
    {{ this }}
)
{% endif %}
UNION ALL
SELECT
  foreign_address AS address,
  block_timestamp
FROM
  {{ source(
    'osmosis_core',
    'fact_transfers'
  ) }}
WHERE
  tx_succeeded = TRUE
  AND len(foreign_address) > 32
  AND REGEXP_LIKE(
    foreign_address,
    '[a-z].*'
  )

{% if is_incremental() %}
AND block_timestamp >= (
  SELECT
    MAX(
      block_timestamp :: DATE -2
    )
  FROM
    {{ this }}
)
{% endif %}
),
cosmo_base AS (
  SELECT
    tx_from AS address,
    block_timestamp
  FROM
    {{ source(
      'cosmos_core',
      'fact_transactions'
    ) }}
  WHERE
    tx_succeeded = TRUE
    AND len(tx_from) > 32

{% if is_incremental() %}
AND block_timestamp >= (
  SELECT
    MAX(
      block_timestamp :: DATE -2
    )
  FROM
    {{ this }}
)
{% endif %}
UNION ALL
SELECT
  sender AS address,
  block_timestamp
FROM
  {{ source(
    'cosmos_core',
    'fact_transfers'
  ) }}
WHERE
  tx_succeeded = TRUE
  AND len(sender) > 32
  AND REGEXP_LIKE(sender, '[a-z].*')

{% if is_incremental() %}
AND block_timestamp >= (
  SELECT
    MAX(
      block_timestamp :: DATE -2
    )
  FROM
    {{ this }}
)
{% endif %}
UNION ALL
SELECT
  receiver AS address,
  block_timestamp
FROM
  {{ source(
    'cosmos_core',
    'fact_transfers'
  ) }}
WHERE
  tx_succeeded = TRUE
  AND len(receiver) > 32
  AND REGEXP_LIKE(receiver, '[a-z].*')

{% if is_incremental() %}
AND block_timestamp >= (
  SELECT
    MAX(
      block_timestamp :: DATE -2
    )
  FROM
    {{ this }}
)
{% endif %}
),
fin AS (
  SELECT
    address,
    SPLIT_PART(
      address,
      '1',
      1
    ) AS blockchain,
    REPLACE(
      address,
      blockchain
    ) AS address_clean,
    LEFT(
      address_clean,
      32
    ) AS address_base,
    MAX(block_timestamp) block_timestamp
  FROM
    (
      SELECT
        address,
        block_timestamp
      FROM
        axelar_base
      UNION ALL
      SELECT
        address,
        block_timestamp
      FROM
        osmo_base
      UNION ALL
      SELECT
        address,
        block_timestamp
      FROM
        cosmo_base
    )
  GROUP BY
    address
)
SELECT
  address,
  blockchain,
  address_base,
  block_timestamp
FROM
  fin
WHERE
  len(address_clean) - len(address_base) = 7
