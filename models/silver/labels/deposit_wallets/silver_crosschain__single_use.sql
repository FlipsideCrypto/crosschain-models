CREATE TABLE udm_address_labels_single_use_staging (LIKE udm_address_labels INCLUDING DEFAULTS);

-- STEP1: CALCULATE ALL TIME CORRECT SINGLE USE
INSERT INTO udm_address_labels_single_use_staging (
  WITH single_senders AS (
    SELECT * FROM (
      SELECT
        e.blockchain,
        event_from,
        max(block_timestamp) as sent_at,
        count(1) as count
      FROM
        udm_transfers e
      JOIN
        udm_chain_meta m
      ON
        e.blockchain = m.blockchain
      WHERE
        m.distributor_labels = true
        AND block_timestamp >= (
          SELECT
          min(metric_date)
          FROM
          udm_stats s
          JOIN
          udm_chain_meta m
          ON
          s.chain = m.blockchain
          WHERE
          m.distributor_labels = true
        )
        GROUP BY 1,2
    )
    WHERE count = 1
  ),
  single_receivers AS (
    SELECT * FROM (
      SELECT
        e.blockchain,
        event_to,
        max(block_timestamp) as received_at,
        count(1) as count
      FROM
        udm_transfers e
      JOIN
        udm_chain_meta m
      ON
        e.blockchain = m.blockchain
      WHERE
        m.distributor_labels = true
        AND block_timestamp >= (
          SELECT
          min(metric_date)
          FROM
          udm_stats s
          JOIN
          udm_chain_meta m
          ON
          s.chain = m.blockchain
          WHERE
          m.distributor_labels = true
        )
        GROUP BY 1,2
    )
    WHERE count = 1
  ), matches AS (
    SELECT
    s.blockchain,
    s.event_from as address,
    sent_at,
    received_at,
    datediff(hour, received_at,sent_at) as hour_diff
    FROM
    single_senders s
    JOIN
    single_receivers r
    ON
    s.event_from = r.event_to
    AND s.blockchain = r.blockchain
  )
  SELECT
    blockchain,
    address,
    NULL::text,
    NULL::text,
    NULL::text,
    'other'::text,
    'other_single_use'::text,
    NULL::text
  FROM matches WHERE hour_diff <= 24
);

-- STEP 2: REMOVE OLD SINGLE USE
DELETE FROM udm_address_labels WHERE l2_label = 'other_single_use';

-- STEP 3: LET OTHER LABELS TAKE PRECEDENCE OVER STAGING
DELETE FROM udm_address_labels_single_use_staging
USING udm_address_labels
WHERE
udm_address_labels.blockchain = udm_address_labels_single_use_staging.blockchain
AND udm_address_labels.address = udm_address_labels_single_use_staging.address;

-- STEP 4
-- CHAIN-SPECIFIC EXCLUSIONS

-- NEAR WALLETS ARE NOT USED AS SINGLE USE
DELETE FROM udm_address_labels_single_use_staging
WHERE blockchain = 'near'
AND address LIKE '%.near';


-- NOW IT'S SAFE TO INSERT REMAINING SINGLE USE LABELS
INSERT INTO udm_address_labels (SELECT * FROM udm_address_labels_single_use_staging);

DROP TABLE udm_address_labels_single_use_staging;