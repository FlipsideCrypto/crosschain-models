CREATE TABLE udm_address_labels_new_satellite_staging (LIKE udm_address_labels_new INCLUDING DEFAULTS);

INSERT INTO udm_address_labels_new_satellite_staging (
  WITH distributor_cex AS (
    SELECT
      l.blockchain,
      address,
      project_name,
      l1_label
    FROM
      udm_address_labels_new l
    JOIN
      udm_chain_meta m
    ON
      l.blockchain = m.blockchain
    WHERE
      l1_label = 'cex' AND l2_label = 'hot_wallet' AND m.distributor_labels = true
  ),
  senders AS (
    SELECT
      dc.blockchain,
      "event_from" as address
    FROM
      udm_transfers e
    JOIN
      distributor_cex dc
    ON
      dc.address = e."event_to"
    JOIN 
      udm_address_labels_new al
    ON 
      al.blockchain = e.blockchain
    WHERE
      block_timestamp >= '{0}'
      AND block_timestamp < '{1}'
    GROUP BY 1,2
  ),
  possible_sats AS (
    SELECT
      senders.blockchain,
      senders.address,
      null::text as address_name,
      dc.project_name,
      dc.l1_label,
      'deposit_wallet' as l2_label,
      count(project_name) over(partition by senders.blockchain, senders.address) as project_count
    FROM
      udm_transfers e
    JOIN
      senders
    ON
      senders.address = e."event_from"
      AND senders.blockchain = e.blockchain
    LEFT OUTER JOIN
      distributor_cex dc
    ON
      dc.address = e."event_to"
    WHERE
      block_timestamp >= '{0}'
      AND block_timestamp < '{1}'
    GROUP BY 1,2,3,4,5,6,7,8
  ), exclusive_sats AS (
   (SELECT address FROM possible_sats WHERE project_count = 1 GROUP BY 1)
   EXCEPT
   (SELECT address FROM possible_sats WHERE project_name IS NULL AND project_count = 1 GROUP BY 1)
  )
  SELECT
    blockchain,
    e.address,
    l1_label,
    l2_label,
    project_name,
    address_name
  FROM
   exclusive_sats e
  JOIN
   possible_sats p
  ON
   e.address = p.address
);

-- WE NOW HAVE "TODAYS" SATS
-- WE NEED TO REMOVE ANYTHING THAT IS CLASSIFIED AS SOMETHING ELSE

-- STEP 1
-- REMOVE EXISTING LABELS THAT SHOW UP TODAY
-- WE WILL RECLASSIFY THEM IF THEY SHOW UP AGAIN TODAY AS SATS
DELETE FROM udm_address_labels_new
USING udm_transfers
WHERE
udm_address_labels_new.address = udm_transfers.event_from
AND udm_address_labels_new.blockchain = udm_transfers.blockchain
AND udm_transfers.block_timestamp >= '{0}'
AND udm_transfers.block_timestamp < '{1}'
AND udm_address_labels_new.l2_label = 'deposit_wallet';

-- STEP 2
-- REMOVE SINGLE USE AND OTHER SATELLITES FROM TARGET LABELS
-- IF AN ADDRESS IS NOW A SAT
-- DO NOT TOUCH OTHER LABELS BECAUSE OF PRECEDENCE
DELETE FROM udm_address_labels_new
USING udm_address_labels_new_satellite_staging
WHERE
udm_address_labels_new.blockchain = udm_address_labels_new_satellite_staging.blockchain
AND udm_address_labels_new.address = udm_address_labels_new_satellite_staging.address
AND udm_address_labels_new.l2_label = 'deposit_wallet';

-- STEP 3
-- REMOVE STAGING ADDRESSES THAT HAVE HIGHER PRECENDENCE LABELS ALREADY
-- WE JUST DELETED SINGLE USE AND EXISTING SATS FROM THE TARGET TABLE SO WE WILL ONLY HAVE
-- HIGHER PRECENDCE LABELS
-- THIS WILL DEDUPE SATS AND LEAVE HIGHER PRECENDENCE LABELS ALONE
DELETE FROM udm_address_labels_new_satellite_staging
USING udm_address_labels_new
WHERE
udm_address_labels_new.blockchain = udm_address_labels_new_satellite_staging.blockchain
AND udm_address_labels_new.address = udm_address_labels_new_satellite_staging.address;

-- STEP 4
-- CHAIN-SPECIFIC EXCLUSIONS

-- NEAR WALLETS ARE NOT USED AS SATELLITES
DELETE FROM udm_address_labels_new_satellite_staging
WHERE blockchain = 'near'
AND address LIKE '%.near';

-- NOW IT'S SAFE TO INSERT EVERYTHING THAT'S LEFT IN THE STAGING TABLE
INSERT INTO udm_address_labels_new (SELECT * FROM udm_address_labels_new_satellite_staging);

DROP TABLE udm_address_labels_new_satellite_staging;