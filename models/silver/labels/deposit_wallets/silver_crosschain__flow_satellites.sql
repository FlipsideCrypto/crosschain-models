CREATE TABLE flow_satellite_staging (LIKE udm_address_labels INCLUDING DEFAULTS);

INSERT INTO flow_satellite_staging (
  WITH distributor_cex AS (
    SELECT
      l.blockchain,
      address,
      project_name,
      symbol,
      l1_label,
      token_contract_address
    FROM
      udm_address_labels l
    JOIN
      udm_chain_meta m
    ON
      l.blockchain = m.blockchain
    WHERE
      l2_label = 'distributor_cex' AND m.distributor_labels = true
  ),
  senders AS (
    SELECT
      dc.blockchain,
      "event_from" as address
    FROM
      gold.flow_transfers e
    JOIN
      distributor_cex dc
    ON
      dc.address = e."event_to"
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
      dc.symbol,
      dc.l1_label,
      'distributor_cex_satellite' as l2_label,
      dc.token_contract_address,
      count(project_name) over(partition by senders.blockchain, senders.address) as project_count
    FROM
      gold.flow_transfers e
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
    address_name,
    project_name,
    symbol,
    l1_label,
    l2_label,
    token_contract_address
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
-- REMOVE SINGLE USE AND OTHER SATELLITES FROM TARGET LABELS
-- IF AN ADDRESS IS NOW A SAT
-- DO NOT TOUCH OTHER LABELS BECAUSE OF PRECEDENCE
DELETE FROM udm_address_labels
USING flow_satellite_staging
WHERE
udm_address_labels.blockchain = flow_satellite_staging.blockchain
AND udm_address_labels.address = flow_satellite_staging.address
AND udm_address_labels.l2_label IN ('other_single_use', 'distributor_cex_satellite');

-- STEP 2
-- REMOVE STAGING ADDRESSES THAT HAVE HIGHER PRECENDENCE LABELS ALREADY
-- WE JUST DELETED SINGLE USE AND EXISTING SATS FROM THE TARGET TABLE SO WE WILL ONLY HAVE
-- HIGHER PRECENDCE LABELS
-- THIS WILL DEDUPE SATS AND LEAVE HIGHER PRECENDENCE LABELS ALONE
DELETE FROM flow_satellite_staging
USING udm_address_labels
WHERE
udm_address_labels.blockchain = flow_satellite_staging.blockchain
AND udm_address_labels.address = flow_satellite_staging.address;


-- NOW IT'S SAFE TO INSERT EVERYTHING THAT'S LEFT IN THE STAGING TABLE
INSERT INTO udm_address_labels (SELECT * FROM flow_satellite_staging);

DROP TABLE flow_satellite_staging;