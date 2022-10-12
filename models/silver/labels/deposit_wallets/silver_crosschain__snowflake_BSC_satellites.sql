CREATE OR REPLACE SCHEMA deposit_wallet_bsc; 
USE SCHEMA deposit_wallet_bsc; 
CREATE OR REPLACE TABLE bsc_addr_labels LIKE bronze.prod_address_label_sink_291098491;  
CREATE OR REPLACE TABLE bsc_address_labels_staging (
  system_created_at TIMESTAMP, 
  insert_date TIMESTAMP, 
  blockchain STRING, 
  address STRING,
  creator STRING, 
  l1_label STRING, 
  l2_label STRING, 
  project_name STRING, 
  address_name STRING, 
  primary key (blockchain, address, l1_label)
); 
INSERT INTO bsc_addr_labels (
  WITH flattened AS (  
    SELECT *
      FROM bronze.prod_address_label_sink_291098491,  
    table(flatten(record_content)) AS rc
  )
  
  SELECT 
  record_metadata, 
  value AS rc, 
  _inserted_timestamp 
  FROM flattened
);
INSERT INTO bsc_address_labels_staging (
  WITH distributor_cex AS (
    -- THIS STATEMENT FINDS KNOWN CEX LABELS WITHIN THE BRONZE ADDRESS LABELS TABLE
    SELECT system_created_at, insert_date, blockchain, address, creator, l1_label, l2_label, address_name, project_name 
    FROM "FLIPSIDE_PROD_DB"."SILVER_CROSSCHAIN"."ADDRESS_LABELS"
    WHERE blockchain = 'bsc'
    AND l1_label = 'cex'
    AND l2_label = 'hot_wallet'
  ), 
  possible_sats AS (
    -- THIS STATEMENT LOCATES POTENTIAL SATELLITE WALLETS BASED ON DEPOSIT BEHAVIOR
    SELECT 
    DISTINCT *
    from (
      SELECT
      DISTINCT 
      dc.system_created_at, 
      dc.insert_date,
      dc.blockchain,
      xfer.from_address as address,
      dc.creator, 
      dc.address_name,
      dc.project_name,
      dc.l1_label,
      'deposit_wallet' as l2_label,
      count(distinct project_name) over(partition by dc.blockchain, xfer.from_address) as project_count -- how many projects has each from address sent to
      FROM bsc.core.fact_token_transfers xfer
      JOIN distributor_cex dc ON dc.address = xfer.to_address
      WHERE block_timestamp > current_date - 10
      AND raw_amount > 0
      group by 1,2,3,4,5,6,7,8,9
      
      UNION
      
      SELECT
      DISTINCT 
      dc.system_created_at, 
      dc.insert_date,
      dc.blockchain,
      tr.from_address as address,
      dc.creator, 
      dc.address_name,
      dc.project_name,
      dc.l1_label,
      'deposit_wallet' as l2_label,
      count(distinct project_name) over(partition by dc.blockchain, tr.from_address) as project_count 
      FROM bsc.core.fact_traces tr
      JOIN distributor_cex dc ON dc.address = tr.to_address
      WHERE block_timestamp > current_date - 10
      AND tx_status = 'SUCCESS'
      AND eth_value > 0
      group by 1,2,3,4,5,6,7,8,9
    )
   ),
   real_sats as (
     SELECT 
     from_address,
     COUNT(DISTINCT COALESCE(project_name,'blunts')) as project_count
     FROM bsc.core.fact_token_transfers xfer
     LEFT OUTER JOIN distributor_cex dc ON dc.address = xfer.to_address
     WHERE block_timestamp > current_date - 10
     AND from_address IN (select address from possible_sats)
     AND raw_amount > 0
     GROUP BY from_address
     
     UNION
     
     SELECT 
     from_address,
     COUNT(DISTINCT COALESCE(project_name,'blunts')) as project_count
     FROM bsc.core.fact_traces tr
     LEFT OUTER JOIN distributor_cex dc ON dc.address = tr.to_address
     WHERE block_timestamp > current_date - 10
     AND from_address IN (select address from possible_sats)
     AND tx_status = 'SUCCESS'
     AND eth_value > 0
     GROUP BY from_address
   ),
   exclusive_sats AS (
    SELECT from_address as address FROM real_sats WHERE project_count = 1 GROUP BY 1
  )
  
  SELECT
  system_created_at, 
  insert_date, 
  blockchain,
  e.address,
  creator, 
  l1_label,
  l2_label,
  project_name,
  concat(project_name, ' deposit_wallet') as address_name
  FROM
  exclusive_sats e
  JOIN
  possible_sats p
  ON
  e.address = p.address
);
-- WE NOW HAVE "TODAYS" SATS
-- WE NEED TO REMOVE ANYTHING THAT IS CLASSIFIED AS SOMETHING ELSE
DELETE FROM bsc_address_labels_staging l  
USING crosschain.address_labels t
WHERE l.address = t.address
AND t.insert_date :: date >= current_date - 10
AND t.insert_date :: date < current_date
AND l.l2_label = 'deposit_wallet'; 
INSERT INTO silver_crosschain.address_labels (system_created_at, insert_date, blockchain, address, creator, l1_label, l2_label, address_name, project_name)
SELECT * FROM deposit_wallet_bsc.bsc_address_labels_staging