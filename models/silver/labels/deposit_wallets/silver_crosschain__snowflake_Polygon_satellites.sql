CREATE OR REPLACE SCHEMA deposit_wallet_poly; 

USE SCHEMA deposit_wallet_poly; 

CREATE OR REPLACE TABLE poly_addr_labels LIKE bronze.prod_address_label_sink_291098491;  

CREATE OR REPLACE TABLE poly_address_labels_staging (
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

INSERT INTO poly_addr_labels (
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

INSERT INTO poly_address_labels_staging (
  WITH distributor_cex AS (
    -- THIS STATEMENT FINDS KNOWN CEX LABELS WITHIN THE BRONZE ADDRESS LABELS TABLE
    SELECT
      (
           record_metadata :CreateTime :: INT / 1000
      ) :: TIMESTAMP AS system_created_at,
      split(substr(record_metadata:key::string, 2, len(record_metadata:key::string)-2),'-')[1]::string AS blockchain,
      to_timestamp(split(substr(record_metadata:key::string, 2, len(record_metadata:key::string)-2),'-')[2]::int) as insert_date, 
      LOWER(t.value :address :: STRING) AS address, 
      t.value :creator :: STRING AS creator,
      t.value :l1_label :: STRING AS l1_label, 
      t.value :l2_label :: STRING AS l2_label, 
      t.value :address_name :: STRING AS address_name, 
      t.value :project_name :: STRING AS project_name
    
    FROM bronze.prod_address_label_sink_291098491, 
        LATERAL FLATTEN (
            input => record_content    
        ) t

    WHERE split(substr(record_metadata:key::string, 2, len(record_metadata:key::string)-2),'-')[1]::string = 'polygon'
    AND t.value:l1_label = 'cex' 
    AND t.value:l2_label = 'hot_wallet'
  ),  
  
  senders AS (
    -- THIS STATEMENT LOCATES THE KNOWN CEX ADDRESSES IN THE EVENT TABLES
    SELECT
      dc.blockchain,
      origin_address as address
    
    FROM polygon.udm_events poly
    
    JOIN distributor_cex dc
    ON dc.address = poly.origin_address
    
    WHERE block_timestamp >= current_date - 365
    AND block_timestamp < current_date
    GROUP BY dc.blockchain, origin_address
  ),   
  
  possible_sats AS (
    -- THIS STATEMENT LOCATES POTENTIAL SATELLITE WALLETS BASED ON BEHAVIOR
    SELECT
      dc.system_created_at, 
      dc.insert_date,
      senders.blockchain,
      senders.address,
      dc.creator, 
      dc.address_name,
      dc.project_name,
      dc.l1_label,
      'deposit_wallet' as l2_label,
      count(project_name) over(partition by senders.blockchain, senders.address) as project_count
    
    FROM polygon.transactions e
    
    JOIN senders
    ON senders.address = e.from_address
    
    LEFT OUTER JOIN distributor_cex dc
    ON dc.address = e.to_address
    
    WHERE block_timestamp >= current_date - 365
    AND block_timestamp < current_date
    GROUP BY 1,2,3,4,5,6,7,8, 9
  ),  
  
  exclusive_sats AS (
   (SELECT address FROM possible_sats WHERE project_count = 1 GROUP BY 1)
   EXCEPT
   (SELECT address FROM possible_sats WHERE project_name IS NULL AND project_count = 1 GROUP BY 1)
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

DELETE FROM poly_address_labels_staging l  
USING crosschain.address_labels t

WHERE l.address = t.address
AND t.insert_date :: date >= current_date - 365
AND t.insert_date :: date < current_date
AND l.l2_label = 'deposit_wallet'; 

INSERT INTO silver_crosschain.address_labels (system_created_at, insert_date, blockchain, address, creator, l1_label, l2_label, address_name, project_name)
SELECT * FROM deposit_wallet_poly.poly_address_labels_staging