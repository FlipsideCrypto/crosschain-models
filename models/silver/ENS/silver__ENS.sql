with nft_purchases_transfers as (
select distinct nft_to_address as buyer_address, tokenid, max(block_timestamp) as block_timestamp, max(event_index) as event_index 
from "ETHEREUM"."CORE"."EZ_NFT_TRANSFERS"
where project_name = 'ens'
group by 1,2
  union
select distinct buyer_address, tokenid, max(block_timestamp) as block_timestamp, NULL as event_index from 
"ETHEREUM"."CORE"."EZ_NFT_SALES"
where project_name = 'ens'
group by 1,2
  ),
  
most_recent_p_t as (
select * from nft_purchases_transfers
qualify(ROW_NUMBER() over(PARTITION BY tokenid
ORDER BY
    block_timestamp DESC, event_index DESC)) = 1
),

register_names as (
select 
  origin_from_address as address,
  regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
  SUBSTR(DATA, 259, len(DATA)) AS ens_name_data,
  udf_hex_to_int(segmented_data[1]::string) as cost,
  udf_hex_to_int(segmented_data[2]::string) as expiration_time,
  DATE( expiration_time ) as expiration_date,
  replace(try_hex_decode_string(ens_name_data), chr(0), '') as ens_name,
  concat('0x',substr(topics[2]::string,27,40)) as owner,
  topics[1]::string as label,
  udf_hex_to_int(substring(label,3)) as tokenid,
   block_timestamp,
  tx_hash
from ethereum.core.fact_event_logs 
where contract_address = '0x283af0b28c62c092c9727f1ee09c02ca627eb7f5'
and topics[0]::string in ('0xca6abbe9d7f11422cb6ca7629fbf6fe9efb1c621f71ce8f02b9f2a230097404f') //event name
qualify(ROW_NUMBER() over(PARTITION BY ens_name
    ORDER BY
        expiration_date DESC)) = 1
),
        
renew_names as (
select 
  regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
  udf_hex_to_int(segmented_data[2]::string) as expiration_time,
  DATE( expiration_time ) as expiration_date,
  topics[1]::string as label,
  udf_hex_to_int(substring(label,3)) as tokenid
from ethereum.core.fact_event_logs 
where contract_address = '0x283af0b28c62c092c9727f1ee09c02ca627eb7f5'
and topics[0]::string in ('0x3da24c024582931cfaf8267d8ed24d13a82a8068d5bd337d30ec45cea4e506ae') //event name
qualify(ROW_NUMBER() over(PARTITION BY tokenid
    ORDER BY
        expiration_date DESC)) = 1
),

-- select from {{this}} where tokenid in ^ and not in ^^ 

node_names as (
select distinct tx_hash, topics[1] :: STRING as node
  from ethereum.core.fact_event_logs 
where 
contract_address = '0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e'
and topics[0]::string = '0x335721b01866dc23fbee8b6b2c7b1e14d6f05c28cd35a2c934239f94095602a0'
),

base_nodes as (
select 
  a.address,
  a.segmented_data,
  a.ens_name_data,
  a.cost,
  greatest(coalesce(a.expiration_time,0),coalesce(c.expiration_time,0)) as expiration_time,
  greatest(coalesce(a.expiration_date,date('1997-01-01')),coalesce(c.expiration_date,date('1997-01-01'))) as expiration_date,
  a.ens_name,
  a.owner,
  a.label,
  a.tokenid,
  a.block_timestamp,
  a.tx_hash,
  b.node
  from register_names a
left join node_names b
  on a.tx_hash = b.tx_hash
left join renew_names c
  on a.tokenid = c.tokenid
),

legacy_names as (
select 
a.address,
b.cost,
b.expiration_time,
b.expiration_date,
b.ens_name,
a.owner,
a.label,
a.tokenid,
a.block_timestamp,
a.tx_hash,
a.node
from (select
      block_timestamp,
      tx_hash,
      origin_from_address as address,
      concat('0x',substr(data::string,27,40)) as owner,
      topics[2] :: STRING as label,
      topics[1] :: STRING as node,
      udf_hex_to_int(substring(label,3)) as tokenid
      from ethereum.core.fact_event_logs 
where contract_address in ('0x314159265dd8dbb310642f98f50c066173c1259b','0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e')
       and topics[0]::string = '0xce0457fe73731f824cc272376169235128c118b49d344817417c6d108d155e82'
     qualify(ROW_NUMBER() over(PARTITION BY label
    ORDER BY
        block_timestamp DESC)) = 1) a
left join (
      select 
      distinct 
      block_timestamp,
      regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
      topics[1] :: STRING as label,
      SUBSTR(DATA, 259, len(DATA)) AS ens_name_data,
      udf_hex_to_int(segmented_data[1]::string) as cost,
      udf_hex_to_int(segmented_data[2]::string) as expiration_time,
      DATE( expiration_time ) as expiration_date,
      replace(try_hex_decode_string(ens_name_data), chr(0), '') as ens_name
        from ethereum.core.fact_event_logs 
      where contract_address in ('0x283af0b28c62c092c9727f1ee09c02ca627eb7f5', '0x82994379b1ec951c8e001dfcec2a7ce8f4f39b97', '0xa271897710a2b22f7a5be5feacb00811d960e0b8',
                               '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85', '0xfac7bea255a6990f749363002136af6556b31e04', '0xf0ad5cad05e10572efceb849f6ff0c68f9700455', 
                               '0xb22c1c159d12461ea124b0deb4b5b93020e6ad16')
      and topics[0]::string = '0x3da24c024582931cfaf8267d8ed24d13a82a8068d5bd337d30ec45cea4e506ae'
      qualify(ROW_NUMBER() over(PARTITION BY label
            ORDER BY
            block_timestamp DESC)) = 1) b
on a.label = b.label
),

full_names as (
select 
    address,
    cost,
    expiration_time,
    expiration_date,
    ens_name,
    owner,
    label,
    tokenid,
    block_timestamp,
    tx_hash,
    node
  from base_nodes
  union
  select 
    address,
    cost,
    expiration_time,
    expiration_date,
    ens_name,
    owner,
    label,
    tokenid,
    block_timestamp,
    tx_hash,
    node
  from legacy_names
),

base_table as (
select a.buyer_address as owner, 
  a.tokenid, 
  a.block_timestamp, 
  b.cost,
  b.expiration_time,
  b.expiration_date,
  b.ens_name,
  b.label,
  b.tx_hash,
  b.node
from most_recent_p_t a
left join full_names b
on a.tokenid = b.tokenid
where buyer_address is not null
  and cost is not null
),

set_name as (
SELECT
  distinct
  block_timestamp,
  from_address as wallets,
  substr(input_data,139) as name_part,
  TRY_HEX_DECODE_STRING(name_part :: STRING) as set_ens_name,
  substring(trim(set_ens_name),1, charindex('.eth',set_ens_name)-1) as cleaned_ens
from ethereum.core.fact_transactions 
where origin_function_signature='0xc47f0027'
and to_address='0x084b1c3c81545d370f3634392de611caabff8148'
qualify(ROW_NUMBER() over(PARTITION BY wallets
    ORDER BY
        block_timestamp DESC)) = 1
),

info_table as (
  SELECT
distinct 
x.tx_hash, 
x.block_timestamp,
from_address,
to_address,
input_data,
contract_name,
event_name,
topics[1]::string as node,
    HEX_ENCODE('com.twitter') as twitter,
    charindex(lower(twitter), input_data) as twitter_index,
    case when twitter_index = 0 then NULL else
    substr(input_data,twitter_index+128,128) end as twitter_name_part,
    TRY_HEX_DECODE_STRING(twitter_name_part :: STRING) as twitter_translate,
    
        HEX_ENCODE('avatar') as avatar,
    charindex(lower(avatar), input_data) as avatar_index,
    case when avatar_index = 0 then NULL else
    substr(input_data,avatar_index+128,256) end as avatar_name_part,
    case when avatar_index = 0 then NULL else
    coalesce(TRY_HEX_DECODE_STRING(substr(input_data,avatar_index+128,256) :: STRING), TRY_HEX_DECODE_STRING(substr(input_data,avatar_index+128,128) :: STRING)) end as avatar_translate,
    
        HEX_ENCODE('com.discord') as discord,
    charindex(lower(discord), input_data) as discord_index,
    case when discord_index = 0 then NULL else
    substr(input_data,discord_index+128,128) end as discord_name_part,
    TRY_HEX_DECODE_STRING(discord_name_part :: STRING) as discord_translate,
    
        HEX_ENCODE('com.github') as github,
    charindex(lower(github), input_data) as github_index,
    case when github_index = 0 then NULL else
    substr(input_data,github_index+128,128) end as github_name_part,
    TRY_HEX_DECODE_STRING(github_name_part :: STRING) as github_translate,
    
        HEX_ENCODE('email') as email,
    charindex(lower(email), input_data) as email_index,
    case when email_index = 0 then NULL else
    substr(input_data,email_index+128,128) end as email_name_part,
    TRY_HEX_DECODE_STRING(email_name_part :: STRING) as email_translate,
    
        HEX_ENCODE('url') as url,
    charindex(lower(url), input_data) as url_index,
    case when url_index = 0 then NULL else
    substr(input_data,url_index+128,128) end as url_name_part,
    TRY_HEX_DECODE_STRING(url_name_part :: STRING) as url_translate,
    
            HEX_ENCODE('description') as description,
    charindex(lower(description), input_data) as description_index,
    case when description_index = 0 then NULL else
    substr(input_data,description_index+128,128) end as description_name_part,
    TRY_HEX_DECODE_STRING(description_name_part :: STRING) as description_translate,
    
            HEX_ENCODE('notice') as notice,
    charindex(lower(notice), input_data) as notice_index,
    case when notice_index = 0 then NULL else
    substr(input_data,notice_index+128,128) end as notice_name_part,
    TRY_HEX_DECODE_STRING(notice_name_part :: STRING) as notice_translate,
    
            HEX_ENCODE('keywords') as keywords,
    charindex(lower(keywords), input_data) as keywords_index,
    case when keywords_index = 0 then NULL else
    substr(input_data,keywords_index+128,128) end as keywords_name_part,
    TRY_HEX_DECODE_STRING(keywords_name_part :: STRING) as keywords_translate,
    
            HEX_ENCODE('com.reddit') as reddit,
    charindex(lower(reddit), input_data) as reddit_index,
    case when reddit_index = 0 then NULL else
    substr(input_data,reddit_index+128,128) end as reddit_name_part,
    TRY_HEX_DECODE_STRING(reddit_name_part :: STRING) as reddit_translate,
    
            HEX_ENCODE('org.telegram') as telegram,
    charindex(lower(telegram), input_data) as telegram_index,
    case when telegram_index = 0 then NULL else
    substr(input_data,telegram_index+128,128) end as telegram_name_part,
    TRY_HEX_DECODE_STRING(telegram_name_part :: STRING) as telegram_translate,
    
                HEX_ENCODE('com.opensea') as opensea,
    charindex(lower(opensea), input_data) as opensea_index,
    case when opensea_index = 0 then NULL else
    substr(input_data,opensea_index+128,128) end as opensea_name_part,
    TRY_HEX_DECODE_STRING(opensea_name_part :: STRING) as opensea_translate,
    
                HEX_ENCODE('com.rarible') as rarible,
    charindex(lower(rarible), input_data) as rarible_index,
    case when rarible_index = 0 then NULL else
    substr(input_data,rarible_index+128,128) end as rarible_name_part,
    TRY_HEX_DECODE_STRING(rarible_name_part :: STRING) as rarible_translate,
    
                HEX_ENCODE('com.superrare') as superrare,
    charindex(lower(superrare), input_data) as superrare_index,
    case when superrare_index = 0 then NULL else
    substr(input_data,superrare_index+128,128) end as superrare_name_part,
    TRY_HEX_DECODE_STRING(superrare_name_part :: STRING) as superrare_translate
    
  from ethereum.core.fact_transactions x
  join ethereum.core.fact_event_logs y on x.tx_hash=y.tx_hash
  where 
  y.contract_address = '0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41'
and y.topics[0]::string ='0xd8c9334b1a9c2f9da342a0a2b32629c1a229b6445dad78947f674b44444a7550'
  ),

--   same as above ^^^ but check to make sure we are only pulling once between

  
info_table_small as (
select 
block_timestamp as last_update_info,
from_address as info_updater,
node,
max(twitter_translate) over (partition by node order by block_timestamp asc) as twitter,
max(avatar_translate) over (partition by node order by block_timestamp asc) as avatar,
max(discord_translate) over (partition by node order by block_timestamp asc) as discord,
max(github_translate) over (partition by node order by block_timestamp asc) as github,
max(email_translate) over (partition by node order by block_timestamp asc) as email,
max(url_translate) over (partition by node order by block_timestamp asc) as url,
max(description_translate) over (partition by node order by block_timestamp asc) as description,
max(notice_translate) over (partition by node order by block_timestamp asc) as notice,
max(keywords_translate) over (partition by node order by block_timestamp asc) as keywords,
max(reddit_translate) over (partition by node order by block_timestamp asc) as reddit,
max(telegram_translate) over (partition by node order by block_timestamp asc) as telegram,
max(opensea_translate) over (partition by node order by block_timestamp asc) as opensea,
max(rarible_translate) over (partition by node order by block_timestamp asc) as rarible,
max(superrare_translate) over (partition by node order by block_timestamp asc) as superrare
from info_table
//order by from_address, node, block_timestamp desc
qualify(ROW_NUMBER() over(PARTITION BY node
ORDER BY
    block_timestamp DESC)) = 1
),

full_ens_table as (
select 
          A.owner,
        A.tokenid,
        A.block_timestamp,
        A.cost,
        A.expiration_time,
        A.expiration_date,
        A.ens_name,
        A.label,
        A.tx_hash,
        b.*
  
  from base_table A
left join info_table_small b 
on a.node = b.node
),

ens_table_small as (
select 
  block_timestamp,
  tx_hash,
  owner, 
  tokenid, 
  max(ens_name) over (partition by tokenid order by block_timestamp asc) as ens_name,
  max(cost :: string) over (partition by tokenid order by block_timestamp asc) as cost,
  max(expiration_time) over (partition by tokenid order by block_timestamp asc) as expiration_time,
  max(expiration_date) over (partition by tokenid order by block_timestamp asc) as expiration_date,
  max(label) over (partition by tokenid order by block_timestamp asc) as label,
  max(node) over (partition by tokenid order by block_timestamp asc) as node,
  max(last_update_info) over (partition by tokenid order by block_timestamp asc) as last_update_info,
  max(info_updater) over (partition by tokenid order by block_timestamp asc) as info_updater,
  max(twitter) over (partition by tokenid order by block_timestamp asc) as twitter,
  max(avatar) over (partition by tokenid order by block_timestamp asc) as avatar,
  max(discord) over (partition by tokenid order by block_timestamp asc) as discord,
  max(github) over (partition by tokenid order by block_timestamp asc) as github,
  max(email) over (partition by tokenid order by block_timestamp asc) as email,
  max(url) over (partition by tokenid order by block_timestamp asc) as url,
  max(description) over (partition by tokenid order by block_timestamp asc) as description,
  max(notice) over (partition by tokenid order by block_timestamp asc) as notice,
  max(keywords) over (partition by tokenid order by block_timestamp asc) as keywords,
  max(reddit) over (partition by tokenid order by block_timestamp asc) as reddit,
  max(telegram) over (partition by tokenid order by block_timestamp asc) as telegram,
  max(opensea) over (partition by tokenid order by block_timestamp asc) as opensea,
  max(rarible) over (partition by tokenid order by block_timestamp asc) as rarible,
  max(superrare) over (partition by tokenid order by block_timestamp asc) as superrare
from full_ens_table
qualify(ROW_NUMBER() over(PARTITION BY tokenid
ORDER BY
    block_timestamp DESC)) = 1
  
),
        ens_table_setname as (
        select 
          distinct
            a.block_timestamp,
            a.tx_hash,
            a.owner, 
            a.tokenid, 
            a.ens_name,
            case when b.set_ens_name is NULL then 'N' else 'Y' end as ens_set,
            a.cost,
            a.expiration_time,
            a.expiration_date,
            a.label,
            a.node,
            a.last_update_info,
            a.info_updater,
            a.twitter,
            a.avatar,
            a.discord,
            a.github,
            a.email,
            a.url,
            a.description,
            a.notice,
            a.keywords,
            a.reddit,
            a.telegram,
            a.opensea,
            a.rarible,
            a.superrare
         from ens_table_small a
          left join set_name b
          on a.owner = b.wallets and a.ens_name = b.cleaned_ens
        )
select 
*
from ens_table_setname
