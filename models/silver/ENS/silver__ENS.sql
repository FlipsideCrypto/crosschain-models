{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tokenid, ens_name)",
    incremental_strategy = 'delete+insert',
) }}



WITH 
register_names AS (
    SELECT
        origin_from_address AS address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        SUBSTR(DATA, 259, len(DATA)) AS ens_name_data,
        udf_hex_to_int(
            segmented_data [1] :: STRING
        ) AS cost,
        udf_hex_to_int(
            segmented_data [2] :: STRING
        ) AS expiration_time,
        DATE(
            expiration_time
        ) AS expiration_date,
        REPLACE(TRY_HEX_DECODE_STRING(ens_name_data), CHR(0), '') AS ens_name,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS owner,
        topics [1] :: STRING AS label,
        udf_hex_to_int(SUBSTRING(label, 3)) AS tokenid,
        block_timestamp,
        tx_hash,
        _inserted_timestamp
    FROM
        {{ source(
            'ethereum_silver',
            'logs'
        ) }}
    WHERE
        contract_address = '0x283af0b28c62c092c9727f1ee09c02ca627eb7f5'
        AND topics [0] :: STRING IN ('0xca6abbe9d7f11422cb6ca7629fbf6fe9efb1c621f71ce8f02b9f2a230097404f')

{% if is_incremental() %}
AND _inserted_timestamp > (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY ens_name
ORDER BY
    expiration_date DESC)) = 1
),
renew_names AS (
    SELECT
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        udf_hex_to_int(
            segmented_data [2] :: STRING
        ) AS expiration_time,
        DATE(
            expiration_time
        ) AS expiration_date,
        topics [1] :: STRING AS label,
        udf_hex_to_int(SUBSTRING(label, 3)) AS tokenid
    FROM
        {{ source(
            'ethereum_silver',
            'logs'
        ) }}
    WHERE
        contract_address = '0x283af0b28c62c092c9727f1ee09c02ca627eb7f5'
        AND topics [0] :: STRING IN ('0x3da24c024582931cfaf8267d8ed24d13a82a8068d5bd337d30ec45cea4e506ae')

{% if is_incremental() %}
AND _inserted_timestamp > (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY tokenid
ORDER BY
    expiration_date DESC)) = 1
),
-- select from {{this}} where tokenid in ^ and not in ^^
node_names AS (
    SELECT
        DISTINCT tx_hash,
        topics [1] :: STRING AS node
    FROM
        {{ source(
            'ethereum_silver',
            'logs'
        ) }}
    WHERE
        contract_address = '0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e'
        AND topics [0] :: STRING = '0x335721b01866dc23fbee8b6b2c7b1e14d6f05c28cd35a2c934239f94095602a0'

{% if is_incremental() %}
AND _inserted_timestamp > (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),

base_nodes AS (
    SELECT
        A.address,
        A.cost,
        GREATEST(COALESCE(A.expiration_time, 0), COALESCE(C.expiration_time, 0)) AS expiration_time,
        GREATEST(COALESCE(A.expiration_date, DATE('1997-01-01')), COALESCE(C.expiration_date, DATE('1997-01-01'))) AS expiration_date,
        A.ens_name,
        A.owner,
        A.label,
        A.tokenid,
        A.block_timestamp,
        A.tx_hash,
        A._inserted_timestamp,
        b.node
    FROM
        register_names A
        LEFT JOIN node_names b
        ON A.tx_hash = b.tx_hash
        LEFT JOIN renew_names C
        ON A.tokenid = C.tokenid
),


{% if is_incremental() %}
incremental_base_nodes as (
    select 
    A.owner as address,
    A.cost,
    GREATEST(COALESCE(A.expiration_time, 0), COALESCE(C.expiration_time, 0)) AS expiration_time,
    GREATEST(COALESCE(A.expiration_date, DATE('1997-01-01')), COALESCE(C.expiration_date, DATE('1997-01-01'))) AS expiration_date,
    A.ens_name,
    A.owner,
    A.label,
    A.tokenid,
    A.block_timestamp,
    '' as tx_hash,
    A._inserted_timestamp,
    A.node
    from {{this}} A
    left join renew_names C
    ON A.tokenid = C.tokenid
    where A.tokenid in (select distinct tokenid from renew_names)
    and A.tokenid not in (select distinct tokenid from register_names)

),
{% endif %}

legacy_names AS (
    SELECT
        A.address,
        b.cost,
        b.expiration_time,
        b.expiration_date,
        b.ens_name,
        A.owner,
        A.label,
        A.tokenid,
        A.block_timestamp,
        A.tx_hash,
        A.node,
        A._inserted_timestamp
    FROM
        (
            SELECT
                block_timestamp,
                _inserted_timestamp,
                tx_hash,
                origin_from_address AS address,
                CONCAT('0x', SUBSTR(DATA :: STRING, 27, 40)) AS owner,
                topics [2] :: STRING AS label,
                topics [1] :: STRING AS node,
                udf_hex_to_int(SUBSTRING(label, 3)) AS tokenid
            FROM
                {{ source(
                    'ethereum_silver',
                    'logs'
                ) }}
            WHERE
                contract_address IN (
                    '0x314159265dd8dbb310642f98f50c066173c1259b',
                    '0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e'
                )
                AND topics [0] :: STRING = '0xce0457fe73731f824cc272376169235128c118b49d344817417c6d108d155e82'

{% if is_incremental() %}
AND _inserted_timestamp > (
SELECT
    MAX(_inserted_timestamp)
FROM
    {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY label
ORDER BY
block_timestamp DESC)) = 1
) A
LEFT JOIN (
SELECT
    DISTINCT block_timestamp,
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
    topics [1] :: STRING AS label,
    SUBSTR(DATA, 259, len(DATA)) AS ens_name_data,
    udf_hex_to_int(
        segmented_data [1] :: STRING
    ) AS cost,
    udf_hex_to_int(
        segmented_data [2] :: STRING
    ) AS expiration_time,
    DATE(
        expiration_time
    ) AS expiration_date,
    REPLACE(TRY_HEX_DECODE_STRING(ens_name_data), CHR(0), '') AS ens_name
FROM
    {{ source(
        'ethereum_silver',
        'logs'
    ) }}
WHERE
    contract_address IN (
        '0x283af0b28c62c092c9727f1ee09c02ca627eb7f5',
        '0x82994379b1ec951c8e001dfcec2a7ce8f4f39b97',
        '0xa271897710a2b22f7a5be5feacb00811d960e0b8',
        '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85',
        '0xfac7bea255a6990f749363002136af6556b31e04',
        '0xf0ad5cad05e10572efceb849f6ff0c68f9700455',
        '0xb22c1c159d12461ea124b0deb4b5b93020e6ad16'
    )
    AND topics [0] :: STRING = '0x3da24c024582931cfaf8267d8ed24d13a82a8068d5bd337d30ec45cea4e506ae'

{% if is_incremental() %}
AND _inserted_timestamp > (
SELECT
    MAX(_inserted_timestamp)
FROM
    {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY label
ORDER BY
block_timestamp DESC)) = 1
) b
ON A.label = b.label
),
full_names AS (
SELECT
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
    node,
    _inserted_timestamp
FROM
    base_nodes
UNION
SELECT
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
    node,
    _inserted_timestamp
FROM
    legacy_names
{% if is_incremental() %}
UNION
SELECT
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
    node,
    _inserted_timestamp
FROM
    incremental_base_nodes
{% endif %}
),
nft_purchases_transfers AS (
    SELECT
        DISTINCT 
        tx_hash,
        nft_to_address AS buyer_address,
        tokenid,
        MAX(block_timestamp) AS block_timestamp,
        MAX(event_index) AS event_index
    FROM
        {{ source(
            'ethereum_core',
            'ez_nft_transfers'
        ) }}
    WHERE
        project_name = 'ens'

{% if is_incremental() %}
AND block_timestamp > (
    SELECT
        MAX(block_timestamp)
    FROM
        {{ this }}
)
OR project_name = 'ens' and tokenid in (select distinct tokenid from full_names)
{% endif %}
GROUP BY
    1,
    2,
    3
UNION
SELECT
    DISTINCT 
    tx_hash,
    buyer_address,
    tokenid,
    MAX(block_timestamp) AS block_timestamp,
    NULL AS event_index
FROM
    {{ source(
        'ethereum_core',
        'ez_nft_sales'
    ) }}
WHERE
    project_name = 'ens'

{% if is_incremental() %}
AND block_timestamp > (
    SELECT
        MAX(block_timestamp)
    FROM
        {{ this }}
)
OR project_name = 'ens' and tokenid in (select distinct tokenid from full_names)
{% endif %}
GROUP BY
    1,
    2,
    3
),
most_recent_p_t AS (
    SELECT
        *
    FROM
        nft_purchases_transfers qualify(ROW_NUMBER() over(PARTITION BY tokenid
    ORDER BY
        block_timestamp DESC, event_index DESC)) = 1
),
base_table AS (
SELECT
    A.buyer_address AS owner,
    A.tokenid,
    A.block_timestamp,
    b.cost,
    b.expiration_time,
    b.expiration_date,
    b.ens_name,
    b.label,
    b.tx_hash,
    b.node,
    b._inserted_timestamp
FROM
    most_recent_p_t A
    LEFT JOIN full_names b
    ON A.tokenid = b.tokenid
WHERE
    buyer_address IS NOT NULL
    AND cost IS NOT NULL
),
set_name AS (
SELECT
    DISTINCT block_timestamp,
    from_address AS wallets,
    SUBSTR(
        input_data,
        139
    ) AS name_part,
    TRY_HEX_DECODE_STRING(
        name_part :: STRING
    ) AS set_ens_name,
    SUBSTRING(TRIM(set_ens_name), 1, CHARINDEX('.eth', set_ens_name) -1) AS cleaned_ens
FROM
    {{ source(
        'ethereum_silver',
        'transactions'
    ) }}
WHERE
    origin_function_signature = '0xc47f0027'
    AND to_address = '0x084b1c3c81545d370f3634392de611caabff8148'

{% if is_incremental() %}
AND _inserted_timestamp > (
SELECT
    MAX(_inserted_timestamp)
FROM
    {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY wallets
ORDER BY
block_timestamp DESC)) = 1
),
info_table AS (
SELECT
    DISTINCT x.tx_hash,
    x.block_timestamp,
    from_address,
    to_address,
    input_data,
    contract_name,
    event_name,
    topics [1] :: STRING AS node,
    HEX_ENCODE('com.twitter') AS twitter,
    CHARINDEX(LOWER(twitter), input_data) AS twitter_index,
    CASE
        WHEN twitter_index = 0 THEN NULL
        ELSE SUBSTR(
            input_data,
            twitter_index + 128,
            128
        )
    END AS twitter_name_part,
    TRY_HEX_DECODE_STRING(
        twitter_name_part :: STRING
    ) AS twitter_translate,
    HEX_ENCODE('avatar') AS avatar,
    CHARINDEX(LOWER(avatar), input_data) AS avatar_index,
    CASE
        WHEN avatar_index = 0 THEN NULL
        ELSE SUBSTR(
            input_data,
            avatar_index + 128,
            256
        )
    END AS avatar_name_part,
    CASE
        WHEN avatar_index = 0 THEN NULL
        ELSE COALESCE(
            TRY_HEX_DECODE_STRING(SUBSTR(input_data, avatar_index + 128, 256) :: STRING),
            TRY_HEX_DECODE_STRING(SUBSTR(input_data, avatar_index + 128, 128) :: STRING)
        )
    END AS avatar_translate,
    HEX_ENCODE('com.discord') AS discord,
    CHARINDEX(LOWER(discord), input_data) AS discord_index,
    CASE
        WHEN discord_index = 0 THEN NULL
        ELSE SUBSTR(
            input_data,
            discord_index + 128,
            128
        )
    END AS discord_name_part,
    TRY_HEX_DECODE_STRING(
        discord_name_part :: STRING
    ) AS discord_translate,
    HEX_ENCODE('com.github') AS github,
    CHARINDEX(LOWER(github), input_data) AS github_index,
    CASE
        WHEN github_index = 0 THEN NULL
        ELSE SUBSTR(
            input_data,
            github_index + 128,
            128
        )
    END AS github_name_part,
    TRY_HEX_DECODE_STRING(
        github_name_part :: STRING
    ) AS github_translate,
    HEX_ENCODE('email') AS email,
    CHARINDEX(LOWER(email), input_data) AS email_index,
    CASE
        WHEN email_index = 0 THEN NULL
        ELSE SUBSTR(
            input_data,
            email_index + 128,
            128
        )
    END AS email_name_part,
    TRY_HEX_DECODE_STRING(
        email_name_part :: STRING
    ) AS email_translate,
    HEX_ENCODE('url') AS url,
    CHARINDEX(LOWER(url), input_data) AS url_index,
    CASE
        WHEN url_index = 0 THEN NULL
        ELSE SUBSTR(
            input_data,
            url_index + 128,
            128
        )
    END AS url_name_part,
    TRY_HEX_DECODE_STRING(
        url_name_part :: STRING
    ) AS url_translate,
    HEX_ENCODE('description') AS description,
    CHARINDEX(LOWER(description), input_data) AS description_index,
    CASE
        WHEN description_index = 0 THEN NULL
        ELSE SUBSTR(
            input_data,
            description_index + 128,
            128
        )
    END AS description_name_part,
    TRY_HEX_DECODE_STRING(
        description_name_part :: STRING
    ) AS description_translate,
    HEX_ENCODE('notice') AS notice,
    CHARINDEX(LOWER(notice), input_data) AS notice_index,
    CASE
        WHEN notice_index = 0 THEN NULL
        ELSE SUBSTR(
            input_data,
            notice_index + 128,
            128
        )
    END AS notice_name_part,
    TRY_HEX_DECODE_STRING(
        notice_name_part :: STRING
    ) AS notice_translate,
    HEX_ENCODE('keywords') AS keywords,
    CHARINDEX(LOWER(keywords), input_data) AS keywords_index,
    CASE
        WHEN keywords_index = 0 THEN NULL
        ELSE SUBSTR(
            input_data,
            keywords_index + 128,
            128
        )
    END AS keywords_name_part,
    TRY_HEX_DECODE_STRING(
        keywords_name_part :: STRING
    ) AS keywords_translate,
    HEX_ENCODE('com.reddit') AS reddit,
    CHARINDEX(LOWER(reddit), input_data) AS reddit_index,
    CASE
        WHEN reddit_index = 0 THEN NULL
        ELSE SUBSTR(
            input_data,
            reddit_index + 128,
            128
        )
    END AS reddit_name_part,
    TRY_HEX_DECODE_STRING(
        reddit_name_part :: STRING
    ) AS reddit_translate,
    HEX_ENCODE('org.telegram') AS telegram,
    CHARINDEX(LOWER(telegram), input_data) AS telegram_index,
    CASE
        WHEN telegram_index = 0 THEN NULL
        ELSE SUBSTR(
            input_data,
            telegram_index + 128,
            128
        )
    END AS telegram_name_part,
    TRY_HEX_DECODE_STRING(
        telegram_name_part :: STRING
    ) AS telegram_translate,
    HEX_ENCODE('com.opensea') AS opensea,
    CHARINDEX(LOWER(opensea), input_data) AS opensea_index,
    CASE
        WHEN opensea_index = 0 THEN NULL
        ELSE SUBSTR(
            input_data,
            opensea_index + 128,
            128
        )
    END AS opensea_name_part,
    TRY_HEX_DECODE_STRING(
        opensea_name_part :: STRING
    ) AS opensea_translate,
    HEX_ENCODE('com.rarible') AS rarible,
    CHARINDEX(LOWER(rarible), input_data) AS rarible_index,
    CASE
        WHEN rarible_index = 0 THEN NULL
        ELSE SUBSTR(
            input_data,
            rarible_index + 128,
            128
        )
    END AS rarible_name_part,
    TRY_HEX_DECODE_STRING(
        rarible_name_part :: STRING
    ) AS rarible_translate,
    HEX_ENCODE('com.superrare') AS superrare,
    CHARINDEX(LOWER(superrare), input_data) AS superrare_index,
    CASE
        WHEN superrare_index = 0 THEN NULL
        ELSE SUBSTR(
            input_data,
            superrare_index + 128,
            128
        )
    END AS superrare_name_part,
    TRY_HEX_DECODE_STRING(
        superrare_name_part :: STRING
    ) AS superrare_translate
FROM
    {{ source(
        'ethereum_silver',
        'transactions'
    ) }}
    x
    JOIN {{ source(
        'ethereum_silver',
        'logs'
    ) }}
    y
    ON x.tx_hash = y.tx_hash
WHERE
    y.contract_address = '0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41'
    AND y.topics [0] :: STRING = '0xd8c9334b1a9c2f9da342a0a2b32629c1a229b6445dad78947f674b44444a7550'

{% if is_incremental() %}
AND y._inserted_timestamp > (
SELECT
    MAX(_inserted_timestamp)
FROM
    {{ this }}
)
{% endif %}
),
--   same as above ^^^ but check to make sure we are only pulling once between
info_table_small AS (
SELECT
    block_timestamp AS last_update_info,
    from_address AS info_updater,
    node,
    MAX(twitter_translate) over (
        PARTITION BY node
        ORDER BY
            block_timestamp ASC
    ) AS twitter,
    MAX(avatar_translate) over (
        PARTITION BY node
        ORDER BY
            block_timestamp ASC
    ) AS avatar,
    MAX(discord_translate) over (
        PARTITION BY node
        ORDER BY
            block_timestamp ASC
    ) AS discord,
    MAX(github_translate) over (
        PARTITION BY node
        ORDER BY
            block_timestamp ASC
    ) AS github,
    MAX(email_translate) over (
        PARTITION BY node
        ORDER BY
            block_timestamp ASC
    ) AS email,
    MAX(url_translate) over (
        PARTITION BY node
        ORDER BY
            block_timestamp ASC
    ) AS url,
    MAX(description_translate) over (
        PARTITION BY node
        ORDER BY
            block_timestamp ASC
    ) AS description,
    MAX(notice_translate) over (
        PARTITION BY node
        ORDER BY
            block_timestamp ASC
    ) AS notice,
    MAX(keywords_translate) over (
        PARTITION BY node
        ORDER BY
            block_timestamp ASC
    ) AS keywords,
    MAX(reddit_translate) over (
        PARTITION BY node
        ORDER BY
            block_timestamp ASC
    ) AS reddit,
    MAX(telegram_translate) over (
        PARTITION BY node
        ORDER BY
            block_timestamp ASC
    ) AS telegram,
    MAX(opensea_translate) over (
        PARTITION BY node
        ORDER BY
            block_timestamp ASC
    ) AS opensea,
    MAX(rarible_translate) over (
        PARTITION BY node
        ORDER BY
            block_timestamp ASC
    ) AS rarible,
    MAX(superrare_translate) over (
        PARTITION BY node
        ORDER BY
            block_timestamp ASC
    ) AS superrare
FROM
    info_table qualify(ROW_NUMBER() over(PARTITION BY node
ORDER BY
    block_timestamp DESC)) = 1
),
full_ens_table AS (
SELECT
    A.owner,
    A.tokenid,
    A.block_timestamp,
    A.cost,
    A.expiration_time,
    A.expiration_date,
    A.ens_name,
    A.label,
    A.tx_hash,
    A._inserted_timestamp,
    b.*
FROM
{% if is_incremental() %}
(select     
    owner,
    tokenid,
    block_timestamp,
    cost,
    expiration_time,
    expiration_date,
    ens_name,
    label,
    tx_hash,
    _inserted_timestamp,
    node
    from base_table
    union
    select 
    owner,
    tokenid,
    block_timestamp,
    cost,
    expiration_time,
    expiration_date,
    ens_name,
    label,
    tx_hash,
    _inserted_timestamp,
    node
    from {{this}}
    where node in (select distinct node from info_table_small where node not in (select distinct node from base_table)) -- distinct nodes where node not in base table but in the update info
    or ens_name in (select distinct cleaned_ens from set_name where cleaned_ens not in (select distinct ens_name from base_table))
    or owner in (select distinct wallets as owner from set_name) and ens_set = 'Y'
    ) A
{% else %}
base_table A
{% endif %}
    LEFT JOIN info_table_small b
    ON A.node = b.node
),
{% if is_incremental() %}
incremental_full_ens_table as (
select * from full_ens_table
union select     
    owner,
    tokenid,
    block_timestamp,
    cost,
    expiration_time,
    expiration_date,
    ens_name,
    label,
    tx_hash,
    _inserted_timestamp,
    last_update_info,
    info_updater,
    node,
    twitter,
    avatar,
    discord,
    github,
    email,
    url,
    description,
    notice,
    keywords,
    reddit,
    telegram,
    opensea,
    rarible,
    superrare
    from {{this}}
    where tokenid in (select distinct tokenid from full_ens_table)
),
{% endif %}
ens_table_small AS (
SELECT
    block_timestamp,
    tx_hash,
    owner,
    tokenid,
    MAX(ens_name) over (
        PARTITION BY tokenid
        ORDER BY
            block_timestamp ASC
    ) AS ens_name,
    MAX(
        cost :: STRING
    ) over (
        PARTITION BY tokenid
        ORDER BY
            block_timestamp ASC
    ) AS cost,
    MAX(expiration_time) over (
        PARTITION BY tokenid
        ORDER BY
            block_timestamp ASC
    ) AS expiration_time,
    MAX(expiration_date) over (
        PARTITION BY tokenid
        ORDER BY
            block_timestamp ASC
    ) AS expiration_date,
    MAX(label) over (
        PARTITION BY tokenid
        ORDER BY
            block_timestamp ASC
    ) AS label,
    MAX(node) over (
        PARTITION BY tokenid
        ORDER BY
            block_timestamp ASC
    ) AS node,
    MAX(last_update_info) over (
        PARTITION BY tokenid
        ORDER BY
            block_timestamp ASC
    ) AS last_update_info,
    MAX(info_updater) over (
        PARTITION BY tokenid
        ORDER BY
            block_timestamp ASC
    ) AS info_updater,
    MAX(twitter) over (
        PARTITION BY tokenid
        ORDER BY
            block_timestamp ASC
    ) AS twitter,
    MAX(avatar) over (
        PARTITION BY tokenid
        ORDER BY
            block_timestamp ASC
    ) AS avatar,
    MAX(discord) over (
        PARTITION BY tokenid
        ORDER BY
            block_timestamp ASC
    ) AS discord,
    MAX(github) over (
        PARTITION BY tokenid
        ORDER BY
            block_timestamp ASC
    ) AS github,
    MAX(email) over (
        PARTITION BY tokenid
        ORDER BY
            block_timestamp ASC
    ) AS email,
    MAX(url) over (
        PARTITION BY tokenid
        ORDER BY
            block_timestamp ASC
    ) AS url,
    MAX(description) over (
        PARTITION BY tokenid
        ORDER BY
            block_timestamp ASC
    ) AS description,
    MAX(notice) over (
        PARTITION BY tokenid
        ORDER BY
            block_timestamp ASC
    ) AS notice,
    MAX(keywords) over (
        PARTITION BY tokenid
        ORDER BY
            block_timestamp ASC
    ) AS keywords,
    MAX(reddit) over (
        PARTITION BY tokenid
        ORDER BY
            block_timestamp ASC
    ) AS reddit,
    MAX(telegram) over (
        PARTITION BY tokenid
        ORDER BY
            block_timestamp ASC
    ) AS telegram,
    MAX(opensea) over (
        PARTITION BY tokenid
        ORDER BY
            block_timestamp ASC
    ) AS opensea,
    MAX(rarible) over (
        PARTITION BY tokenid
        ORDER BY
            block_timestamp ASC
    ) AS rarible,
    MAX(superrare) over (
        PARTITION BY tokenid
        ORDER BY
            block_timestamp ASC
    ) AS superrare,
    _inserted_timestamp
FROM

{% if is_incremental() %}
incremental_full_ens_table
{% else %}
full_ens_table
{% endif %}
    
    qualify(ROW_NUMBER() over(PARTITION BY tokenid
ORDER BY
    block_timestamp DESC)) = 1
),
ens_table_setname AS (
SELECT
    DISTINCT A.block_timestamp,
    A.tx_hash,
    A.owner,
    A.tokenid,
    A.ens_name,
    CASE
        WHEN b.set_ens_name IS NULL THEN 'N'
        ELSE 'Y'
    END AS ens_set,
    A.cost,
    A.expiration_time,
    A.expiration_date,
    A.label,
    A.node,
    A.last_update_info,
    A.info_updater,
    A.twitter,
    A.avatar,
    A.discord,
    A.github,
    A.email,
    A.url,
    A.description,
    A.notice,
    A.keywords,
    A.reddit,
    A.telegram,
    A.opensea,
    A.rarible,
    A.superrare,
    A._inserted_timestamp
FROM
    ens_table_small A
    LEFT JOIN set_name b
    ON A.owner = b.wallets
    AND A.ens_name = b.cleaned_ens
)
SELECT
*
FROM
ens_table_setname

