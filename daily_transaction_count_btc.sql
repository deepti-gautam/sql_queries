WITH trxns_hash_global as(
    SELECT 
    DATE(block_timestamp) as date,
    trxns.hash as global_hashes,
    input_value as trxn_value
FROM 
  `bigquery-public-data.crypto_bitcoin.transactions` as trxns
),
trxns_hash_local as(
SELECT 
    DATE(block_timestamp) as date,
    trxns.hash as hashes,
    input_value
FROM 
  `bigquery-public-data.crypto_bitcoin.transactions` as trxns
    ,UNNEST(trxns.outputs) as outputs
INNER JOIN 
(SELECT addresses
FROM `investigation-team-scratch.abby.champion_summary_btc` as c
INNER JOIN `investigation-team-scratch.abby.btc_clustermap` as d
ON c.cluster_id=d.cluster_id
) as a
ON ARRAY_TO_STRING(outputs.addresses,' ,') =a.addresses
UNION DISTINCT
SELECT 
    DATE(block_timestamp) as date, 
    trxns.hash as hashes,
    input_value
FROM 
  `bigquery-public-data.crypto_bitcoin.transactions` as trxns
    ,UNNEST(trxns.inputs) as inputs
INNER JOIN 
(SELECT addresses
FROM `investigation-team-scratch.abby.champion_summary_btc` as c
INNER JOIN `investigation-team-scratch.abby.btc_clustermap` as d
ON c.cluster_id=d.cluster_id
) as b
ON ARRAY_TO_STRING(inputs.addresses,' ,') = b.addresses
),
local_global_trxn_count as
(
SELECT g.date as date, 
        count(DISTINCT global_hashes) as global_trxn_count,
       sum(trxn_value/1e8) as global_trxn_value,
       count(DISTINCT hashes) as local_trxn_count,
       sum(input_value/1e8) as local_trxn_value
FROM trxns_hash_local as l
INNER JOIN trxns_hash_global as g
ON l.date=g.date
GROUP BY 1
)
 SELECT
    b.date as date, 
    global_trxn_count,
    global_trxn_value,
    local_trxn_count,
    local_trxn_value
 FROM local_global_trxn_count as b
 WHERE b.date BETWEEN '2020-03-01'
 and '2020-04-15'
 ORDER BY 1