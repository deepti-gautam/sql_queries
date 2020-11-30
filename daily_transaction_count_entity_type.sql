
WITH trxns_hash_local AS
	(
		SELECT 
			  DATE(block_timestamp) as date
		,	  trxns.hash as hashes
		,	  input_value

		FROM 
  `			'bigquery-public-data.crypto_bitcoin.transactions` as trxns
    
		INNER JOIN (
			SELECT 
				  tag_name_verbose
			,	  tag_type_verbose
			,	  addresses
			FROM 
				`investigation-team-scratch.abby.champion_summary_btc` as c
			INNER JOIN 
				`investigation-team-scratch.abby.btc_clustermap` as d
			ON 
				c.cluster_id=d.cluster_id
			WHERE 
				tag_type_verbose = '{{ entity_type }}'
	) 
    AS a
    ON 
    	ARRAY_TO_STRING(outputs.addresses,' ,') =a.addresses

	UNION DISTINCT

	SELECT 
    	  DATE(block_timestamp) as date 
    	  trxns.hash as hashes
          input_value
	FROM 
  		`bigquery-public-data.crypto_bitcoin.transactions` as trxns

	INNER JOIN 
		(	
			SELECT 
				  tag_name_verbose
			,     tag_type_verbose, addresses
			FROM 
			    `investigation-team-scratch.abby.champion_summary_btc` as c
			INNER JOIN  
			    `investigation-team-scratch.abby.btc_clustermap` as d
			ON  
			   c.cluster_id=d.cluster_id
			WHERE  
			      tag_type_verbose = '{{ entity_type }}'
		) 
		AS b
		ON ARRAY_TO_STRING(inputs.addresses,' ,') = b.addresses
		),

		local_trxn_count as
		(
		SELECT date, 
		       count(DISTINCT hashes) as local_trxn_count,
		       sum(input_value/1e8) as local_trxn_value
		FROM trxns_hash_local
		GROUP BY 1
		)

		 SELECT
		    b.date as date, 
		    local_trxn_count,
		    local_trxn_value
		 FROM local_trxn_count as b
		 WHERE b.date BETWEEN '{{ mydate.start }}'
		 and '{{ mydate.end }}'
		 ORDER BY 1