# cbi-queries

Describe all queries being used on the Cardano Blockchain Insights (https://lookerstudio.google.com/u/0/reporting/3136c55b-635e-4f46-8e4b-b8ab54f2d460/page/k5r9B).

All queries are being used againt the BigQuery project (`BQ_PROJECT_ID`): `blockchain-analytics-392322`

## Retrieve pool total stake distribution per epoch

```sql
    SELECT
        pool_hash AS pool_id,
        epoch_no,
        SUM(amount) / 1000000 AS stake,
        COUNT(stake_addr_hash) AS num_addr
    FROM
        `{BQ_PROJECT_ID}.cardano_mainnet.epoch_stake`
    WHERE
        epoch_no = @epoch
        AND amount > 0
    GROUP BY
        pool_hash,
        epoch_no
```

## Retrieve wallet wealth percentile distribution per epoch

```sql
    SELECT
        buckets.epoch_no AS epoch,
        MIN(buckets.amount) AS min_treshold,
        SUM(buckets.amount) AS total,
        percentile
    FROM (
        SELECT
            epoch_no,
            amount,
            NTILE(100) OVER (PARTITION BY epoch_no ORDER BY amount DESC) AS percentile
        FROM
            `{BQ_PROJECT_ID}.cardano_mainnet.epoch_stake`
        WHERE
            amount >= 1
            AND epoch_no=@epoch) AS buckets
    GROUP BY
        1,4
    ORDER BY
        1,4
```

## Retrieve wallet wealth bracket distribution per epoch 

```sql
    SELECT epoch_stake.epoch_no,
       SUM(CASE WHEN (epoch_stake.amount / 1000000) BETWEEN 0.000001 AND 1 THEN 1 ELSE 0 END) AS `0-1`,
       SUM(CASE WHEN (epoch_stake.amount / 1000000) BETWEEN 1.000001 AND 10 THEN 1 ELSE 0 END) AS `1-10`,
       SUM(CASE WHEN (epoch_stake.amount / 1000000) BETWEEN 10.000001 AND 25 THEN 1 ELSE 0 END) AS `10-25`,
       SUM(CASE WHEN (epoch_stake.amount / 1000000) BETWEEN 25.000001 AND 50 THEN 1 ELSE 0 END) AS `25-50`,
       SUM(CASE WHEN (epoch_stake.amount / 1000000) BETWEEN 50.000001 AND 100 THEN 1 ELSE 0 END) AS `50-100`,
       SUM(CASE WHEN (epoch_stake.amount / 1000000) BETWEEN 100.000001 AND 250 THEN 1 ELSE 0 END) AS `100-250`,
       SUM(CASE WHEN (epoch_stake.amount / 1000000) BETWEEN 250.000001 AND 500 THEN 1 ELSE 0 END) AS `250-500`,
       SUM(CASE WHEN (epoch_stake.amount / 1000000) BETWEEN 500.000001 AND 750 THEN 1 ELSE 0 END) AS `500-750`,
       SUM(CASE WHEN (epoch_stake.amount / 1000000) BETWEEN 750.000001 AND 1000 THEN 1 ELSE 0 END) AS `750-1000`,
       SUM(CASE WHEN (epoch_stake.amount / 1000000) BETWEEN 1000.000001 AND 1500 THEN 1 ELSE 0 END) AS `1000-1500`,
       SUM(CASE WHEN (epoch_stake.amount / 1000000) BETWEEN 1500.000001 AND 2000 THEN 1 ELSE 0 END) AS `1500-2000`,
       SUM(CASE WHEN (epoch_stake.amount / 1000000) BETWEEN 2000.000001 AND 5000 THEN 1 ELSE 0 END) AS `2000-5000`,
       SUM(CASE WHEN (epoch_stake.amount / 1000000) BETWEEN 5000.000001 AND 10000 THEN 1 ELSE 0 END) AS `5000-10000`,
       SUM(CASE WHEN (epoch_stake.amount / 1000000) BETWEEN 10000.000001 AND 25000 THEN 1 ELSE 0 END) AS `10000-25000`,
       SUM(CASE WHEN (epoch_stake.amount / 1000000) BETWEEN 25000.000001 AND 50000 THEN 1 ELSE 0 END) AS `25000-50000`,
       SUM(CASE WHEN (epoch_stake.amount / 1000000) BETWEEN 50000.000001 AND 75000 THEN 1 ELSE 0 END) AS `50000-75000`,
       SUM(CASE WHEN (epoch_stake.amount / 1000000) BETWEEN 75000.000001 AND 100000 THEN 1 ELSE 0 END) AS `75000-100000`,
       SUM(CASE WHEN (epoch_stake.amount / 1000000) BETWEEN 100000.000001 AND 150000 THEN 1 ELSE 0 END) AS `100001-150000`,
       SUM(CASE WHEN (epoch_stake.amount / 1000000) BETWEEN 150000.000001 AND 200000 THEN 1 ELSE 0 END) AS `150000-200000`,
       SUM(CASE WHEN (epoch_stake.amount / 1000000) BETWEEN 200000.000001 AND 400000 THEN 1 ELSE 0 END) AS `200000-400000`,
       SUM(CASE WHEN (epoch_stake.amount / 1000000) BETWEEN 400000.000001 AND 500000 THEN 1 ELSE 0 END) AS `400000-500000`,
       SUM(CASE WHEN (epoch_stake.amount / 1000000) BETWEEN 500000.000001 AND 750000 THEN 1 ELSE 0 END) AS `500000-750000`,
       SUM(CASE WHEN (epoch_stake.amount / 1000000) BETWEEN 750000.000001 AND 1000000 THEN 1 ELSE 0 END) AS `750000-1000000`,
       SUM(CASE WHEN (epoch_stake.amount / 1000000) BETWEEN 1000000.000001 AND 5000000 THEN 1 ELSE 0 END) AS `1000000-5000000`,
       SUM(CASE WHEN (epoch_stake.amount / 1000000) BETWEEN 5000000.000001 AND 10000000 THEN 1 ELSE 0 END) AS `5000000-10000000`,
       SUM(CASE WHEN (epoch_stake.amount / 1000000) BETWEEN 10000000.000001 AND 25000000 THEN 1 ELSE 0 END) AS `10000000-25000000`,
       SUM(CASE WHEN (epoch_stake.amount / 1000000) BETWEEN 25000000.000001 AND 50000000 THEN 1 ELSE 0 END) AS `25000000-50000000`,
       SUM(CASE WHEN (epoch_stake.amount / 1000000) BETWEEN 50000000.000001 AND 100000000 THEN 1 ELSE 0 END) AS `50000000-100000000`,
       SUM(CASE WHEN (epoch_stake.amount / 1000000) BETWEEN 100000000.000001 AND 250000000 THEN 1 ELSE 0 END) AS `100000000-250000000`,
       SUM(CASE WHEN (epoch_stake.amount / 1000000) > 250000000 THEN 1 ELSE 0 END) AS `250000000-`
    FROM `{BQ_PROJECT_ID}.cardano_mainnet.epoch_stake` AS epoch_stake
    WHERE epoch_no=@epoch
    GROUP BY epoch_stake.epoch_no
```

## Retrieve Cardano Blockchain general statistics

```sql
    WITH block_info AS (
       SELECT count(epoch_no) as blocks_with_tx, sum(tx_count) as trans
       FROM `{BQ_PROJECT_ID}.cardano_mainnet.block`
    ),
    block_hash_info AS (
       SELECT count(block_hash) as blocks
       FROM  `{BQ_PROJECT_ID}.cardano_mainnet.block_hash`
    ),
    account_info AS (
       SELECT count(stake_address) as accounts
       FROM  `{BQ_PROJECT_ID}.cardano_mainnet.rel_stake_hash`
    ),
    delegation_info AS (
       SELECT count(distinct stake_addr_hash) as all_delegators
       FROM  `{BQ_PROJECT_ID}.cardano_mainnet.delegation`
    ),
    delegation_deregistration_info as (
       SELECT count(distinct stake_addr_hash) as delegators_deregistred
       FROM  `{BQ_PROJECT_ID}.cardano_mainnet.stake_deregistration`
    )
    SELECT
       blocks_with_tx,
       trans,
       blocks,
       accounts,
       (all_delegators - delegators_deregistred) as `delegators`
    FROM block_info, block_hash_info, account_info, delegation_info, delegation_deregistration_info;
```

##  Retrieve minted block statistics per epoch

```sql 
    SELECT
        block.epoch_no,
        COUNT(block.slot_no) AS block_count,
        AVG(block.tx_count) AS avg_transactions_per_block,
        AVG(block.block_size) AS avg_block_size_bytes
    FROM
        `{BQ_PROJECT_ID}.cardano_mainnet.block` AS block
    WHERE
        epoch_no=@epoch
    GROUP BY
        epoch_no
    ORDER BY
        epoch_no DESC;
```

##  Retrieve number of retiring pools per epoch

```sql
    SELECT
        retiring_epoch,
        COUNT(*) AS retiring_pools
    FROM
        `{BQ_PROJECT_ID}.cardano_mainnet.pool_retire` as pool_retire
    WHERE
        retiring_epoch=@epoch
    GROUP BY
        pool_retire.retiring_epoch
    ORDER BY
        retiring_epoch DESC;
```

##  Retrieve Cardno Blockchain parameters information per epoch

```sql
    SELECT
        epoch_no,
        max_block_size,
        max_tx_size
    FROM
        `{BQ_PROJECT_ID}.cardano_mainnet.epoch_param` as epoch_param
    WHERE
        epoch_no=@epoch;
```

##  Retrieve script count per type

```sql
    SELECT
        type,
        COUNT(type)
    FROM
        `{BQ_PROJECT_ID}.cardano_mainnet.script` AS scripts
    GROUP BY
        type
```

##  Retrieve total value locked on scripts

```sql
    WITH
      addr_outputs AS (
      SELECT
        epoch_no,
        address,
        CAST(JSON_VALUE(o, '$.idx') AS INT64) AS idx,
        CAST(JSON_VALUE(o, '$.slot_no') AS INT64) AS slot_no,
        CAST(JSON_VALUE(o, '$.txidx') AS INT64) AS txidx
      FROM
        `{BQ_PROJECT_ID}.cardano_mainnet.rel_addr_txout`
      LEFT JOIN
        UNNEST(JSON_EXTRACT_ARRAY(outputs, "$") ) AS o
      WHERE
        address_has_script = TRUE),
      INCOMING_UTXOS AS (
      SELECT
        tio.epoch_no,
        addr_outputs.address,
        tio.slot_no,
        tio.txidx,
        addr_outputs.idx,
        CAST(JSON_VALUE(a, '$.out_value') AS INT64 ) AS value,
      FROM
        `{BQ_PROJECT_ID}.cardano_mainnet.tx_in_out` AS tio
      JOIN
        addr_outputs
      ON
        tio.epoch_no = addr_outputs.epoch_no
        AND tio.slot_no = addr_outputs.slot_no
        AND tio.txidx = addr_outputs.txidx
      JOIN
        UNNEST(JSON_QUERY_ARRAY(outputs)) AS a
      ON
        CAST(JSON_VALUE(a, '$.out_idx') AS INT64 ) = addr_outputs.idx ),
      OUTGOING_UTXOS AS (
      SELECT
        value AS value,
      FROM
        INCOMING_UTXOS AS io
      LEFT JOIN
        `{BQ_PROJECT_ID}.cardano_mainnet.tx_consumed_output` AS cons
      ON
        io.slot_no = cons.slot_no
        AND io.txidx = cons.txidx
        AND io.idx = cons.index
      WHERE
        cons.slot_no IS NULL
        AND cons.txidx IS NULL
        AND cons.index IS NULL )
    SELECT
      SUM(value) / 1000000 AS sum_outgoing
    FROM
      OUTGOING_UTXOS     
```

##  Retrieve transactions information per epoch 

```sql
    WITH
      unnested_redeemer AS (
      SELECT
        redeemer.slot_no,
        redeemer.txidx,
        json_extract_scalar(r, '$.script_hash') as script_hash
        FROM 
          `{BQ_PROJECT_ID}.cardano_mainnet.redeemer` as redeemer,
        UNNEST(JSON_EXTRACT_ARRAY(redeemers)) AS r
        WHERE redeemer.epoch_no = @epoch
      ),

      tx_info AS (
      SELECT
        tx.epoch_no,
        COUNT(DISTINCT CONCAT(tx.slot_no, tx.txidx)) AS trx_total_count,
        AVG(tx.size) AS avg_txs_size_bytes,
        SUM(tx.size) / 432000 AS tx_bytes_per_second,
        SUM(tx.fee) / SUM(tx.size) AS tx_fees_per_byte,
        FROM
          `{BQ_PROJECT_ID}.cardano_mainnet.tx` as tx
        GROUP BY 
          tx.epoch_no
        HAVING
          tx.epoch_no = @epoch
      ),
      
      q1 AS (
      SELECT
        tx.epoch_no,
        COUNT(DISTINCT tx_metadata.tx_hash) AS trx_with_metadata,
        COALESCE(COUNT(DISTINCT
            CASE
              WHEN tx_metadata.tx_hash IS NULL AND (script.type LIKE 'plutusV%') THEN tx.tx_hash
          END
            ), 0) AS trx_with_plutus_only,
        COUNT(DISTINCT (CASE
              WHEN (script.type LIKE 'plutusV%') THEN tx_metadata.tx_hash
            ELSE
            NULL
          END
            )) AS trx_with_plutus_with_metadata,
        COALESCE(AVG(CASE
              WHEN tx_metadata.tx_hash IS NOT NULL AND script.type IS NULL THEN tx.size
          END
            ), 0) AS avg_txs_with_metadata_no_script_size_bytes, -- bad!
        -- has metadata but no script (with metadata only)
        COALESCE(AVG(CASE
              WHEN tx_metadata.tx_hash IS NULL AND script.type IS NULL THEN tx.size
          END
            ), 0) AS avg_txs_without_metadata_no_script_size_bytes,
        -- no metadata and no scripts (simple transactions)
        COALESCE(AVG(CASE
              WHEN tx_metadata.tx_hash IS NOT NULL AND script.script_hash IS NOT NULL AND (script.type LIKE 'plutusV%') THEN tx.size
          END
            ), 0) AS avg_txs_on_plutus_contracts_with_metadata_size_bytes, -- bad!
        COALESCE(AVG(CASE
              WHEN tx_metadata.tx_hash IS NULL AND script.script_hash IS NOT NULL AND (script.type LIKE 'plutusV%') THEN tx.size
          END
            ), 0) AS avg_txs_on_plutus_contracts_without_metadata_size_bytes
      FROM
        `{BQ_PROJECT_ID}.cardano_mainnet.tx` as tx
      LEFT JOIN
        `{BQ_PROJECT_ID}.cardano_mainnet.tx_metadata` as tx_metadata
      ON
        tx.tx_hash = tx_metadata.tx_hash and tx.epoch_no = tx_metadata.epoch_no
      LEFT JOIN
        unnested_redeemer
      ON
        tx.slot_no = unnested_redeemer.slot_no and tx.txidx = unnested_redeemer.txidx
      LEFT JOIN
        `{BQ_PROJECT_ID}.cardano_mainnet.script` as script
      ON
        unnested_redeemer.script_hash = script.script_hash
      WHERE
        tx.epoch_no = @epoch
      GROUP BY
        epoch_no
      ORDER BY
        epoch_no DESC)

    SELECT
      tx_info.epoch_no,
      tx_info.trx_total_count,
      tx_info.trx_total_count - t1.trx_with_plutus_only - t1.trx_with_metadata AS trx_simple_count,
      t1.trx_with_plutus_only,
      t1.trx_with_plutus_with_metadata,
      t1.trx_with_metadata - t1.trx_with_plutus_with_metadata AS trx_with_metadata_only,
      tx_info.avg_txs_size_bytes,
      tx_info.tx_bytes_per_second,
      tx_info.tx_fees_per_byte,
      t1.avg_txs_with_metadata_no_script_size_bytes,
      -- average size of metadata only transactions
      t1.avg_txs_without_metadata_no_script_size_bytes,
      -- average size of simple transactions
      t1.avg_txs_on_plutus_contracts_with_metadata_size_bytes,
      -- average size of plutus transactions with metadata
      t1.avg_txs_on_plutus_contracts_without_metadata_size_bytes -- average size of plutus transactions without metadata
    FROM
      q1 t1
    INNER JOIN
      tx_info
    ON
      t1.epoch_no = tx_info.epoch_no;
```

##  Retrieve transactions metadata information per epoch

```sql
    WITH
        tx_metadata_unnested AS (
        SELECT
            tx_metadata.epoch_no,
            tx_metadata.tx_hash,
            JSON_EXTRACT_SCALAR(m, '$.index') AS key
        FROM
            `{BQ_PROJECT_ID}.cardano_mainnet.tx_metadata` AS tx_metadata,
            UNNEST(JSON_EXTRACT_ARRAY(metadata)) AS m
        WHERE
            tx_metadata.epoch_no = @epoch)

    SELECT
        tx_metadata_unnested.epoch_no,
        tx_metadata_unnested.key,
        COUNT(DISTINCT tx_metadata_unnested.tx_hash) AS tx_count,
        TRUNC(AVG(tx.size),2) AS avg_txs_size_bytes
    FROM
        tx_metadata_unnested
    INNER JOIN
        `{BQ_PROJECT_ID}.cardano_mainnet.tx` AS tx
    ON
        tx.tx_hash = tx_metadata_unnested.tx_hash
    INNER JOIN
        `{BQ_PROJECT_ID}.cardano_mainnet.block` AS block
    ON
        block.slot_no = tx.slot_no
    WHERE
        tx.epoch_no = @epoch
    GROUP BY
        tx_metadata_unnested.epoch_no,
        tx_metadata_unnested.key
    ORDER BY
        epoch_no DESC, tx_count DESC;
```

##  Retrieve top N scripts per usage

```sql
    SELECT
        JSON_EXTRACT_SCALAR(r, '$.script_hash') AS script_hash,
        COUNT(1) AS total_calls
    FROM
        `{BQ_PROJECT_ID}.cardano_mainnet.redeemer` AS redeemer
    LEFT JOIN
        UNNEST(JSON_EXTRACT_ARRAY(redeemer.redeemers)) AS r
    GROUP BY
        script_hash
    ORDER BY
        total_calls DESC
    LIMIT
        @top;
```

##  Retrieve treasury information

```sql
    SELECT
        treasury
    FROM
        `{BQ_PROJECT_ID}.cardano_mainnet.ada_pots`
    WHERE
        epoch_no=@epoch
```

##  Retrieve minted tokens and policies

```sql
    SELECT
        COUNT(DISTINCT policyId) AS total_policies,
        COUNT(DISTINCT fingerprint) AS total_tokens
    FROM
       `{BQ_PROJECT_ID}.cardano_mainnet.ma_minting`;
```

##  Retrieve NFTs count

```sql
    WITH
        single_assets AS (
        SELECT
            fingerprint,
            policyId
        FROM
            `{BQ_PROJECT_ID}.cardano_mainnet.ma_minting`
        WHERE
            ARRAY_LENGTH(JSON_EXTRACT_ARRAY(minting,'$')) = 1
            AND JSON_EXTRACT_SCALAR(minting, '$[0].quantity') = '1'),
        non_duplicated_assets AS (
            SELECT
                fingerprint,
                policyId,
                SUM(1) AS total
            FROM
                single_assets
            GROUP BY
                fingerprint,
                policyId
            HAVING
                total = 1)
    SELECT
        COUNT(*) as total_nft,
    FROM
        non_duplicated_assets;
```

##  Retrieve stake distribution per pool, per epoch

```sql
    SELECT
        pool_hash,
        epoch_no,
        SUM(amount) / 1000000 AS stake,
        COUNT(stake_addr_hash) AS num_addr
    FROM
        `{BQ_PROJECT_ID}.cardano_mainnet.epoch_stake`
    WHERE
        epoch_no = @epoch
        AND amount > 0
    GROUP BY
        pool_hash,
        epoch_no;
```
