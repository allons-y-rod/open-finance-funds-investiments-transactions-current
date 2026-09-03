# Pipeline imperativo (Auto Loader + Structured Streaming) - bronze_transactions_current

Pipeline em Auto Loader (`cloudFiles`) + Structured Streaming puro (sem decorators de framework
declarativo): leitura, transformação e escrita via `DataFrame`/`writeStream`/`foreachBatch`
explícitos no código.

Este README descreve o **estado atual** do pipeline (o que ele faz hoje).

## Estrutura

```
imperativo/
└── autoloader/
    ├── common/
    │   ├── config.py               # paths, tabelas, opções do Auto Loader e reader
    │   ├── rules_module.py          # get_rules(tags) → dict[nome, constraint] (regras de validade)
    │   └── spark.py                 # obtenção/criação da SparkSession
    ├── bronze/
    │   ├── bronze_transactions_current.py   # pipeline (leitura, escrita, start)
    │   └── tables_bronze_config.py          # schema UC, payload StructType e DDL da tabela bronze
    └── silver/
        ├── silver_transactions_current.py   # pipeline (cast, flag quarentena, split, dedup, upsert, start)
        └── tables_silver_config.py          # DDL das tabelas, business key, dedup order
```

## Camadas

### Bronze (`bronze/bronze_transactions_current.py`)

Lê o Volume de landing via Auto Loader (`cloudfiles_reader` + `.schema(SCHEMA)`), acrescenta
`source_file`/`ingestion_ts`/`ingestion_date`, explode o array `data` de cada arquivo e projeta as
colunas de negócio — todas `STRING`, exceto `transaction_conversion_date` (`DATE`, via
`.cast("date")` na leitura). Deriva `transaction_conversion_month` (`STRING`, formato `"yyyy-MM"`)
a partir do mesmo campo bruto já castado para `DATE`, garantindo que as duas colunas falhem juntas
(`NULL`) se a data de origem vier malformada. Nenhuma validação/rejeição acontece nesta camada —
tudo que chega é gravado via sink nativo (`writeStream...toTable`, `outputMode("append")`,
`trigger(availableNow=True)`), sem `foreachBatch`.

### Silver (`silver/silver_transactions_current.py`)

Segue a **mesma lógica de dados** da silver declarativa (`declarativa/lakeflow/silver`), na forma
imperativa. Lê a tabela bronze como streaming source
(`spark.readStream.option("skipChangeCommits", "true").table(BRONZE_TABLE)` — Delta como fonte
incremental, não Auto Loader; `skipChangeCommits` para o stream não quebrar quando a bronze é
reescrita fora de `append`). Por micro-batch (`foreachBatch`):

1. **Cast** (`_cast_columns`): campos monetários/quantidade para `DECIMAL(20, 2)`
   (`transaction_conversion_date`/`transaction_conversion_month` já chegam tipados da bronze).
2. **Flag de quarentena** (`_flag_quarantine`): calcula `is_quarantined` (`NOT(<regras de
   `get_rules("validity")` ANDadas>)` — `transaction_id`/`client_id` não nulos e não vazios) e
   `failure_reason` (nomes das regras violadas por linha). Equivale à tabela intermediária
   `_temporary` + `@dp.expect_all` do declarativo, sem materializar nada.
3. **Split** (`_split_batch`): `valid_df` = `is_quarantined = false` (dropa a flag e
   `failure_reason`); `invalid_df` = `is_quarantined = true` (mantém `failure_reason`).
4. **Escrita**:
   - `invalid_df` é gravado em `append` na tabela de rejeitados **com schema largo** (colunas de
     negócio casted + `failure_reason`) — sem serialização JSON, sem dedup (recebe todas as linhas
     inválidas do batch).
   - `valid_df` é **deduplicado** (`_deduplicate_transactions` — `Window` pela business key,
     ordenada por `DEDUP_ORDER` = `transaction_conversion_date`/`ingestion_ts`/`source_file` desc) e
     gravado via `MERGE` (`_upsert_valid`, `DeltaTable`) — upsert por business key, com poda por
     `client_id` (coluna imutável; `CLUSTER BY (transaction_conversion_month, client_id)`) e
     `whenMatchedUpdateAll` condicionado a `struct(*DEDUP_ORDER)` mais recente. A dedup acontece só
     do lado válido (como o `create_auto_cdc_flow` declarativo); garante zero duplicatas na tabela
     final entre micro-batches e reinícios do stream.
