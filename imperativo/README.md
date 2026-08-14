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
    │   └── spark.py                 # obtenção/criação da SparkSession
    ├── bronze/
    │   ├── bronze_transactions_current.py   # pipeline (leitura, escrita, start)
    │   └── tables_bronze_config.py          # schema UC, payload StructType e DDL da tabela bronze
    └── silver/
        ├── silver_transactions_current.py   # pipeline (cast, dedup, split, upsert, start)
        └── tables_silver_config.py          # schemas das tabelas, EXPECTATIONS, business key, dedup order
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

Lê a tabela bronze como streaming source (`spark.readStream.table(BRONZE_TABLE)` — Delta como
fonte incremental, não Auto Loader). Por micro-batch (`foreachBatch`):

1. **Dedup** (`_deduplicate_transactions`): `Window` particionada pela business key
   (`client_id`, `transaction_id`), ordenada por `ingestion_ts`/`source_file` desc, mantendo só a
   linha mais recente por chave dentro do micro-batch.
2. **Cast** (`_cast_columns`): campos monetários/quantidade para `DECIMAL(20, 2)`
   (`transaction_conversion_date`/`transaction_conversion_month` já chegam tipados da bronze).
3. **Split** (`_split_batch`): aplica `EXPECTATIONS` (`valid_business_key` — `transaction_id`/
   `client_id` não nulos e não vazios), separando `valid_df`/`rejected_df`.
4. **Escrita**: `rejected_df` é serializado em JSON e gravado em `append` na tabela de rejeitados;
   `valid_df` é gravado via `MERGE` (`_upsert_valid`, `DeltaTable`) — upsert por business key, com
   poda por `transaction_conversion_month` e `whenMatchedUpdateAll` condicionado ao registro mais
   recente — garantindo zero duplicatas na tabela final entre micro-batches e reinícios do stream.
