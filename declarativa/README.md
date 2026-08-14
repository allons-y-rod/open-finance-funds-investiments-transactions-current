# Pipeline declarativo (Lakeflow Declarative Pipelines) - bronze_transactions_current

Pipeline em Lakeflow Declarative Pipelines (`pyspark.pipelines`, importado como `dp`), usando
Auto Loader (`cloudFiles`) + `@dp.table`/`@dp.view`/`@dp.expect`.

Este README descreve o **estado atual** do pipeline (o que ele faz hoje).

## Estrutura

```
declarativa/
├── README.md                          # este arquivo
└── lakeflow/
    ├── common/
    │   └── config.py                  # INPUT_PATH, CLOUDFILES_OPTIONS, cloudfiles_reader
    ├── bronze/
    │   ├── bronze_transactions_current.py   # @dp.table: leitura, explode, select/cast
    │   └── table_bronze_tc_config.py        # transactions_current_schema (payload StructType)
    └── silver/
        ├── silver_transactions_current.py   # @dp.view/@dp.table + create_auto_cdc_flow (upsert)
        └── table_silver_tc_config.py        # schemas das tabelas, EXPECTATIONS, business key, dedup order
```

## Camadas

### Bronze (`bronze_transactions_current`, `@dp.table`)

Lê o Volume de landing via Auto Loader (`cloudfiles_reader` + `.schema(SCHEMA)`), explode o array
`data` de cada arquivo e projeta as colunas de negócio — todas `STRING`, exceto
`transaction_conversion_date` (`DATE`, via `.cast("date")` na leitura). Deriva
`transaction_conversion_month` (`STRING`, formato `"yyyy-MM"`) a partir do mesmo campo bruto
já castado para `DATE`, garantindo que as duas colunas falhem juntas (`NULL`) se a data de origem
vier malformada. Nenhuma validação/rejeição acontece nesta camada — tudo que chega é gravado.

### Silver

- **`silver_transactions_current_casted`** (`@dp.view`): lê a bronze via `dp.read_stream`, tipa
  os campos monetários/quantidade para `DECIMAL(20, 2)` (`transaction_conversion_date` já chega
  como `DATE` da bronze, sem recast).
- **`silver_transactions_current_valid`** (`@dp.view` + `@dp.expect_all_or_drop(EXPECTATIONS)`):
  aplica `valid_business_key` (`transaction_id`/`client_id` não nulos e não vazios), descartando
  linhas inválidas do fluxo que alimenta a tabela final.
- **Tabela final** (`dp.create_streaming_table` + `dp.create_auto_cdc_flow`): upsert incremental
  por `TRANSACTION_BUSINESS_KEY` (`client_id`, `transaction_id`), com `sequence_by` pela dupla
  `DEDUP_ORDER` (`ingestion_ts`, `source_file`) — o framework decide o `MERGE`, sem código manual.
- **`silver_transactions_current_rechaco`** (`@dp.table`): reaproveita a mesma view `_casted` para
  isolar as linhas que falham `EXPECTATIONS`, serializando o payload em JSON (`data`) mais
  `failure_reason`/`rejected_at`/`rejected_at_month`, particionada por `rejected_at_month`.
