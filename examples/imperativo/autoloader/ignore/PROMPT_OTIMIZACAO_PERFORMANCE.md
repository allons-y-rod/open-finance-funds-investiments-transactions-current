# Guias de Implementação de Otimizações de Performance

> **Módulo Target:** `imperativo/autoloader`  
> **Objetivo:** Guia passo a passo de refatoração para execução das otimizações de performance identificadas no pipeline PySpark / Auto Loader / Delta Lake.

---

## 🎯 Lista de Tarefas para Execução

---

### 🟢 Tarefa 1: Inverter ordem de Deduplicação e Cast de Colunas na Silver
- **Arquivo Target:** `imperativo/autoloader/silver/silver_transactions_current.py`
- **Função:** `_write_batch(batch_df: DataFrame, batch_id: int)`
- **Descrição:** Atualmente o `_cast_columns(batch_df)` roda antes de `_deduplicate_transactions(...)`. Como o deduplicate só usa `ingestion_ts` e `source_file`, altere a ordem para deduplicar **antes** de realizar os casts de decimal e data, evitando parsing de tipos em linhas descartadas.

**Código Esperado:**
```python
def _write_batch(batch_df: DataFrame, batch_id: int) -> None:
    # 1. Deduplica primeiro sobre o batch bruto
    deduped_df = _deduplicate_transactions(batch_df)
    
    # 2. Converte tipos de dados apenas nas linhas sobreviventes
    casted_df = _cast_columns(deduped_df)

    casted_df.persist()
    try:
        payload_columns = [c for c in casted_df.columns if c not in REJECTED_PAYLOAD_EXCLUDED_COLUMNS]
        valid_df, rejected_df = _split_batch(casted_df)

        if not rejected_df.isEmpty():
            rejected_count = rejected_df.count()
            logger.warning(f"[batch {batch_id}] {rejected_count} rows failed expectations, sending to {SILVER_REJECTED_TABLE}")
            (
                rejected_df
                .select(
                    F.to_json(F.struct(*payload_columns)).cast("string").alias("data"),
                    F.col("_failure_reason").cast("string").alias("failure_reason"),
                    F.current_timestamp().cast("timestamp").alias("rejected_at"),
                )
                .withColumn(
                    "rejected_at_month",
                    F.date_format(F.col("rejected_at"), "yyyy-MM").cast("string"),
                )
                .write.format("delta").mode("append").saveAsTable(SILVER_REJECTED_TABLE)
            )

        _upsert_valid(valid_df)
    finally:
        casted_df.unpersist()
```

---

### 🟢 Tarefa 2: Evitar `count()` desnecessário usando `isEmpty()` na Rejeição
- **Arquivo Target:** `imperativo/autoloader/silver/silver_transactions_current.py`
- **Função:** `_write_batch`
- **Descrição:** Em vez de executar `rejected_df.count()` em todo micro-batch, verifique primeiro `if not rejected_df.isEmpty():` para só contar e gravar se houver registros rejeitados de fato.

---

### 🟢 Tarefa 3: Short-circuit no `_upsert_valid` para Batches Vazio
- **Arquivo Target:** `imperativo/autoloader/silver/silver_transactions_current.py`
- **Função:** `_upsert_valid(valid_df: DataFrame)`
- **Descrição:** Se `valid_df.isEmpty()` for `True`, retorne imediatamente para não abrir transação ou executar plano de MERGE no Delta Lake sem dados válidos.

**Código Esperado:**
```python
def _upsert_valid(valid_df: DataFrame) -> None:
    if valid_df.isEmpty():
        return

    merge_key_condition = " AND ".join(f"target.{c} = source.{c}" for c in TRANSACTION_BUSINESS_KEY)
    ...
```

---

### 🟢 Tarefa 4: Refatorar `.withColumn` encadeados para `.withColumns`
- **Arquivo Target:** `imperativo/autoloader/silver/silver_transactions_current.py`
- **Função:** `_cast_columns(batch_df: DataFrame)`
- **Descrição:** Substituir as 9 chamadas encadeadas de `.withColumn(...)` por um único dicionário passado em `.withColumns({...})`.

**Código Esperado:**
```python
def _cast_columns(batch_df: DataFrame) -> DataFrame:
    return batch_df.withColumns({
        "transaction_conversion_date"     : F.col("transaction_conversion_date").cast("date"),
        "transaction_quota_price_amount"  : F.col("transaction_quota_price_amount").cast(DecimalType(20, 2)),
        "transaction_quota_quantity"      : F.col("transaction_quota_quantity").cast(DecimalType(20, 2)),
        "transaction_value_amount"        : F.col("transaction_value_amount").cast(DecimalType(20, 2)),
        "transaction_gross_value_amount"  : F.col("transaction_gross_value_amount").cast(DecimalType(20, 2)),
        "income_tax_amount"               : F.col("income_tax_amount").cast(DecimalType(20, 2)),
        "financial_transaction_tax_amount": F.col("financial_transaction_tax_amount").cast(DecimalType(20, 2)),
        "transaction_exit_fee_amount"     : F.col("transaction_exit_fee_amount").cast(DecimalType(20, 2)),
        "transaction_net_value_amount"    : F.col("transaction_net_value_amount").cast(DecimalType(20, 2)),
    })
```

---

### 🟢 Tarefa 5: Trocar `date_format` por `substring` na Camada Bronze
- **Arquivo Target:** `imperativo/autoloader/bronze/bronze_transactions_current.py`
- **Função:** `read_bronze_stream()`
- **Descrição:** Substituir `F.date_format(F.col("transaction.transactionConversionDate"), "yyyy-MM")` por `F.substring(F.col("transaction.transactionConversionDate"), 1, 7)` na definição do `transaction_conversion_month`.

**Código Esperado:**
```python
        "_rescued_data",
        F.substring(
            F.col("transaction.transactionConversionDate"), 1, 7
        ).alias("transaction_conversion_month"),
    )
```

---

### 🟢 Tarefa 6: Ativar Deletion Vectors nos DDLs de Tabelas Delta
- **Arquivos Target:**
  - `imperativo/autoloader/silver/tables_silver_config.py` (`create_silver_table`)
  - `imperativo/autoloader/bronze/tables_bronze_config.py` (`create_bronze_table`)
- **Descrição:** Adicionar a propriedade `'delta.enableDeletionVectors' = 'true'` em `TBLPROPERTIES` para reduzir Write Amplification em operações de `MERGE` e `UPDATE`.

**Código Esperado (`tables_silver_config.py`):**
```python
        TBLPROPERTIES (
            'quality' = 'silver',
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true',
            'delta.enableDeletionVectors' = 'true'
        )
```

---

### 🟢 Tarefa 7: Otimizar Configurações do Auto Loader
- **Arquivo Target:** `imperativo/autoloader/common/config.py`
- **Variável:** `CLOUDFILES_OPTIONS`
- **Descrição:** Adicionar a opção `"cloudFiles.maxBytesPerTrigger": "512m"` para estabilizar a volumetria de memória dos micro-batches.

**Código Esperado:**
```python
CLOUDFILES_OPTIONS = {
    "cloudFiles.format": "json",
    "cloudFiles.includeExistingFiles": "true",
    "cloudFiles.schemaEvolutionMode": "rescue",
    "cloudFiles.schemaLocation": BRONZE_CHECKPOINT_PATH,
    "cloudFiles.allowOverwrites": "true",
    "cloudFiles.maxFilesPerTrigger": "1000",
    "cloudFiles.maxBytesPerTrigger": "512m",
    "pathGlobFilter": "*.json",
    "multiline": "true",
}
```

---

### 🟢 Tarefa 8: Adicionar Configurações de AQE e Low Shuffle Merge na Spark Session
- **Arquivo Target:** `imperativo/autoloader/common/spark.py`
- **Função:** `get_spark()`
- **Descrição:** Adicionar as propriedades de `spark.sql.adaptive.enabled` e `spark.databricks.delta.merge.enableLowShuffle` no builder da `SparkSession`.

**Código Esperado:**
```python
def get_spark() -> SparkSession:
    spark = SparkSession.getActiveSession()

    if spark is None:
        spark = (
            SparkSession.builder
            .appName("POC OpenFinance")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.databricks.delta.merge.enableLowShuffle", "true")
            .getOrCreate()
        )

    return spark
```

---

## 📋 Checklist de Validação Pós-Implementação

- [ ] Executar o módulo `bronze/bronze_transactions_current.py` e verificar se a tabela Bronze é populada sem erros.
- [ ] Executar o módulo `silver/silver_transactions_current.py` e validar que as transações válidas sofrem `MERGE` corretamente.
- [ ] Verificar na Spark UI do Databricks se o tempo de execução do `foreachBatch` diminuiu e se não há shuffles desnecessários.
