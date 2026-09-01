# Relatório de Análise de Performance e Otimizações do Pipeline PySpark / Databricks

> **Módulo:** `imperativo/autoloader`  
> **Camadas Impactadas:** `common`, `bronze`, `silver`  
> **Objetivo:** Fornecer um guia técnico detalhado para correção de gargalos de performance, eliminando custos excessivos de computação (re-computação de DAG, falta de pruning no MERGE Delta) e otimizando a escala da solução.

---

## 1. Resumo Executivo

Após auditoria completa do pipeline streaming (`Auto Loader` + `Structured Streaming` + `Delta Lake`), identificamos que o pipeline **não está performático para ambientes de produção com alto volume de dados**. 

Apesar de utilizar tecnologias modernas do Databricks (como Auto Loader e Liquid Clustering `CLUSTER BY`), existem falhas graves de design no processamento por micro-batch que causam:
1. **Full Table Scans** recorrentes em todas as operações de `MERGE` na camada Silver.
2. **Re-computação redundante de gráficos de transformação (DAG)** e shuffles de windowing dentro do `foreachBatch`.
3. **Conversões ineficientes de tipos/datas** no Spark JVM.
4. **Fragmentação de arquivos** por falta de configurações de auto-compactação no Delta Lake.

---

## 2. Gargalos Identificados e Soluções Propostas

### 🚨 2.1. Falta de Pruning no `MERGE` da Camada Silver
- **Arquivo:** `silver/silver_transactions_current.py` (`_upsert_valid`)
- **Problema:** A tabela Silver está configurada com Liquid Clustering em `CLUSTER BY (transaction_conversion_month)`. Porém, o predicado do `MERGE` utiliza apenas `client_id` e `transaction_id`:
  ```python
  merge_key_condition = "target.client_id = source.client_id AND target.transaction_id = source.transaction_id"
  ```
- **Impacto:** O engine do Databricks não consegue aplicar *Data Pruning* (poda de arquivos). A cada micro-batch inserido, o Spark é obrigado a varrer a tabela Silver inteira (*Full Scan*). Conforme a tabela cresce, o tempo de processamento aumentará exponencialmente.
- **Solução:** Incluir a chave de clustering `transaction_conversion_month` na condição do `MERGE`:
  ```python
  merge_key_condition = (
      "target.transaction_conversion_month = source.transaction_conversion_month AND "
      "target.client_id = source.client_id AND "
      "target.transaction_id = source.transaction_id"
  )
  ```

---

### 🚨 2.2. Re-computação do DAG e Ausência de Cache no `foreachBatch`
- **Arquivo:** `silver/silver_transactions_current.py` (`_write_batch`)
- **Problema:** No processamento de cada micro-batch, são disparadas múltiplas ações Spark (`rejected_df.count()`, escrita de rejeitados e `_upsert_valid`). Como o `deduped_df` faz operações custosas de Window (`row_number() over partitionBy(...)`) e **não utiliza `.persist()`**, o Spark recalcula todo o DAG e executa a ordenação/shuffle 2 a 3 vezes por micro-batch.
- **Impacto:** Overhead massivo de CPU e Memory Shuffle em cada micro-batch.
- **Solução:** Adicionar `deduped_df.persist()` no início do micro-batch e `deduped_df.unpersist()` em um bloco `finally`.

---

### ⚡ 2.3. Conversão Ineficiente de Mês/Data na Camada Bronze
- **Arquivo:** `bronze/bronze_transactions_current.py` (`read_bronze_stream`)
- **Problema:** O cálculo de `transaction_conversion_month` faz duas conversões de formato desnecessárias:
  ```python
  F.to_date(
      F.date_format(F.col("transaction.transactionConversionDate"), "yyyy-MM"), "yyyy-MM"
  ).alias("transaction_conversion_month")
  ```
- **Impacto:** Formatar em String para depois parsing de volta para Date consome mais CPU do executor Spark JVM.
- **Solução:** Substituir pela função nativa de truncamento de data:
  ```python
  F.trunc(F.to_date(F.col("transaction.transactionConversionDate")), "MM").alias("transaction_conversion_month")
  ```

---

### ⚙️ 2.4. Ausência de Auto-Otimização de Arquivos no Delta Lake
- **Arquivos:** `bronze/tables_bronze_config.py` e `silver/tables_silver_config.py`
- **Problema:** As tabelas Delta são criadas sem as propriedades de compactação automática e otimização de escrita.
- **Impacto:** Escritas streaming constantes geram milhares de arquivos pequenos (Small Files Problem), degradando a performance de leitura e queries analíticas.
- **Solução:** Incluir as propriedades `TBLPROPERTIES` no DDL SQL:
  ```sql
  TBLPROPERTIES (
      'quality' = 'silver',
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact' = 'true'
  )
  ```

---

### 🔍 2.5. Ajuste no Liquid Clustering da Camada Bronze
- **Arquivo:** `bronze/tables_bronze_config.py` (`create_bronze_table`)
- **Problema:** A tabela Bronze possui `CLUSTER BY (transaction_conversion_month, transaction_id)`.
- **Impacto:** `transaction_id` é uma chave de altíssima cardinalidade (GUID/String). No Liquid Clustering, colocar chaves de alta cardinalidade sem necessidade de filtro de busca direta gera custo extra de manutenção.
- **Solução:** Ajustar o clustering da Bronze para focar na data ou na combinação `(transaction_conversion_month, client_id)`.

---

## 3. Alterações Exatas de Código Requeridas

### 📄 File: `bronze/tables_bronze_config.py`
Adicionar propriedades Delta de compactação automática e ajustar o Liquid Clustering:
```python
def create_bronze_table() -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
            client_id                          STRING,
            investiment_id                     STRING,
            transaction_id                     STRING,
            type                                STRING,
            transaction_type                   STRING,
            transaction_type_additional_info   STRING,
            transaction_conversion_date        STRING,
            transaction_quota_price_amount     STRING,
            transaction_quota_price_currency   STRING,
            transaction_quota_quantity         STRING,
            transaction_value_amount           STRING,
            transaction_value_currency         STRING,
            transaction_gross_value_amount     STRING,
            transaction_gross_value_currency   STRING,
            income_tax_amount                  STRING,
            income_tax_currency                STRING,
            financial_transaction_tax_amount   STRING,
            financial_transaction_tax_currency STRING,
            transaction_exit_fee_amount        STRING,
            transaction_exit_fee_currency      STRING,
            transaction_net_value_amount       STRING,
            transaction_net_value_currency     STRING,
            source_file                        STRING,
            ingestion_ts                       TIMESTAMP,
            ingestion_date                     DATE,
            _rescued_data                      STRING,
            transaction_conversion_month       DATE
        )
        USING DELTA
        CLUSTER BY (transaction_conversion_month, client_id)
        COMMENT 'Bronze layer - Fundos de Investimentos - Transactions Current'
        TBLPROPERTIES (
            'quality' = 'bronze',
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
    """)
```

---

### 📄 File: `bronze/bronze_transactions_current.py`
Otimizar o truncamento do mês de conversão:
```python
# Na função read_bronze_stream():
        "source_file",
        "ingestion_ts",
        "ingestion_date",
        "_rescued_data",
        F.trunc(F.to_date(F.col("transaction.transactionConversionDate")), "MM").alias("transaction_conversion_month"),
    )
```

---

### 📄 File: `silver/tables_silver_config.py`
Adicionar propriedades Delta de auto-compactação nas tabelas Silver principal e de rejeitos:
```python
def create_silver_table() -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            ...
        )
        USING DELTA
        CLUSTER BY (transaction_conversion_month)
        COMMENT 'Silver layer - Fundos de Investimentos - Transactions Current'
        TBLPROPERTIES (
            'quality' = 'silver',
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
    """)

def create_silver_rejected_table() -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_REJECTED_TABLE} (
            ...
        )
        USING DELTA
        PARTITIONED BY (rejected_at_month)
        COMMENT 'Quarentena - Silver layer - Fundos de Investimentos - Transactions Current - linhas que falharam expectations'
        TBLPROPERTIES (
            'quality' = 'silver_rejected',
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
    """)
```

---

### 📄 File: `silver/silver_transactions_current.py`
Implementar Pruning no `MERGE` e Caching no `foreachBatch`:
```python
def _upsert_valid(valid_df: DataFrame) -> None:
    # Adicionada a chave de clustering transaction_conversion_month para permitir Pruning
    merge_key_condition = (
        "target.transaction_conversion_month = source.transaction_conversion_month AND "
        "target.client_id = source.client_id AND "
        "target.transaction_id = source.transaction_id"
    )
    source_order = ", ".join(f"source.{c}" for c in DEDUP_ORDER)
    target_order = ", ".join(f"target.{c}" for c in DEDUP_ORDER)
    newer_condition = f"struct({source_order}) >= struct({target_order})"

    target_table = DeltaTable.forName(spark, SILVER_TABLE)
    (
        target_table.alias("target")
        .merge(valid_df.alias("source"), merge_key_condition)
        .whenMatchedUpdateAll(condition=newer_condition)
        .whenNotMatchedInsertAll()
        .execute()
    )


def _write_batch(batch_df: DataFrame, batch_id: int) -> None:
    casted_df = _cast_columns(batch_df)
    deduped_df = _deduplicate_transactions(casted_df)
    
    # 1. Caching do DataFrame deduplicado para evitar recomputar a Window function
    deduped_df.persist()

    try:
        payload_columns = [c for c in deduped_df.columns if c not in REJECTED_PAYLOAD_EXCLUDED_COLUMNS]
        valid_df, rejected_df = _split_batch(deduped_df)

        rejected_count = rejected_df.count()
        if rejected_count > 0:
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
        # 2. Desalocação do cache da memória RAM após a conclusão do micro-batch
        deduped_df.unpersist()
```

---

## 4. Plano de Validação e Teste

1. **Validação de Pruning:**
   - Executar uma rodada do pipeline e verificar o plano de execução (`EXPLAIN MERGE`) no Databricks Spark UI.
   - Confirmar se o `DataFilters` do merge indica eliminação de arquivos via `transaction_conversion_month`.
2. **Validação de Memória e Shuffles:**
   - Verificar na aba *Details* do job se a contagem de tarefas de Shuffle diminuiu durante o `foreachBatch`.
3. **Validação Funcional:**
   - Garantir que todas as transações válidas continuam sendo inseridas/atualizadas na tabela Silver sem duplicidade.
   - Garantir que transações inválidas continuam fluindo para a tabela de rejeito `silver_transactions_current_rechaco`.
