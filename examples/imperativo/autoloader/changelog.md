# Pipeline imperativo (autoloader) - bronze_transactions_current

Reescrita do pipeline declarativo `dlt/bronze/bronze_transactions_current.py` (Lakeflow
Declarative Pipelines / `pyspark.pipelines`) em uma forma imperativa, usando Auto Loader
(`cloudFiles`) + Structured Streaming puro, sem `@dp.table` / `@dp.expect`.

> **Nota de reorganização:** este README foi movido de `imperativo/README.md` para
> `imperativo/autoloader/README.md` quando o diretório `imperativo/` foi dividido em dois
> pipelines isolados: `imperativo/autoloader/` (este, com Auto Loader + Structured Streaming) e
> `imperativo/batch/` (sem Auto Loader, leitura em lote — ver `imperativo/batch/README.md`).
> Todo o histórico abaixo ("O que foi feito") descreve a evolução deste pipeline **antes** da
> divisão, quando `common/`, `bronze/` e `silver/` ficavam direto em `imperativo/` — os caminhos
> citados nos itens 1-13 são relativos a esse layout antigo e não foram reescritos, para manter o
> registro histórico fiel. A partir da divisão, todo caminho é relativo a `imperativo/autoloader/`.

> **Convenção:** este README deve ser atualizado a cada alteração relevante neste pipeline
> (`imperativo/autoloader/`) — inclusive mudanças pequenas de organização de arquivos, imports ou
> nomes. Ver `imperativo/PIPELINE.md` para o desenho completo e atualizado de cada camada, e
> `imperativo/FOREACHBATCH.md` para o porquê do uso de `foreachBatch` na silver e suas
> alternativas. O pipeline `batch/` tem seu próprio README (`imperativo/batch/README.md`) e é
> atualizado separadamente.

## Estrutura

```
imperativo/autoloader/
├── common/
│   ├── config.py               # paths, tabelas, opções do Auto Loader e reader
│   └── spark.py                 # obtenção/criação da SparkSession
├── bronze/
│   ├── bronze_transactions_current.py   # pipeline (leitura, escrita, start)
│   └── tables_bronze_config.py          # schema UC, payload StructType e DDL da tabela bronze
└── silver/
    ├── silver_transactions_current.py   # pipeline (cast, split, escrita, start)
    └── tables_silver_config.py          # schema UC e DDL das tabelas silver/rejeitados
```

## O que foi feito

1. **Estrutura inicial (`imperativo/common` + `imperativo/bronze`)**
   - `common/schemas.py`: cópia fiel de `dlt/common/schemas.py` (`transactions_current_schema`).
   - `common/config.py`: cópia de `dlt/common/config.py` (`INPUT_PATH`, `CLOUDFILES_OPTIONS`,
     `cloudfiles_reader`), com dois acréscimos necessários por não haver mais uma pipeline
     declarativa gerenciando destino/checkpoint:
     - `CHECKPOINT_PATH`: local do checkpoint da streaming query.
     - `TARGET_TABLE`: tabela Delta de destino (`catalog.schema.table`) — ajustar conforme o
       Unity Catalog do ambiente.
     - `REJECTED_TABLE`: tabela Delta de quarentena para linhas que falham expectations (ver
       item 7).
   - `bronze/bronze_transactions_current.py`, com:
     - `create_target_table()`: cria a tabela Delta explicitamente via DDL (`CREATE TABLE IF
       NOT EXISTS ... USING DELTA`), incluindo `CLUSTER BY (transaction_conversion_month,
       transaction_id)` e `TBLPROPERTIES ('quality' = 'bronze')` — no modo declarativo isso era
       implícito nos parâmetros de `@dp.table` (`cluster_by`, `table_properties`).
     - `read_bronze_stream()`: mesma leitura via Auto Loader (`cloudfiles_reader` +
       `.schema(SCHEMA)` + `.load(INPUT_PATH)`), explode de `data`, seleção/cast das colunas e
       derivação de `transaction_conversion_month` — lógica de transformação idêntica à da
       função declarativa original.
     - `_write_batch(batch_df, batch_id)`: grava cada micro-batch em modo `append` na
       `TARGET_TABLE`.
     - `start_bronze_stream()`: monta o `writeStream.foreachBatch(_write_batch)` com
       `checkpointLocation` e `trigger(availableNow=True)`, e retorna a `StreamingQuery`.
     - bloco `if __name__ == "__main__"`: inicia o stream e aguarda término
       (`query.awaitTermination()`).
   - As **expectations** (`@dp.expect("valid_business_key", ...)` e
     `@dp.expect("no_rescued_data", ...)`) foram inicialmente replicadas de forma imperativa:
     um dicionário `EXPECTATIONS` com as mesmas duas condições, calculado a cada micro-batch
     em `_log_expectations()`, que contava e logava (via `print`) quantas linhas violavam cada
     condição — sem descartar nenhuma linha, replicando o comportamento "warn-only" padrão do
     `@dp.expect` (sem `_or_drop`/`_or_fail`).

2. **Centralização da SparkSession (`common/spark.py`)**
   - Usuário criou `imperativo/common/spark.py` com `get_spark()` (retorna a sessão ativa ou
     cria uma nova com `appName("POC OpenFinance")`) e a instância `spark` no nível do módulo.
   - `bronze_transactions_current.py` foi ajustado para importar `spark` de `common.spark` em
     vez de instanciar `SparkSession` diretamente. Os parâmetros `spark: SparkSession` foram
     removidos das funções `create_target_table`, `read_bronze_stream` e `start_bronze_stream`,
     que passaram a usar o `spark` importado; o bloco `__main__` deixou de criar a sessão e
     apenas chama `start_bronze_stream()`.

3. **Remoção e posterior reintrodução das expectations**
   - As expectations e o `_write_batch` nomeado chegaram a ser removidos (dicionário
     `EXPECTATIONS` e função `_log_expectations` apagados, `foreachBatch` virou lambda inline
     fazendo só o `append`), ficando temporariamente **sem nenhuma validação** de
     `transaction_id`/`client_id` não nulos nem de `_rescued_data`.
   - Reintroduzidos novamente: `EXPECTATIONS` e `_log_expectations(batch_df, batch_id)`, que
     conta e loga (via `print`) quantas linhas violam cada condição, sem descartar nenhuma
     linha — mesmo comportamento "warn-only" do `@dp.expect` original.
   - `EXPECTATIONS` ficou apenas com `valid_business_key`
     (`transaction_id IS NOT NULL AND client_id IS NOT NULL`); a condição `no_rescued_data`
     (`_rescued_data IS NULL`) foi removida do dicionário e não é mais checada/logada.
   - Constatado que `IS NOT NULL` não cobre string vazia (`""`) — uma linha com
     `client_id = ""` passava como válida, já que `""` não é `NULL` em SQL. Condição de
     `valid_business_key` ajustada para também exigir `transaction_id != ''` e
     `client_id != ''`, cobrindo tanto `NULL` quanto string vazia.
   - Tentativa 1: trocar `print` por `logging.getLogger("bronze_transactions_current")` +
     `logging.basicConfig(level=logging.INFO)`. **Não funcionou** — o log continuou não
     aparecendo. Causa: no Databricks o root logger Python já vem com handlers configurados
     pelo próprio runtime antes do código do usuário rodar; `logging.basicConfig(...)` **não
     faz nada** quando o root logger já tem handlers (é um no-op silencioso da stdlib, a menos
     que se passe `force=True`). Mesmo com `force=True`, a saída do `logging` padrão do Python
     não é roteada de forma confiável para a UI de Driver Logs do Databricks quando emitida a
     partir da thread em background do `foreachBatch`.
   - Tentativa 2: usar o logger Log4j da própria JVM do Spark, via `spark._jvm`
     (`spark._jvm.org.apache.log4j.LogManager.getLogger(...)`). **Não é viável neste
     ambiente**: falha com `[JVM_ATTRIBUTE_NOT_SUPPORTED] Directly accessing the underlying
     Spark driver JVM using the attribute '_jvm' is not supported on serverless compute` — o
     ambiente de execução é serverless, que não expõe a JVM subjacente ao código Python
     (sandboxing de segurança/isolamento do serverless).
   - Tentativa 3 (atual): voltar para `logging` do Python, mas sem depender do root logger
     (que é o que fazia o `basicConfig` da tentativa 1 ser um no-op). Em vez disso, um handler
     é anexado diretamente no logger nomeado do módulo:
     ```python
     logger = logging.getLogger("bronze_transactions_current")
     logger.setLevel(logging.WARNING)
     logger.propagate = False
     if not logger.handlers:
         _handler = logging.StreamHandler(sys.stdout)
         _handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
         logger.addHandler(_handler)
     ```
     Isso garante que o logger sempre tenha seu próprio `StreamHandler` escrevendo em
     `sys.stdout`, independente de como o root logger foi configurado (ou não) pelo runtime —
     compatível com serverless compute, já que não toca em nada relacionado à JVM.
     `logger.warning(...)` voltou a ser o método usado (API padrão do `logging`, diferente do
     `warn` do Log4j da tentativa 2).
   - `_write_batch(batch_df, batch_id)` voltou a existir como função nomeada, chamando
     `_log_expectations` e em seguida gravando o micro-batch em `append` na `TARGET_TABLE`.
     `start_bronze_stream()` usa `.foreachBatch(_write_batch)` em vez do lambda inline.

4. **`cloudFiles.schemaLocation` ausente**
   - Mesmo com `.schema(SCHEMA)` explícito (schema não é inferido a partir dos arquivos), o
     Auto Loader ainda exige `cloudFiles.schemaLocation` sempre que
     `cloudFiles.schemaEvolutionMode` for diferente de `"none"` (aqui está como `"rescue"`) —
     essa opção não existia em `CLOUDFILES_OPTIONS` e foi adicionada.
   - Esse local é onde o Auto Loader persiste o estado de evolução do schema entre reinícios
     do stream (ele cria uma subpasta interna `_schemas/` ali dentro); sem ele o schema
     evolution/rescue não é rastreado de forma consistente entre execuções.
   - Solução adotada: reaproveitar o próprio `CHECKPOINT_PATH` como `cloudFiles.schemaLocation`
     (padrão recomendado pela Databricks, em vez de criar um path novo/dedicado só para isso).

5. **Controle de volume por trigger (`maxFilesPerTrigger`)**
   - Adicionada a opção `cloudFiles.maxFilesPerTrigger: "1000"` em `CLOUDFILES_OPTIONS`, para
     limitar quantos arquivos novos o Auto Loader processa em cada micro-batch/trigger.
   - Isso evita que um backlog grande de arquivos acumulados (ex.: após um período parado, ou
     no primeiro `includeExistingFiles=true`) seja lido de uma vez só em um único trigger,
     reduzindo picos de memória/shuffle no cluster e permitindo processamento mais previsível
     e incremental ao longo de várias execuções.

6. **`foreachBatch` com lambda inline, depois revertido para função nomeada**
   - Em um dado momento `_write_batch` não fazia nada além do `append` puro (sem expectations,
     sem uso real de `batch_id`), e a função nomeada foi removida em favor de um lambda inline
     em `start_bronze_stream()` — mantendo o `foreachBatch` como ponto de extensão, mas com
     menos código.
   - Com a reintrodução das expectations (item 3), `_write_batch` voltou a ter mais de uma
     responsabilidade (logar expectations + gravar), então a função nomeada foi restaurada e
     `start_bronze_stream()` voltou a usar `.foreachBatch(_write_batch)`.

7. **Expectations passaram de "warn-only" (log) para quarentena real (tabela de rechaço)**
   - Motivo: logar violações não impedia que linhas inválidas fossem parar na `TARGET_TABLE`;
     decidido rejeitar de fato as linhas que falham alguma `EXPECTATIONS` e enviá-las para uma
     tabela separada em vez de só logar.
   - Nova tabela `REJECTED_TABLE` (`common/config.py`):
     `imperative_open_finance_funds_investiments_transactions_current.bronze.bronze_transactions_current_rechaco`,
     criada por `create_rejected_table()` (mesmo padrão de `create_target_table()`, DDL
     explícito). Schema inicial usava uma coluna única `rescue_data STRING` com a linha
     inteira serializada em JSON — **trocado** para as mesmas colunas "normais" da
     `TARGET_TABLE` (todas as colunas do payload transformado, incluindo `source_file`,
     `ingestion_ts`, `_rescued_data`, `transaction_conversion_month` etc.) mais três colunas de
     metadados adicionais:
     - `failure_reason STRING` — nomes das expectations que a linha violou, concatenados por
       `", "` (ex.: `"valid_business_key"`).
     - `batch_id BIGINT`, `rejected_at TIMESTAMP` — id do micro-batch e timestamp da rejeição.
     Motivo da troca: manter a linha rejeitada com as mesmas colunas tipadas da tabela
     principal (mais fácil de consultar/filtrar/comparar direto via SQL), em vez de exigir
     `from_json`/parsing manual de uma coluna JSON para inspecionar os dados.
   - `_split_batch(batch_df)` (substitui `_log_expectations`): para cada regra em
     `EXPECTATIONS`, avalia `NOT (condição)` por linha; monta um array com o nome de cada
     expectation violada (`F.when(~F.expr(condition), F.lit(name))`, filtrando os `null` com
     `F.filter(..., lambda x: x.isNotNull())`) e concatena em `_failure_reason` via
     `F.concat_ws(", ", ...)`. Linhas com `_failure_reason == ""` viram `valid_df`; as demais
     viram `rejected_df`. Retorna a tupla `(valid_df, rejected_df)`.
   - `_write_batch(batch_df, batch_id)` agora:
     1. chama `_split_batch` para separar válidas/rejeitadas;
     2. se houver linhas rejeitadas, loga a contagem (`logger.warning`) e grava as colunas
        originais do batch (`*original_columns`) + `failure_reason`/`batch_id`/`rejected_at`
        em `append` na `REJECTED_TABLE`;
     3. grava `valid_df` em `append` na `TARGET_TABLE` (como antes).
   - `start_bronze_stream()` chama `create_rejected_table()` além de `create_target_table()`
     antes de iniciar o stream.
   - Diferença do comportamento anterior do `@dp.expect` declarativo: lá, mesmo com
     `@dp.expect` "puro" (sem `_or_drop`), nenhuma linha era removida da tabela final — aqui,
     linhas inválidas **não** entram mais na `TARGET_TABLE`, ficando só na `REJECTED_TABLE`
     para investigação/reprocessamento posterior.

8. **Reorganização de arquivos dentro de `imperativo/bronze`**
   - `create_target_table()` e `create_rejected_table()` foram extraídas de
     `bronze_transactions_current.py` para um novo módulo `bronze/tables_bronze.py` (agrupa as
     duas funções de DDL das tabelas, que antes ficavam misturadas com a lógica de
     leitura/escrita do stream).
   - `common/schemas.py` foi movido para `bronze/schemas_bronze.py` (mesmo conteúdo, só
     renomeado/realocado) — deixou de existir em `common/`.
   - `bronze_transactions_current.py` ajustado para importar dessas novas localizações:
     `from schemas_bronze import transactions_current_schema` e `from tables_bronze import
     create_rejected_table, create_target_table` (imports "irmãos", já que o script roda
     diretamente de dentro de `bronze/` e o Python adiciona automaticamente o diretório do
     script ao `sys.path`). O restante do pipeline (`_split_batch`, `read_bronze_stream`,
     `_write_batch`, `start_bronze_stream`) permanece em `bronze_transactions_current.py`.
   - `common/` ficou só com `config.py` e `spark.py` (utilitários realmente compartilháveis por
     outras camadas futuras, se vierem a existir); o que é específico da camada bronze
     (schema do payload, DDL das tabelas bronze) passou a viver dentro de `bronze/`.

9. **`REJECTED_TABLE` trocou de "mesmas colunas da bronze" para `data` (JSON) + metadados,
   particionada por mês**
   - A versão anterior gravava a linha rejeitada com as mesmas colunas tipadas da
     `TARGET_TABLE` inteira (incluindo `ingestion_ts`, `ingestion_date`, `_rescued_data`,
     `transaction_conversion_month` — colunas que só existem por causa do processamento da
     bronze, não fazem parte do payload de negócio em si).
   - Novo schema de `REJECTED_TABLE` (`tables_bronze.py`), bem mais enxuto:
     - `data STRING` — o payload rejeitado serializado em JSON, mas **sem** as colunas
       técnicas adicionadas pela bronze (`ingestion_ts`, `ingestion_date`, `_rescued_data`,
       `transaction_conversion_month`); mantém as 21 colunas de negócio + `source_file`.
     - `failure_reason STRING`, `batch_id BIGINT`, `rejected_at TIMESTAMP` — inalterados.
     - `rejected_at_month STRING` — novo, derivado de `rejected_at` via `date_format(...,
       "yyyy-MM")`.
   - `REJECTED_TABLE` agora é `PARTITIONED BY (rejected_at_month)` (particionamento Hive
     tradicional, não `CLUSTER BY`/Liquid Clustering como a `TARGET_TABLE`) — facilita
     consultas/expurgo por período de rejeição e evita pastas com volume desbalanceado ao
     longo do tempo.
   - `bronze_transactions_current.py`: nova constante `REJECTED_PAYLOAD_EXCLUDED_COLUMNS`
     (`{"ingestion_ts", "ingestion_date", "_rescued_data", "transaction_conversion_month"}`).
     `_write_batch` monta `payload_columns = [c for c in batch_df.columns if c not in
     REJECTED_PAYLOAD_EXCLUDED_COLUMNS]` e serializa só essas colunas via
     `F.to_json(F.struct(*payload_columns))` na coluna `data`; `rejected_at_month` é derivado
     da própria coluna `rejected_at` já materializada (`F.date_format(F.col("rejected_at"),
     "yyyy-MM")`), garantindo que os dois valores fiquem consistentes entre si.
   - Cada coluna gravada na `REJECTED_TABLE` recebeu `.cast(...)` explícito para o tipo
     declarado em `create_rejected_table()` (`data`/`failure_reason`/`rejected_at_month` →
     `string`, `batch_id` → `bigint`, `rejected_at` → `timestamp`). Motivo: sem esses casts, os
     tipos já sairiam corretos na prática (as funções `to_json`/`concat_ws`/`current_timestamp`/
     `date_format` têm retorno de tipo fixo, e `F.lit(batch_id)` sobre `int` já infere `bigint`),
     mas o schema ficava implícito — dependendo do tipo de retorno de cada expressão em vez de
     estar explícito no código, diferente do padrão já usado em `read_bronze_stream()` para a
     `TARGET_TABLE` (onde cada campo tem `.cast(...)` explícito). Deixar explícito documenta a
     intenção e funciona como trava caso alguma expressão mude de comportamento no futuro.

10. **Divisão em bronze + silver: bronze volta a ser só landing (tudo como `STRING`, tabela
    única); casts, expectations e quarentena migram para uma nova camada `silver`**
    - Motivo: a bronze estava fazendo trabalho de mais de uma camada — tipagem de negócio
      (`DECIMAL`/`DATE`) e validação/quarentena (`EXPECTATIONS`/`_split_batch`) não são
      responsabilidade de uma bronze no sentido clássico de medallion architecture (cópia fiel
      da origem, schema-on-read). Decidido separar: bronze só ingere e grava tudo; silver tipa,
      valida e separa válido/rejeitado.
    - `imperativo/bronze/` ficou reduzido a uma única tabela (`BRONZE_TABLE`, renomeada de
      `TARGET_TABLE`): `create_bronze_table()` em `tables_bronze.py` cria as mesmas colunas de
      negócio de antes, mas todas como `STRING` (nenhum `.cast(...)` para `DECIMAL`/`DATE`) —
      inclusive `transaction_conversion_date`, que antes era `DATE`. A coluna derivada
      `transaction_conversion_month` deixou de existir na bronze (dependia da data já tipada);
      `CLUSTER BY` trocou de `(transaction_conversion_month, transaction_id)` para
      `(ingestion_date)`, única coluna técnica de particionamento lógico disponível agora nesta
      camada.
    - `read_bronze_stream()` manteve exatamente a mesma leitura via Auto Loader
      (`.schema(SCHEMA)` + `explode_outer("data")`) e a mesma lista de colunas projetadas — só
      removendo os `.cast(...)` de cada campo. `EXPECTATIONS`, `_split_batch` e
      `REJECTED_PAYLOAD_EXCLUDED_COLUMNS` foram removidos do módulo da bronze inteiramente (não
      há mais rejeição nesta camada — toda linha lida é gravada).
    - `_write_batch`/`foreachBatch` também saíram da bronze: como não há mais split nem lógica
      por micro-batch, `start_bronze_stream()` passou a usar o sink nativo do Structured
      Streaming (`.writeStream.format("delta").outputMode("append")...toTable(BRONZE_TABLE)`)
      em vez de `foreachBatch` com uma função nomeada — mais simples, já que "carregar tudo" já
      é o comportamento padrão de um append stream sem transformação por batch.
    - Nova camada `imperativo/silver/` (mesmo padrão de organização da bronze):
      - `tables_silver.py`: `create_silver_table()` (mesmo shape que a antiga bronze tipada —
        `DECIMAL(20, 2)`/`DATE` nos campos de negócio, `transaction_conversion_month`,
        `CLUSTER BY (transaction_conversion_month, transaction_id)`) e
        `create_silver_rejected_table()` (mesmo schema enxuto da antiga `REJECTED_TABLE`: `data`
        JSON + `failure_reason`/`batch_id`/`rejected_at`/`rejected_at_month`, particionada por
        `rejected_at_month`).
      - `silver_transactions_current.py`: `EXPECTATIONS` e `_split_batch` migraram literalmente
        da antiga bronze (mesma condição `valid_business_key`, mesma lógica de array de nomes
        violados + `concat_ws`). Nova função `_cast_columns(batch_df)` reúne os `.cast(...)` que
        antes viviam em `read_bronze_stream()` (8 campos monetários/quantidade →
        `DecimalType(20, 2)`, `transaction_conversion_date` → `date`) e deriva
        `transaction_conversion_month` a partir da data já tipada. `read_silver_stream()` lê a
        `BRONZE_TABLE` como streaming source via `spark.readStream.table(BRONZE_TABLE)` (Delta
        Lake suporta stream incremental nativo sobre uma tabela Delta, sem precisar de Auto
        Loader/`cloudFiles`) — usa seu próprio checkpoint (`SILVER_CHECKPOINT_PATH`), separado do
        checkpoint arquivos→bronze. `_write_batch` da silver segue a mesma sequência da antiga
        `_write_batch` da bronze (casta → separa via `_split_batch` → grava rejeitadas com
        `to_json` → grava válidas), só que operando sobre um micro-batch lido da `BRONZE_TABLE`
        em vez de arquivos.
    - `common/config.py`: `TARGET_TABLE`/`REJECTED_TABLE`/`CHECKPOINT_PATH` renomeados/divididos
      em `BRONZE_TABLE`, `SILVER_TABLE`, `SILVER_REJECTED_TABLE`, `BRONZE_CHECKPOINT_PATH` (o
      antigo `CHECKPOINT_PATH`, ainda reaproveitado como `cloudFiles.schemaLocation`) e
      `SILVER_CHECKPOINT_PATH` (novo, exclusivo do stream bronze→silver).
    - Efeito colateral aceito: a `BRONZE_TABLE` agora pode acumular linhas com
      `transaction_id`/`client_id` nulos ou malformados (nada é rejeitado nesta camada) — isso é
      intencional (bronze = cópia fiel do que chegou), mas significa que ela cresce com linhas
      que nunca vão virar dado utilizável na silver. Ver `imperativo/PIPELINE.md` para o desenho
      completo atualizado.

11. **`SCHEMA_NOT_FOUND` ao criar as tabelas da silver + consolidação dos módulos de config em
    um único arquivo por camada**
    - Erro `[SCHEMA_NOT_FOUND] The schema
      imperative_open_finance_funds_investiments_transactions_current.silver cannot be found`
      ao rodar `create_silver_table()`/`create_silver_rejected_table()`. Causa: `CREATE TABLE IF
      NOT EXISTS catalog.schema.tabela` no Unity Catalog **não cria o schema pai** automaticamente
      — só a tabela. O schema `bronze` já existia (de execuções anteriores/criação manual), mas o
      schema `silver` nunca tinha sido criado.
    - Solução: `create_bronze_schema()`/`create_silver_schema()`, cada um rodando `CREATE SCHEMA
      IF NOT EXISTS {catalog}.{schema}` — o `catalog.schema` é **derivado** de `BRONZE_TABLE`/
      `SILVER_TABLE` (`".".join(TABLE.split(".")[:2])`) em vez de hardcoded como string literal,
      para nunca ficar dessincronizado caso o catalog/schema mude em `common/config.py`.
    - As duas funções são chamadas **no nível do módulo**, logo após os imports em
      `bronze_transactions_current.py`/`silver_transactions_current.py` — mesmo padrão já usado
      para `SCHEMA = transactions_current_schema()` na bronze (calculado uma vez na carga do
      módulo), e não dentro de `start_bronze_stream()`/`start_silver_stream()` (que continuam
      chamando só a criação das tabelas, como antes).
    - Reorganização adicional: os arquivos separados de cada camada (`schemas_bronze.py` +
      `tables_bronze.py`; `schema_silver.py` + `tables_silver.py`) foram consolidados em um único
      módulo por camada — `bronze/tables_bronze_config.py` e `silver/tables_silver_config.py` —
      reunindo schema do Unity Catalog, schema do payload (StructType, só na bronze) e DDL das
      tabelas. `bronze_transactions_current.py`/`silver_transactions_current.py` passaram a
      importar tudo de um só lugar por camada.
    - Mesmo problema também existe para **volumes** do Unity Catalog: `BRONZE_CHECKPOINT_PATH`/
      `SILVER_CHECKPOINT_PATH` (usados como `checkpointLocation` e, na bronze, também como
      `cloudFiles.schemaLocation`) apontam para dentro de um volume `checkpoints` em cada schema
      (`.../bronze/checkpoints/...`, `.../silver/checkpoints/...`) que também nunca era criado
      explicitamente. Adicionadas `create_bronze_checkpoints_volume()`/
      `create_silver_checkpoints_volume()` em `tables_bronze_config.py`/`tables_silver_config.py`,
      rodando `CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}` — o identificador do
      volume é **derivado** do próprio `BRONZE_CHECKPOINT_PATH`/`SILVER_CHECKPOINT_PATH`
      (`".".join(PATH.strip("/").split("/")[1:4])`, já que o path segue o formato
      `/Volumes/{catalog}/{schema}/{volume}/...`), mesma lógica de derivação usada para
      `BRONZE_SCHEMA`/`SILVER_SCHEMA`.
    - Diferente das funções de schema (chamadas no nível do módulo), as de volume são chamadas
      dentro de `start_bronze_stream()`/`start_silver_stream()`, logo antes de
      `create_bronze_table()`/`create_silver_table()` — o volume só é necessário quando o stream
      de fato inicia (leitura/escrita), não no momento em que o módulo é importado.

12. **Deduplicação na silver (`_deduplicate_transactions`), portada de outro pipeline**
    - Conceito trazido de `common/transformations.py` de outro pipeline (`deduplicate_bills`,
      para linhas de fatura): uma `Window` particionada pela chave de negócio, ordenada
      (desc) por colunas de "recência", mantendo só a primeira linha (`row_number() = 1`) de
      cada partição via `F.row_number().over(window)` + `filter("_row_number = 1")` +
      `drop("_row_number")`.
    - Adaptado para `silver_transactions_current.py`: `TRANSACTION_BUSINESS_KEY =
      ["client_id", "transaction_id"]` (mesma chave usada em
      `EXPECTATIONS["valid_business_key"]`) e `DEDUP_ORDER = ["ingestion_ts", "source_file"]`
      (mesmas colunas do exemplo original — já existiam no schema desta tabela).
    - `_write_batch` chama `_deduplicate_transactions(casted_df)` logo após `_cast_columns`
      e antes de `_split_batch` — dedup roda antes da validação de expectations, para que
      apenas a linha "vencedora" (mais recente) de cada chave de negócio seja avaliada e
      possa ir para `SILVER_TABLE`/`SILVER_REJECTED_TABLE`.
    - `_deduplicate_transactions` sozinha só deduplica **dentro do micro-batch** — não é
      suficiente, já que `SILVER_TABLE` não pode ter duplicatas em hipótese alguma (ao
      contrário da `BRONZE_TABLE`, onde duplicatas são aceitáveis). Duas execuções/micro-batches
      diferentes gravando a mesma chave de negócio em momentos distintos não seriam pegas por
      uma dedup só de batch.
    - Por isso `valid_df` deixou de ser gravado com `.write...mode("append")` e passou a ser
      gravado via `_upsert_valid()` (novo), um `MERGE INTO` (Delta) usando
      `DeltaTable.forName(spark, SILVER_TABLE)`: `whenMatchedUpdateAll` (só se a linha de
      origem for igual ou mais recente que a já gravada, comparando `struct(*DEDUP_ORDER)` do
      lado `source` vs. `target`) + `whenNotMatchedInsertAll`. Isso garante **zero duplicatas**
      na `SILVER_TABLE` de forma permanente (entre micro-batches, reinícios do stream,
      reprocessamentos etc.), não só dentro de um único batch.
    - `SILVER_REJECTED_TABLE` continua em `append` — quarentena não precisa de unicidade por
      chave de negócio.

13. **`batch_id` removido da `SILVER_REJECTED_TABLE`**
    - Coluna `batch_id BIGINT` retirada do DDL de `create_silver_rejected_table()`
      (`tables_silver_config.py`) e da gravação em `_write_batch` (`silver_transactions_current.py`)
      — `SILVER_REJECTED_TABLE` agora tem só `data`, `failure_reason`, `rejected_at`,
      `rejected_at_month`. O parâmetro `batch_id` de `_write_batch` continua existindo e sendo
      usado só no `logger.warning(...)`, não é mais persistido na tabela.

14. **`BRONZE_TABLE` passou a ser clusterizada por `transaction_conversion_month` em vez de
    `ingestion_date`, igual à `SILVER_TABLE`**
    - Nova coluna `transaction_conversion_month STRING` no DDL de `create_bronze_table()`
      (`tables_bronze_config.py`), derivada em `read_bronze_stream()` via
      `F.date_format(F.col("transaction.transactionConversionDate"), "yyyy-MM")` — direto sobre
      a coluna de origem ainda `STRING` (sem `.cast("date")` antes), já que o Spark casta
      implicitamente ao avaliar `date_format` e a bronze continua sem tipar nenhuma coluna de
      negócio (item 10 acima).
    - `CLUSTER BY` trocou de `(ingestion_date)` para `(transaction_conversion_month,
      transaction_id)` — mesmas colunas e mesma ordem da `SILVER_TABLE`. Motivo: consultas que
      filtram por período/chave de negócio (o padrão mais comum de acesso) passam a se beneficiar
      do Liquid Clustering em ambas as camadas, não só na silver.
    - Nota de terminologia: só existe `CLUSTER BY` aqui, não `PARTITIONED BY` — assim como a
      `SILVER_TABLE`, que também é só clusterizada. Delta não permite `CLUSTER BY` e
      `PARTITIONED BY` na mesma tabela; "particionada" no pedido que motivou essa mudança foi
      entendido como "organizada por" `transaction_conversion_month`, no sentido do Liquid
      Clustering, não um `PARTITIONED BY` Hive literal (esse padrão de partição Hive só é usado
      nas tabelas de rechaço, por `rejected_at_month`).

15. **`PRIMARY KEY` informativa em `SILVER_TABLE` (`client_id`, `transaction_id`), motivada pelos
    Performance Insights do Databricks**
    - Após rodar o pipeline, o Databricks reportou duas recomendações de performance: (1) Photon
      não acelera `bitmapaggregator` (agregador interno do Delta pra resolver Deletion Vectors —
      esperado sempre que um `MERGE` roda contra uma tabela com Deletion Vectors habilitados, sem
      correção via código); e (2) "Redundant Object Hash Aggregate on fileInScanId,
      deletionVectorId... considere aplicar constraints de chave primária/estrangeira" — o
      otimizador precisa verificar, a cada `MERGE`, que a chave de origem casa com no máximo uma
      linha de destino; sem uma PK declarada ele faz isso via agregação extra a cada execução.
    - A única tabela deste pipeline que roda `MERGE` é a `SILVER_TABLE`, via `_upsert_valid()`
      (`silver_transactions_current.py`) — a `BRONZE_TABLE` só faz `append` nativo, não deveria
      gerar esse padrão.
    - `client_id`/`transaction_id` viraram `STRING NOT NULL` (exigência do Unity Catalog pra
      colunas de chave primária) e uma `CONSTRAINT pk_silver_transactions_current PRIMARY KEY
      (client_id, transaction_id)` foi adicionada ao DDL de `create_silver_table()` — mesma dupla
      de colunas já usada em `TRANSACTION_BUSINESS_KEY` pra deduplicação/merge. Como no Unity
      Catalog PK/FK em Delta são só informativas (não enforced em runtime), isso não muda
      comportamento nem adiciona validação — é só o otimizador passando a confiar numa unicidade
      que o `_upsert_valid()` já garante na prática.
    - Como as outras colunas de `create_silver_table()`, isso só se aplica numa tabela criada do
      zero (`CREATE TABLE IF NOT EXISTS` é no-op se a tabela já existe) — numa `SILVER_TABLE` já
      existente seria preciso um `ALTER TABLE ... ADD CONSTRAINT` separado.
    - Confirmado em cluster real: sintaxe `CONSTRAINT ... PRIMARY KEY (...)` aceita e constraint
      aplicada com sucesso na `SILVER_TABLE`. Ver item 18 para o desdobramento — o insight (2)
      persistiu mesmo após a PK, e foi investigado/fechado separadamente.

16. **Removido recálculo redundante de `transaction_conversion_month` em `_cast_columns` (silver)**
    - `_cast_columns()` (`silver_transactions_current.py`) tinha um `.withColumn(
      "transaction_conversion_month", F.date_format(F.col("transaction_conversion_date"),
      "yyyy-MM"))` ao final, recalculando a partir de `transaction_conversion_date` já castado
      para `DATE`.
    - Constatado que isso é redundante: a bronze já calcula e persiste
      `transaction_conversion_month` (item 14 acima), e a silver lê a `BRONZE_TABLE` inteira via
      `spark.readStream.table(BRONZE_TABLE)` (`read_silver_stream()`) — a coluna já chega pronta
      no `batch_df` de `_write_batch`, com o mesmo valor (`yyyy-MM`), antes mesmo de
      `_cast_columns` rodar. O `.withColumn(...)` só recomputava o mesmo resultado em cima do
      `transaction_conversion_date` recém-castado.
    - `.withColumn("transaction_conversion_month", ...)` removido de `_cast_columns`; a coluna
      que já vem da bronze é usada sem alteração.

17. **`transaction_conversion_month` trocou de `STRING` para `DATE`, em bronze e silver**
    - DDL de `create_bronze_table()` (`tables_bronze_config.py`) e `create_silver_table()`
      (`tables_silver_config.py`): coluna `transaction_conversion_month` passou de `STRING` para
      `DATE` nas duas tabelas.
    - `read_bronze_stream()` (`bronze_transactions_current.py`) trocou a expressão de
      `F.date_format(F.col("transaction.transactionConversionDate"), "yyyy-MM")` (retornava
      `STRING`) para `F.to_date(F.date_format(F.col("transaction.transactionConversionDate"),
      "yyyy-MM"), "yyyy-MM")` — mantém o mesmo padrão `yyyy-MM` como formato intermediário
      (`date_format`), mas em seguida reconverte para `DATE` via `to_date` usando o mesmo padrão
      como máscara de parsing; como `yyyy-MM` não tem componente de dia, o resultado sempre cai
      no dia 1 do mês (ex.: `transactionConversionDate = "2024-03-15"` → `transaction_conversion_month
      = 2024-03-01`).
    - `silver_transactions_current.py` não precisou de nenhuma mudança: a silver já não recalcula
      mais essa coluna desde o item 16 — como ela vem pronta da `BRONZE_TABLE` via
      `spark.readStream.table(BRONZE_TABLE)`, o novo tipo `DATE` chega automaticamente junto com o
      resto do schema da bronze.
    - Motivo: ter a coluna como `DATE` (em vez de `STRING` no formato texto `yyyy-MM`) permite
      comparações/filtros por intervalo de data diretamente em SQL (`transaction_conversion_month
      BETWEEN ... AND ...`, `>`, `<`) sem precisar de parsing/cast explícito a cada consulta.

18. **Insight "Redundant Object Hash Aggregate on fileInScanId, deletionVectorId" persiste na
    silver mesmo após a `PRIMARY KEY` (item 15) — confirmado como limitação inerente ao `MERGE`,
    sem correção via código**
    - Após aplicar a `PRIMARY KEY` (`client_id`, `transaction_id`) na `SILVER_TABLE` (item 15), o
      Databricks Performance Insights continuou reportando a mesma recomendação: "Remove Object
      Hash Aggregate on fileInScanId, deletionVectorId or apply key constraints" — confirmado pelo
      usuário como originado na silver.
    - Investigada primeiro a hipótese de que a `SILVER_REJECTED_TABLE` fosse a origem (por não ter
      `PRIMARY KEY`) — descartada: `_write_batch` grava a `SILVER_REJECTED_TABLE` só via
      `.write...mode("append")` (nunca `MERGE`), e as colunas `fileInScanId`/`deletionVectorId` só
      aparecem no plano de execução de um `MERGE` contra tabela com Deletion Vectors habilitados
      (mecanismo de resolução de qual arquivo/linha do `target` casou com o `source`) — um
      `append` puro não gera esse padrão de plano, com ou sem PK na tabela.
    - Causa mais provável: a única operação deste pipeline que roda `MERGE` é `_upsert_valid()`
      (`silver_transactions_current.py`) contra a `SILVER_TABLE`. A `PRIMARY KEY` no `target`
      prova unicidade da própria tabela, mas o `source` do `MERGE` é o micro-batch em streaming —
      um `DataFrame` de runtime, não uma tabela com constraint declarada. O Delta ainda precisa
      verificar em runtime que nenhuma linha do `source` casa com mais de uma linha do `target`
      (para evitar o erro "multiple source rows matched"), mesmo já deduplicado via
      `_deduplicate_transactions` antes do merge — essa dedup é uma garantia de runtime, não algo
      que o otimizador consiga provar estaticamente a partir de um `DataFrame` sem constraint.
    - Considerada e descartada a opção de desabilitar Deletion Vectors na `SILVER_TABLE`
      (eliminaria o `deletionVectorId` do plano, mas piora reescrita de arquivos em
      update/delete — trade-off ruim para esse ganho).
    - Conclusão: tratado como limitação inerente ao padrão `MERGE` + Deletion Vectors deste
      pipeline (mesma natureza do ponto (1) já registrado no item 15, sobre o `bitmapaggregator`
      do Photon) — sem correção via código pendente. Fecha a dúvida que tinha ficado em aberto nas
      pendências do item 15.

## Diferenças em relação ao pipeline declarativo original

| Aspecto | Declarativo (`dlt/bronze`) | Imperativo — bronze (`imperativo/autoloader/bronze`) | Imperativo — silver (`imperativo/autoloader/silver`) |
|---|---|---|---|
| Definição da tabela | implícita via `@dp.table(...)` | `CREATE TABLE IF NOT EXISTS ...` explícito em `create_bronze_table()` | `CREATE TABLE IF NOT EXISTS ...` explícito em `create_silver_table()`/`create_silver_rejected_table()` |
| Leitura | Auto Loader + explode + select/cast num único passo | Auto Loader + explode + select **sem cast** (tudo `STRING`) | `spark.readStream.table(BRONZE_TABLE)` (Delta como streaming source, não Auto Loader) |
| Tipagem de negócio | cast já no `@dp.table` | só `transaction_conversion_date` (`DATE`, item 25) — resto `STRING` | `_cast_columns()` (`DECIMAL`/`DATE` nos demais campos; `transaction_conversion_date`/`transaction_conversion_month` já vêm prontas da bronze) |
| Escrita | gerenciada pela pipeline (framework decide) | sink nativo `.writeStream...toTable(BRONZE_TABLE)`, sem `foreachBatch` | `writeStream.foreachBatch(_write_batch)` com `checkpointLocation` próprio e `trigger(availableNow=True)` |
| Expectations | `@dp.expect` (warn-only, métricas no event log; duas condições) | nenhuma — tudo é gravado | `EXPECTATIONS` + `_split_batch()` (quarentena real: linhas inválidas vão para `SILVER_REJECTED_TABLE`, não para `SILVER_TABLE`; só `valid_business_key`, `no_rescued_data` removida) |
| SparkSession | implícita no runtime da pipeline | criada/obtida via `common/spark.py` (`get_spark()`) | idem |

Ver item 10 acima para o histórico da divisão bronze/silver, e `imperativo/PIPELINE.md` para o
desenho completo e atualizado de cada camada.

19. **Otimizações de performance aplicadas a partir de `PERFORMANCE_OPTIMIZATIONS.md`**
    - Relatório de auditoria de performance apontou 5 gargalos; 4 foram aplicados como propostos e
      1 (pruning no `MERGE`) foi ajustado por risco de correção de dados antes de aplicar.
    - **Auto-compactação Delta**: `TBLPROPERTIES` de `create_bronze_table()`
      (`tables_bronze_config.py`), `create_silver_table()` e `create_silver_rejected_table()`
      (`tables_silver_config.py`) ganharam `'delta.autoOptimize.optimizeWrite' = 'true'` e
      `'delta.autoOptimize.autoCompact' = 'true'`, para reduzir o Small Files Problem gerado por
      escritas streaming frequentes. Como `CREATE TABLE IF NOT EXISTS` é no-op em tabela já
      existente (mesma ressalva do item 15), isso só vale para tabelas criadas do zero — nas já
      existentes seria necessário `ALTER TABLE ... SET TBLPROPERTIES` à parte.
    - **`CLUSTER BY` da `BRONZE_TABLE`**: trocou de `(transaction_conversion_month,
      transaction_id)` para `(transaction_conversion_month, client_id)`
      (`tables_bronze_config.py`). Motivo: `transaction_id` é praticamente único por linha (alta
      cardinalidade sem padrão de busca direta por ele), gerando custo de manutenção do Liquid
      Clustering sem ganho real de pruning; `client_id` tem cardinalidade mais baixa e casa com o
      padrão de consulta mais comum ("transações de um cliente num período"). Diferente da
      `SILVER_TABLE` (que segue clusterizada só por `transaction_conversion_month`) — não é mais
      um requisito manter as duas camadas com a mesma chave de clustering, já que cada uma tem um
      padrão de leitura próprio.
    - **`transaction_conversion_month` na bronze — truncamento em vez de format+parse**
      (`bronze_transactions_current.py`, `read_bronze_stream()`): trocado
      `F.to_date(F.date_format(col, "yyyy-MM"), "yyyy-MM")` (formata a string em `yyyy-MM` e
      reconverte para `DATE`) por `F.trunc(F.to_date(col), "MM")` (converte direto para `DATE` e
      trunca pro primeiro dia do mês) — mesmo resultado (`transactionConversionDate = "2024-03-15"`
      → `2024-03-01`), evitando o round-trip string→date→string→date.
    - **`persist()`/`unpersist()` em `deduped_df` (silver, `_write_batch`)**: `deduped_df` (saída
      de `_deduplicate_transactions`, que faz `row_number() over (Window...)`) alimenta 3 ações no
      mesmo micro-batch (`rejected_df.count()`, escrita da `SILVER_REJECTED_TABLE` e o `MERGE` de
      `_upsert_valid`) — sem cache, o Spark recomputava o shuffle/ordenação da window a cada uma
      delas. `deduped_df.persist()` logo após a dedup, `deduped_df.unpersist()` em `finally`
      (garante liberação mesmo se `_upsert_valid`/escrita de rejeitados lançar exceção).
    - **Pruning no `MERGE` da silver (`_upsert_valid`) — ajustado em relação ao relatório**: o
      relatório propunha embutir `target.transaction_conversion_month = source.transaction_conversion_month`
      na condição de match, linha a linha. Risco identificado antes de aplicar: a `PRIMARY KEY` da
      `SILVER_TABLE` é só `(client_id, transaction_id)` — **sem** o mês —, o que indica que o
      design já admite que `transaction_conversion_date` possa ser corrigido/reemitido pela origem
      sem mudar a chave de negócio; nesse cenário, filtrar o `target` por igualdade de mês poderia
      não encontrar a linha antiga (que ficou em outro arquivo/mês), fazendo o `MERGE` executar um
      `INSERT` em vez de `UPDATE` e duplicar a business key. Decisão (confirmada com o usuário):
      aplicar uma variante menos restritiva — em vez de igualdade por linha, calcula os meses
      distintos presentes no próprio micro-batch (`valid_df.select("transaction_conversion_month").distinct().collect()`)
      e usa `target.transaction_conversion_month IN (DATE'...', DATE'...', ...)` antes da condição
      de chave de negócio; padrão oficial da Databricks para partition pruning em `MERGE`. Isso
      nunca poda um arquivo que a versão de igualdade estrita também podaria (é um superconjunto
      estritamente menos restritivo), mas mantém um risco residual: se a correção de data para a
      mesma business key chegar em um micro-batch **futuro** (outro conjunto de meses no batch),
      o pruning ainda pode não encontrar a linha antiga — aceito conscientemente em troca do ganho
      de performance (evita full scan da `SILVER_TABLE` a cada `MERGE`, que crescia conforme a
      tabela cresce). Guard para `valid_df` vazio: se não houver meses no batch, o filtro `IN` é
      omitido e a condição volta a ser só a chave de negócio (equivalente ao comportamento
      anterior).

20. **`transaction_conversion_month` trocou de `DATE` para `STRING` no formato `"yyyy-MM"`, em
    bronze e silver — reverte a decisão do item 17**
    - Pedido explícito do usuário: a coluna deve conter literalmente o texto `"yyyy-MM"` (ex.:
      `"2024-03"`), não uma data completa truncada pro dia 1 (`2024-03-01`, o que o item 17 e a
      otimização de performance do item 19 (`F.trunc(F.to_date(...), "MM")`) produziam).
    - `bronze/bronze_transactions_current.py` (`read_bronze_stream()`): expressão trocou de
      `F.trunc(F.to_date(F.col("transaction.transactionConversionDate")), "MM")` para
      `F.date_format(F.col("transaction.transactionConversionDate"), "yyyy-MM")` — retorna
      `STRING` diretamente, sem passar por `DATE`.
    - DDL de `create_bronze_table()` (`tables_bronze_config.py`) e `create_silver_table()`
      (`tables_silver_config.py`): `transaction_conversion_month` voltou de `DATE` para `STRING`.
    - `silver/silver_transactions_current.py` não precisou de mudança na derivação (item 16 já
      tinha removido o recálculo — a coluna só é lida da `BRONZE_TABLE`), mas o pruning do `MERGE`
      em `_upsert_valid()` (item 19) precisou de ajuste: os literais da cláusula `IN`, que eram
      `DATE'{month}'` (sintaxe de literal `DATE` do Spark SQL), viraram `'{month}'` (literal de
      string simples), já que a coluna deixou de ser `DATE`.
    - Mesma mudança replicada em `declarativa/lakeflow/` (ver item 16 do changelog declarativo) —
      os dois pipelines seguem consistentes entre si quanto ao formato desta coluna.
    - `imperativo/batch/` não tem essa coluna (bronze é só landing, sem nenhuma coluna derivada) —
      fora do escopo desta mudança.
    - Risco (mesmo padrão do item 15 do changelog declarativo): se `BRONZE_TABLE`/`SILVER_TABLE`
      já existirem fisicamente com `transaction_conversion_month` como `DATE` (de uma execução
      anterior ao item 20), a próxima escrita vai falhar por incompatibilidade de schema — a
      tabela precisaria ser recriada/ter a coluna migrada antes de rodar o pipeline com o schema
      novo (`CREATE TABLE IF NOT EXISTS` é no-op em tabela já existente, mesma ressalva dos itens
      15/19).

21. **8 otimizações aplicadas a partir de `ignore/PROMPT_OTIMIZACAO_PERFORMANCE.md`**
    - Guia com 8 tarefas de refatoração pontuais; todas aplicadas como propostas no arquivo (sem
      ajuste de escopo, diferente do que aconteceu no item 19).
    - **Tarefa 1 — dedup antes do cast (`silver_transactions_current.py`, `_write_batch`)**: ordem
      trocou de `_cast_columns` → `_deduplicate_transactions` para `_deduplicate_transactions` →
      `_cast_columns`. Seguro porque `_deduplicate_transactions` só usa a business key
      (`TRANSACTION_BUSINESS_KEY`) e `DEDUP_ORDER` (`ingestion_ts`, `source_file`) — nenhuma
      coluna tocada pelos casts de `DECIMAL`/`DATE` — então castar depois da dedup evita converter
      tipos em linhas que a window function já vai descartar. `deduped_df`/`casted_df` (nome da
      variável local também trocou para refletir a nova ordem) continua persistida/despersistida
      do mesmo jeito (item 19).
    - **Tarefa 2 — `isEmpty()` antes de `count()` para rejeitados** (`_write_batch`): trocado
      `rejected_count = rejected_df.count(); if rejected_count > 0:` por
      `if not rejected_df.isEmpty(): rejected_count = rejected_df.count(); ...` — `isEmpty()` só
      precisa achar 1 linha pra retornar (short-circuit), enquanto `count()` varre tudo; o
      `count()` só roda mesmo quando já se sabe que há pelo menos 1 rejeitada (necessário pro
      texto do log).
    - **Tarefa 3 — short-circuit em `_upsert_valid`**: `if valid_df.isEmpty(): return` logo no
      início — evita calcular os meses do batch (item 19) e abrir uma transação de `MERGE` no
      Delta quando não há nenhuma linha válida no micro-batch.
    - **Tarefa 4 — `.withColumn` encadeado → `.withColumns({...})`** (`_cast_columns`): as 9
      chamadas encadeadas de `.withColumn(...)` viraram um único `.withColumns({...})` com um
      dicionário — mesmo resultado, uma única projeção em vez de 9 encadeadas.
    - **Tarefa 5 — `date_format` → `substring` na bronze** (`bronze_transactions_current.py`,
      `read_bronze_stream()`): `F.date_format(col, "yyyy-MM")` trocado por
      `F.substring(col, 1, 7)`. Verificado antes de aplicar: os exemplos reais do payload
      (`examples/payload*.json`) confirmam `transactionConversionDate` sempre no formato
      `"yyyy-MM-dd"` (ex.: `"2023-01-07"`), então `substring(1, 7)` produz exatamente o mesmo
      resultado que `date_format` para linhas bem formadas, sem o custo de cast implícito
      string→timestamp e formatação. Diferença de comportamento para linhas malformadas: antes,
      uma string não parseável como data virava `NULL` (cast implícito do `date_format` falha
      silenciosamente); agora, `substring` sempre retorna os 7 primeiros caracteres da string,
      mesmo que não formem um `"yyyy-MM"` válido — coerente com a bronze não fazer nenhuma
      validação (item 10), mas quem consumir essa coluna direto da bronze deve estar ciente de que
      ela pode conter lixo não-validado em vez de `NULL` para datas malformadas.
    - **Tarefa 6 — Deletion Vectors explícitos**: `'delta.enableDeletionVectors' = 'true'`
      adicionado ao `TBLPROPERTIES` de `create_bronze_table()` (`tables_bronze_config.py`) e
      `create_silver_table()` (`tables_silver_config.py`, só a tabela principal — não a
      `_rejected`, que só faz `append`). Nota: o item 18 já tinha constatado que Deletion Vectors
      parecem estar ativos por padrão na `SILVER_TABLE` deste ambiente (plano do `MERGE` já
      mostrava `deletionVectorId`); esta tarefa só torna essa configuração explícita no DDL em vez
      de depender do default do ambiente. Na `BRONZE_TABLE` (só `append`, nunca `MERGE`/`UPDATE`),
      a propriedade fica sem efeito prático hoje, mas aplicada por consistência/robustez a
      mudanças futuras, como pedido no guia.
    - **Tarefa 7 — `cloudFiles.maxBytesPerTrigger`** (`common/config.py`,
      `CLOUDFILES_OPTIONS`): adicionado `"cloudFiles.maxBytesPerTrigger": "512m"`, junto do
      `cloudFiles.maxFilesPerTrigger` já existente (item 5) — Auto Loader respeita o primeiro
      limite que for atingido entre os dois, estabilizando o volume de bytes por trigger além do
      número de arquivos.
    - **Tarefa 8 — AQE e low-shuffle merge na `SparkSession`** (`common/spark.py`, `get_spark()`):
      adicionado `.config("spark.sql.adaptive.enabled", "true")` e
      `.config("spark.databricks.delta.merge.enableLowShuffle", "true")` no branch
      `if spark is None:` do builder. Ressalva: esse branch só executa quando **não** existe uma
      `SparkSession` ativa; em notebook/job do Databricks a sessão já vem ativa pelo runtime
      (`SparkSession.getActiveSession()` retorna não-`None`), então essas duas configs
      provavelmente nunca chegam a ser aplicadas na prática nesse ambiente — mesma limitação
      estrutural que já existia no resto de `get_spark()` antes desta tarefa. Além disso, o
      ambiente é serverless (item 3) e algumas configs do Spark podem ser restritas/ignoradas
      nesse modo de execução; não confirmado se `spark.databricks.delta.merge.enableLowShuffle`
      está entre elas.

22. **SQL injection no `IN (...)` de pruning do `MERGE` (`_upsert_valid`) — corrigido**
    - Bug introduzido como efeito colateral da Tarefa 5 do item 21: `transaction_conversion_month`
      passou a vir de `F.substring(transactionConversionDate, 1, 7)`, sem nenhuma validação de
      formato (ao contrário do `F.date_format` anterior, que devolvia `NULL` para strings não
      parseáveis como data — `NULL` era então filtrado antes de chegar em `batch_months`). Isso
      significa que `transaction_conversion_month` pode conter qualquer um dos 7 primeiros
      caracteres crus do campo de origem (JSON não confiável), inclusive aspas simples.
    - O pruning do `MERGE` (item 19) montava a cláusula `IN (...)` via f-string:
      `months_in_clause = ", ".join(f"'{month}'" for month in batch_months)`, interpolando esses
      valores direto numa string SQL passada pra `DeltaTable.merge(...)`. Uma
      `transactionConversionDate` malformada contendo `'` nos 7 primeiros caracteres (dado
      corrompido ou JSON malicioso) quebra o literal SQL — na melhor hipótese, gera um erro de
      parse; na pior, injeta uma condição arbitrária na cláusula `ON` do `MERGE`, podendo casar
      linhas erradas e corromper a `SILVER_TABLE` inteira.
    - Correção em `_upsert_valid()` (`silver_transactions_current.py`): a condição do `MERGE`
      deixou de ser montada como string SQL concatenada e passou a ser um `Column` (`F.expr(...)`
      pra parte fixa da business key + `F.col("target.transaction_conversion_month").isin(batch_months)`
      pra parte do pruning, combinados via `&`). `.isin(...)` passa `batch_months` como literais
      parametrizados pela API de DataFrame (não concatenação de string) — imune a esse tipo de
      caractere, independente do conteúdo de `transaction_conversion_month`. `DeltaTable.merge()`
      aceita tanto `str` quanto `Column` como condição, então a troca não muda mais nada no
      comportamento do `MERGE` além de fechar essa brecha.
    - `newer_condition` (usado só em `whenMatchedUpdateAll(condition=...)`) continua como string
      SQL — permanece seguro porque só referencia `DEDUP_ORDER` (`ingestion_ts`, `source_file`),
      nomes de coluna fixos no código, nunca valores vindos dos dados.
    - Fecha a pendência aberta no item 21 sobre `transaction_conversion_month` não ser mais
      validada como data — o risco de dado malformado continua existindo na coluna em si (ainda
      pode conter lixo em vez de `NULL`), mas deixou de ser explorável como injeção no `MERGE`.

23. **`persist()`/`unpersist()` (item 19) quebrou em produção — `NOT_SUPPORTED_WITH_SERVERLESS`;
    removido de `_write_batch`, versão anterior mantida comentada**
    - Erro real em produção ao rodar `silver_transactions_current.py`:
      `StreamingQueryException: [STREAM_FAILED] ... AnalysisException:
      [NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless compute`, no
      `casted_df.persist()` dentro de `_write_batch`. Confirma pra `persist()`/`cache()` a mesma
      limitação já documentada no item 3 pro acesso a `spark._jvm`: o client Spark Connect usado
      pelo compute serverless não suporta essa API (`cache()` é só um alias de
      `persist(MEMORY_AND_DISK)`, mesma restrição).
    - Sem `persist()` disponível, os `isEmpty()` de short-circuit adicionados nas Tarefas 2 e 3 do
      item 21 (`rejected_df.isEmpty()` em `_write_batch`, `valid_df.isEmpty()` em `_upsert_valid`)
      passaram a ser contraproducentes: cada `isEmpty()` força uma recomputação inteira do
      dedup+cast (window function + shuffle) só pra checar se há linhas, **antes** da ação
      "de verdade" (`count()`/escrita/`MERGE`) recomputar tudo de novo — mais recomputação do que
      a versão anterior ao item 21, não menos.
    - Correção em `silver_transactions_current.py`:
      - `_write_batch`: removidos `casted_df.persist()`/`try`/`finally: casted_df.unpersist()` e o
        `if not rejected_df.isEmpty():` — voltou a ser um único `rejected_count =
        rejected_df.count()` seguido de `if rejected_count > 0:`, igual ao comportamento anterior
        ao item 21 (Tarefa 2), agora a opção correta já que não há cache pra amortizar o
        `isEmpty()` extra.
      - `_upsert_valid`: `if valid_df.isEmpty(): return` (Tarefa 3) comentado — mesma razão.
      - A ordem dedup → cast (Tarefa 1) e o `.withColumns({...})` (Tarefa 4) do item 21
        **permanecem** ativos: nenhum dos dois depende de `persist()`, ambos continuam válidos.
      - A pedido do usuário, a versão anterior completa de `_write_batch` (com
        `persist()`/`unpersist()` e os dois `isEmpty()`) foi mantida **comentada** logo abaixo da
        função ativa, pra reaproveitar caso o pipeline volte a rodar num cluster clássico (onde
        `persist()` funciona normalmente).
    - Padrão a observar daqui pra frente: qualquer otimização baseada em cache/persist de
      DataFrame neste pipeline precisa ser validada contra compute serverless antes de virar
      padrão — mesma cautela que já valia pra acesso à JVM (item 3).

24. **`.withColumn` encadeado → `.withColumns({...})` em `read_bronze_stream()` (bronze)**
    - Mesmo padrão já aplicado em `_cast_columns` da silver (Tarefa 4 do item 21), agora replicado
      na bronze: as três chamadas encadeadas `.withColumn("source_file", ...)`,
      `.withColumn("ingestion_ts", ...)`, `.withColumn("ingestion_date", ...)` em
      `read_bronze_stream()` (`bronze_transactions_current.py`) viraram um único
      `.withColumns({"source_file": ..., "ingestion_ts": ..., "ingestion_date": ...})` — mesmo
      resultado, uma única projeção em vez de três encadeadas.

25. **`transaction_conversion_date` passou de `STRING` para `DATE` já na bronze, com ajustes na
    silver — replicado em `declarativa/lakeflow` e `imperativo/batch`**
    - Pedido explícito do usuário: `transaction_conversion_date` deixa de seguir o padrão
      "bronze = tudo `STRING`" do item 10 — passa a ser convertida para `DATE` já na leitura da
      bronze (`read_bronze_stream()`), não mais só na silver.
    - `bronze/bronze_transactions_current.py`:
      `F.col("transaction.transactionConversionDate").alias("transaction_conversion_date")` ganhou
      `.cast("date")` antes do `.alias(...)`. A derivação de `transaction_conversion_month` (item
      21, Tarefa 5 — `F.substring(col, 1, 7)`) foi trocada para
      `F.date_format(F.col("transaction.transactionConversionDate").cast("date"), "yyyy-MM")`,
      repetindo o `.cast("date")` inline em vez de reaproveitar uma variável entre as duas
      expressões (preferência explícita do usuário: cast dentro do `select()`, sem indireção fora
      dele). Fecha de vez o risco de dado sujo que ficou em aberto nos itens 21/22 (a Tarefa 5 do
      item 21 trocara `date_format` por `substring` justamente pra evitar que o cast implícito
      falhasse silenciosamente pra `NULL`; com `.cast("date")` de volta, strings malformadas
      voltam a virar `NULL` de forma controlada, e `transaction_conversion_month` nunca mais
      carrega lixo arbitrário do JSON de origem).
    - DDL de `create_bronze_table()` (`tables_bronze_config.py`): `transaction_conversion_date`
      mudou de `STRING` para `DATE`.
    - `silver/silver_transactions_current.py` (`_cast_columns()`): o `.cast("date")` de
      `transaction_conversion_date` foi removido — ficou redundante, já que a coluna chega tipada
      da `BRONZE_TABLE` via `spark.readStream.table(BRONZE_TABLE)` (mesmo raciocínio do item 16,
      que já tinha removido o recálculo redundante de `transaction_conversion_month`).
    - Mesma mudança replicada, pra manter os três pipelines consistentes:
      - `declarativa/lakeflow/bronze/bronze_transactions_current.py`: `.cast("date")` adicionado
        no `select()` da bronze; `table_bronze_tc_config.py` não tem DDL explícito de bronze
        (schema é implícito via `@dp.table`), então não precisou de alteração.
        `silver/silver_transactions_current.py` (`silver_transactions_current_casted`):
        `.withColumn("transaction_conversion_date", F.col("transaction_conversion_date").cast("date"))`
        removido pela mesma razão de redundância. Ver `declarativa/changelog.md` (item 17).
      - `imperativo/batch/bronze/bronze_transactions_current.py`: `.cast("date")` adicionado no
        `select()` de `_parse()`; DDL de `create_bronze_table()` (`tables_bronze_config.py`)
        trocou `transaction_conversion_date` de `STRING` para `DATE`. Este pipeline não tem
        camada silver nem coluna `transaction_conversion_month` (ver item 20 acima), então não há
        mais nenhum ajuste a fazer ali. Ver `imperativo/batch/README.md` (item 5).
    - Risco (mesmo padrão dos itens 15/19/20): se `BRONZE_TABLE` (autoloader ou batch) já existir
      fisicamente com `transaction_conversion_date` como `STRING`, a próxima escrita falha por
      incompatibilidade de schema — precisa recriar a tabela ou migrar a coluna antes de rodar com
      o schema novo.

26. **`EXPECTATIONS`, `TRANSACTION_BUSINESS_KEY`, `DEDUP_ORDER`, `REJECTED_PAYLOAD_EXCLUDED_COLUMNS`
    movidos de `silver_transactions_current.py` para `tables_silver_config.py`**
    - Essas quatro constantes eram definidas no nível do módulo em `silver_transactions_current.py`;
      passaram a ser definidas em `tables_silver_config.py` e importadas de lá, junto das funções
      que já vinham desse módulo (`create_silver_checkpoints_volume`, `create_silver_rejected_table`,
      `create_silver_schema`, `create_silver_table`).
    - Motivo: consolidar num só lugar por camada tudo que descreve o "shape" da tabela — schema/DDL
      (já estava em `tables_silver_config.py`) e agora também a business key, a ordem de dedup e a
      lista de expectations, que são conceitualmente parte da definição da tabela, não da lógica de
      streaming. Mesmo padrão já usado em `declarativa/lakeflow/silver/table_silver_tc_config.py`,
      que reúne essas mesmas constantes desde a criação do pipeline declarativo.
    - Sem mudança de comportamento — só de onde `silver_transactions_current.py` importa
      `EXPECTATIONS`/`TRANSACTION_BUSINESS_KEY`/`DEDUP_ORDER`/`REJECTED_PAYLOAD_EXCLUDED_COLUMNS`;
      os valores permanecem idênticos.

27. **Pruning do `MERGE` da silver trocou de `transaction_conversion_month` (mutável) para
    `client_id` (imutável); `CLUSTER BY` da `SILVER_TABLE` passou a `(transaction_conversion_month,
    client_id)` — fecha o risco de duplicidade dos itens 19/22**
    - Problema: o pruning do `MERGE` em `_upsert_valid()` (item 19) filtrava o `target` por
      `transaction_conversion_month IN (<meses do micro-batch>)`. Como a `PRIMARY KEY` da
      `SILVER_TABLE` é só `(client_id, transaction_id)` — sem o mês —, o design admite que a origem
      reemita a mesma business key com uma `transactionConversionDate` corrigida, mudando o mês.
      Quando isso acontecia num micro-batch cujo conjunto de meses **não incluía o mês antigo** da
      linha já gravada, o filtro `IN` deixava essa linha fora do escopo do `MERGE` →
      `whenNotMatchedInsertAll` disparava → **business key duplicada** na `SILVER_TABLE` (a PK é só
      informativa, não barra). Mesmo furo valia para linhas com `transaction_conversion_month`
      `NULL` (data de origem malformada, item 25) misturadas com linhas de mês não-nulo no mesmo
      batch: o `NULL` era descartado de `batch_months` e a linha-alvo com mês `NULL` nunca casava o
      `IN`.
    - Correção (`silver_transactions_current.py`, `_upsert_valid()`): `batch_months` /
      `F.col("target.transaction_conversion_month").isin(batch_months)` trocado por
      `batch_client_ids` / `F.col("target.client_id").isin(batch_client_ids)`. `client_id` faz
      parte da própria identidade da linha — para uma dada `(client_id, transaction_id)` ele nunca
      muda —, então a linha antiga está **sempre** no escopo do `MERGE`, independente de qualquer
      correção de data. Continua sendo um `Column` via `.isin(...)` (não string concatenada —
      mantém a proteção contra injeção do item 22). O guard para lista vazia (`if batch_client_ids:`)
      é o mesmo padrão de antes.
    - `create_silver_table()` (`tables_silver_config.py`): `CLUSTER BY (transaction_conversion_month)`
      → `CLUSTER BY (transaction_conversion_month, client_id)`. Motivo: o predicado
      `target.client_id IN (...)` só faz file skipping se a tabela estiver clusterizada por
      `client_id`; adicionar a coluna como segunda chave de clustering preserva o skipping por
      período (leitura analítica, `transaction_conversion_month` continua primeiro) e habilita o
      skipping por `client_id` no `MERGE`. A lista de `client_id` distintos por micro-batch é bem
      menor que a de `transaction_id` (vários lançamentos por cliente), então o `IN` fica enxuto.
    - Trade-off aceito: a poda por `client_id` continua sendo "melhor esforço" (não é garantia
      absoluta de evitar full scan) — se um micro-batch espalhar por um número grande de clientes
      distintos em relação à base total, ou se o Liquid Clustering ainda não tiver compactado os
      arquivos por `client_id` (pós-`ALTER TABLE ... CLUSTER BY` só reorganiza no próximo
      `OPTIMIZE`/auto-compact), o skipping degrada. Mas **nunca fica incorreta** (não duplica) e
      nunca lê mais do que um full scan já leria. Garantia absoluta de zero full scan exigiria
      clusterizar só por `client_id`, sacrificando a poda por período nas consultas analíticas —
      não feito.
    - Só vale para `SILVER_TABLE` criada do zero (`CREATE TABLE IF NOT EXISTS` é no-op em tabela
      existente, mesma ressalva dos itens 15/19/25) — numa tabela já existente é preciso
      `ALTER TABLE {SILVER_TABLE} CLUSTER BY (transaction_conversion_month, client_id)` seguido de
      `OPTIMIZE {SILVER_TABLE}` para reorganizar os arquivos já gravados.
    - Escopo: mudança específica do `imperativo/autoloader`. O `declarativa/lakeflow` usa
      `dp.create_auto_cdc_flow` (SCD1, o framework resolve o upsert — não há `MERGE` manual nem
      pruning por mês no código) e o `imperativo/batch` não tem camada silver — nenhum dos dois
      precisou de alteração.

## Pendências / pontos de atenção

- Item 23: se o pipeline migrar de compute serverless para um cluster clássico, considerar
  reativar a versão comentada de `_write_batch` (com `persist()`/`unpersist()` e os `isEmpty()` de
  short-circuit) — só compensa quando `persist()` está disponível.

- Item 21 (Tarefa 8): `spark.sql.adaptive.enabled`/`spark.databricks.delta.merge.enableLowShuffle`
  só são aplicados quando `get_spark()` cria uma `SparkSession` nova — no runtime normal do
  Databricks (sessão já ativa) esse branch não roda, e não está confirmado se essas configs têm
  efeito quando aplicadas de outra forma em compute serverless. Validar na Spark UI se
  `spark.databricks.delta.merge.enableLowShuffle` está de fato ativo durante o `MERGE` da silver;
  se não estiver, considerar configurar via cluster/serverless policy em vez do builder.
- Item 21 (Tarefa 5) / 22: `transaction_conversion_month` da bronze não é mais validada como data
  (pode conter lixo em vez de `NULL` se `transactionConversionDate` vier malformado) — o item 22
  já eliminou o risco de isso virar injeção no `MERGE` da silver, mas o dado sujo em si continua
  possível; se isso importar para algum consumidor direto da `BRONZE_TABLE`, considerar validação
  explícita.
- Item 20: se `BRONZE_TABLE`/`SILVER_TABLE` já existirem fisicamente com
  `transaction_conversion_month` como `DATE`, a escrita após essa mudança falha por
  incompatibilidade de schema — recriar a tabela (ou migrar a coluna) antes de rodar.
- Item 25: se `BRONZE_TABLE` (autoloader ou `imperativo/batch`) já existir fisicamente com
  `transaction_conversion_date` como `STRING`, a escrita após essa mudança falha por
  incompatibilidade de schema — recriar a tabela (ou migrar a coluna) antes de rodar.
- `TBLPROPERTIES` de auto-otimização (item 19) só entram em vigor em tabelas criadas do zero —
  aplicar `ALTER TABLE ... SET TBLPROPERTIES` manualmente nas tabelas já existentes do ambiente
  atual, se aplicável.
- ~~Risco residual do pruning no `MERGE` da silver (item 19): correção de
  `transaction_conversion_date` para uma business key já existente...~~ **Resolvido no item 27** — o
  pruning passou a ser por `client_id` (imutável), então a linha antiga está sempre no escopo do
  `MERGE` independente de mudança de mês. Continua valendo, se for preocupação do domínio, um job
  periódico de auditoria de duplicidade em `(client_id, transaction_id)` na `SILVER_TABLE` como
  defesa em profundidade.
- Item 27: se a `SILVER_TABLE` já existir fisicamente, aplicar
  `ALTER TABLE {SILVER_TABLE} CLUSTER BY (transaction_conversion_month, client_id)` + `OPTIMIZE`
  manualmente — `CREATE TABLE IF NOT EXISTS` não altera o clustering de uma tabela existente, e sem
  a reorganização por `OPTIMIZE` o pruning por `client_id` no `MERGE` fica fraco.
- Confirmar `BRONZE_TABLE`, `SILVER_TABLE` e `SILVER_REJECTED_TABLE` em `common/config.py`
  conforme o catalog/schema reais do ambiente.
- A `SILVER_REJECTED_TABLE` guarda a linha já tipada (pós-`_cast_columns`), não o payload bruto
  `STRING` como está na `BRONZE_TABLE` — decidir se isso é suficiente para reprocessamento ou se
  seria melhor capturar a linha crua da bronze diretamente.
- Nenhum processo de reprocessamento da `SILVER_REJECTED_TABLE` foi criado ainda (ex.: corrigir e
  reenviar linhas para a `SILVER_TABLE`) — atualmente é só um destino de quarentena para
  investigação manual.
- `foreachBatch` da silver usa `_write_batch` nomeada; se a lógica por micro-batch crescer ainda
  mais (ex.: `MERGE`/idempotência customizada), manter essa função como ponto de extensão.
- A `BRONZE_TABLE` agora nunca rejeita nada (linhas com `transaction_id`/`client_id`
  nulo/malformado entram normalmente) — se isso gerar volume relevante de "lixo" persistente na
  bronze, considerar alguma política de retenção/expurgo específica para ela.
