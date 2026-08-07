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
    - Não testado em cluster real ainda — vale confirmar que a sintaxe `CONSTRAINT ... PRIMARY
      KEY (...)` é aceita na versão do Databricks Runtime/Unity Catalog do ambiente antes de
      rodar em produção.

## Diferenças em relação ao pipeline declarativo original

| Aspecto | Declarativo (`dlt/bronze`) | Imperativo — bronze (`imperativo/autoloader/bronze`) | Imperativo — silver (`imperativo/autoloader/silver`) |
|---|---|---|---|
| Definição da tabela | implícita via `@dp.table(...)` | `CREATE TABLE IF NOT EXISTS ...` explícito em `create_bronze_table()` | `CREATE TABLE IF NOT EXISTS ...` explícito em `create_silver_table()`/`create_silver_rejected_table()` |
| Leitura | Auto Loader + explode + select/cast num único passo | Auto Loader + explode + select **sem cast** (tudo `STRING`) | `spark.readStream.table(BRONZE_TABLE)` (Delta como streaming source, não Auto Loader) |
| Tipagem de negócio | cast já no `@dp.table` | nenhuma | `_cast_columns()` (`DECIMAL`/`DATE` + `transaction_conversion_month`) |
| Escrita | gerenciada pela pipeline (framework decide) | sink nativo `.writeStream...toTable(BRONZE_TABLE)`, sem `foreachBatch` | `writeStream.foreachBatch(_write_batch)` com `checkpointLocation` próprio e `trigger(availableNow=True)` |
| Expectations | `@dp.expect` (warn-only, métricas no event log; duas condições) | nenhuma — tudo é gravado | `EXPECTATIONS` + `_split_batch()` (quarentena real: linhas inválidas vão para `SILVER_REJECTED_TABLE`, não para `SILVER_TABLE`; só `valid_business_key`, `no_rescued_data` removida) |
| SparkSession | implícita no runtime da pipeline | criada/obtida via `common/spark.py` (`get_spark()`) | idem |

Ver item 10 acima para o histórico da divisão bronze/silver, e `imperativo/PIPELINE.md` para o
desenho completo e atualizado de cada camada.

## Pendências / pontos de atenção

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
