# `foreachBatch` no pipeline imperativo — conceitos, motivação e alternativas

Este documento explica por que `imperativo/silver/silver_transactions_current.py` usa
`writeStream.foreachBatch(_write_batch)`, enquanto `imperativo/bronze/bronze_transactions_current.py`
não usa (usa o sink nativo `.toTable(...)`). Para o desenho completo de cada camada, ver
`imperativo/PIPELINE.md`; para o histórico de decisões, `imperativo/README.md`.

## 1. O que é `foreachBatch`

Structured Streaming processa dados em **micro-batches**: a cada `trigger`, o motor pega tudo que
chegou de novo na fonte desde o último micro-batch e materializa isso como um `DataFrame`
**limitado/estático** (não mais um `DataFrame` streaming "infinito"). Por padrão, esse DataFrame
de micro-batch é entregue direto a um *sink* (`.toTable(...)`, `.format("delta")`, `.foreach(...)`
etc.), com um único destino e um único `outputMode`.

`foreachBatch(func)` é um sink especial: em vez de escrever automaticamente para um destino fixo,
o motor chama `func(batch_df, batch_id)` uma vez por micro-batch, passando:

- `batch_df`: o `DataFrame` **estático** daquele micro-batch (não streaming) — todas as
  operações normais do `DataFrame`/`Dataset` API ficam disponíveis, sem as restrições que existem
  sobre `DataFrame`s streaming.
- `batch_id`: um identificador monotonicamente crescente do micro-batch, único por streaming
  query (reinicia se o checkpoint for apagado).

Dentro de `func`, o código roda como **Spark batch normal**: pode gravar em quantos destinos
quiser, com formatos/modos diferentes, chamar APIs que não existem para streaming (como
`DeltaTable.merge(...)`), fazer `.count()`, `.collect()`, joins arbitrários com tabelas estáticas
etc.

## 2. Por que usamos `foreachBatch` na silver (e não na bronze)

A bronze (`start_bronze_stream()`) usa o sink nativo:

```python
bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", BRONZE_CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .toTable(BRONZE_TABLE)
```

Isso é suficiente porque a bronze tem exatamente **um** destino, **um** modo de escrita
(`append`), e nenhuma lógica além de "gravar tudo que chegou" — não há motivo para pagar a
complexidade adicional de `foreachBatch`.

A silver (`start_silver_stream()`) usa `foreachBatch(_write_batch)` por três motivos concretos,
todos presentes em `_write_batch`:

1. **Dois destinos por micro-batch.** Cada micro-batch da silver precisa terminar em
   `SILVER_TABLE` (linhas válidas) **ou** `SILVER_REJECTED_TABLE` (linhas rejeitadas) — nunca nas
   duas. Um sink nativo de streaming escreve para um único destino; `foreachBatch` permite
   `rejected_df.write...saveAsTable(SILVER_REJECTED_TABLE)` e, na sequência, gravar `valid_df`
   separadamente.
2. **`MERGE` (upsert), não só `append`.** `SILVER_TABLE` não pode ter duplicatas por
   `TRANSACTION_BUSINESS_KEY` (ver `imperativo/README.md`, itens 12-13). Isso é feito com
   `DeltaTable.forName(spark, SILVER_TABLE).merge(...)` — uma API do **Delta Lake batch**, que só
   existe porque `batch_df` é estático dentro de `foreachBatch`. Não há um `outputMode`/sink
   nativo de streaming que faça `MERGE` condicional por chave de negócio contra uma tabela Delta
   arbitrária.
3. **Transformações que dependem do micro-batch inteiro.** `_deduplicate_transactions` usa uma
   `Window.partitionBy(*TRANSACTION_BUSINESS_KEY).orderBy(...)` com `row_number()` — uma `Window`
   sem agregação incremental, que o Structured Streaming não permite diretamente sobre um
   `DataFrame` streaming "infinito" sem `watermark`/`outputMode("complete")` (impraticável aqui).
   Sobre o `batch_df` estático de um micro-batch, essa mesma `Window` é uma operação batch comum,
   sem restrição nenhuma.

Em resumo: `foreachBatch` é usado porque a silver precisa de **múltiplos destinos + upsert +
lógica de DataFrame arbitrária por micro-batch** — nenhuma dessas três coisas é expressável com
um sink nativo de streaming.

## 3. Fundamentos do Structured Streaming por trás dessa escolha

- **Modelo micro-batch.** `trigger(availableNow=True)` (usado em ambas as camadas) processa tudo
  que está disponível na fonte em uma série de micro-batches e encerra sozinho quando não sobra
  mais nada — diferente de um trigger de intervalo fixo (`trigger(processingTime=...)`), que
  ficaria rodando indefinidamente.
- **`DataFrame` streaming vs. `DataFrame` batch.** Um `DataFrame` streaming representa uma
  computação sobre uma fonte de dados não-limitada; por isso o Spark restringe certas operações
  nele (múltiplas agregações encadeadas, `sort()` sem `outputMode("complete")`, alguns tipos de
  join, `Window` sem watermark para casos não-agregados, etc.). `foreachBatch` "escapa" dessa
  restrição: o `batch_df` recebido já é um recorte finito e materializado, então qualquer operação
  batch normal é válida sobre ele.
- **Checkpoint e reprocessamento (`at-least-once`).** O `checkpointLocation` rastreia até onde a
  fonte já foi lida (offsets de arquivo na bronze; versão da tabela Delta na silver). Se o
  processo cair no meio da execução de `_write_batch`, o Structured Streaming **reexecuta o mesmo
  micro-batch inteiro** na próxima execução — `foreachBatch` garante *at-least-once*, não
  *exactly-once*, para o que acontece dentro da função. Isso significa que o código dentro de
  `_write_batch` precisa ser **idempotente**: reprocessar o mesmo micro-batch duas vezes não pode
  corromper o estado final.
  - É exatamente por isso que `_upsert_valid` usa `MERGE` em vez de `append`: rodar o mesmo
    `MERGE` duas vezes com os mesmos dados produz o mesmo resultado (idempotente).
  - **Ponto de atenção:** a gravação em `SILVER_REJECTED_TABLE` continua em `append` — isso
    *não* é idempotente. Se `_write_batch` falhar depois de gravar as rejeitadas mas antes de
    terminar o `MERGE`, o reprocessamento do mesmo micro-batch grava as mesmas linhas rejeitadas
    de novo, duplicando-as na tabela de quarentena. Hoje isso é uma limitação aceita (quarentena é
    só para inspeção manual, não alimenta nada a jusante), mas vale registrar caso
    `SILVER_REJECTED_TABLE` passe a ser consumida por outro processo no futuro.

## 4. Alternativas ao `foreachBatch` (e por que não foram usadas aqui)

| Alternativa | Como funcionaria | Por que não se aplica (ou quando se aplicaria) |
|---|---|---|
| **Sink nativo** (`.toTable`/`.format(...).save(...)`) | Um destino só, `outputMode` fixo, sem lógica por micro-batch. | É o que a **bronze** já usa — funciona porque ela tem um único destino e `append` puro. Não serve para a silver (dois destinos + upsert). |
| **Duas streaming queries independentes** | Duas queries separadas lendo `BRONZE_TABLE`, cada uma com seu próprio `readStream`/`writeStream` e checkpoint — uma calculando/gravando só as válidas, outra só as rejeitadas. | Duplicaria a leitura/transformação da bronze (cast, split) em dois processos, com dois checkpoints para manter sincronizados; e a query da tabela válida ainda precisaria de `foreachBatch` para fazer o `MERGE` (Structured Streaming não tem sink nativo de upsert). Resolve o problema de "múltiplos destinos" mas não o de "upsert". |
| **Lakeflow Declarative Pipelines / DLT com `APPLY CHANGES INTO`** | Voltar ao modelo declarativo (`dlt/`) e usar `create_auto_cdc_flow`/`APPLY CHANGES INTO` — a forma declarativa nativa de fazer upsert por chave, com `SEQUENCE BY` no lugar do `DEDUP_ORDER` manual. | É a alternativa mais "correta" do ponto de vista de manutenção (upsert declarado, não código de `MERGE` manual) — mas contradiz o próprio objetivo deste diretório (`imperativo/`), que é reescrever o pipeline de forma imperativa/explícita. Vale como referência para uma eventual migração de volta ao declarativo. |
| **`ForeachWriter`** (`.foreach(writer)`, processamento linha a linha) | Implementar `open`/`process`/`close` e escrever linha por linha (tipicamente usado para sistemas sem *Data Source* Spark nativo, ex.: chamadas a uma API externa por linha). | Não se aplica a escrita em tabelas Delta — processar linha a linha (em vez de em lote) seria muito mais lento para o volume gravado aqui, e não dá nenhum ganho, já que Delta já tem suporte a escrita/`MERGE` em lote. |
| **Job batch periódico (sem streaming)** | Trocar `readStream`/`writeStream` por um job Spark batch comum, agendado (ex.: a cada N minutos), lendo com `spark.read.table(BRONZE_TABLE)` e fazendo o mesmo cast/dedup/split/MERGE "na mão", sem checkpoint de streaming. | Perderia o rastreamento incremental automático (teria que implementar controle de "o que já foi processado" manualmente) e a garantia de processar exatamente o delta desde a última execução — o que o Structured Streaming já resolve de graça via checkpoint. Só faria sentido se o requisito de latência/frequência fosse muito baixo e se quisesse simplificar a operação abrindo mão do streaming. |

## 5. Onde isso aparece no código

- `imperativo/bronze/bronze_transactions_current.py` → `start_bronze_stream()`: sink nativo, sem
  `foreachBatch`.
- `imperativo/silver/silver_transactions_current.py` → `start_silver_stream()`:
  `.foreachBatch(_write_batch)`; `_write_batch()` orquestra cast → dedup → split → grava
  rejeitadas (`append`) → grava válidas (`_upsert_valid`, `MERGE`).

Ver `imperativo/PIPELINE.md` (§6.2) para a descrição linha a linha de cada função, e
`imperativo/README.md` (itens 7, 12 e 13) para o histórico de como a silver chegou a esse desenho.
