# Pipeline imperativo (batch) - bronze_transactions_current

Variante do pipeline bronze **sem Auto Loader** (`cloudFiles`) e sem Structured Streaming: leitura
em lote (`spark.read`), com validação estrutural do JSON (arquivo válido + todos os campos do
schema presentes) antes da escrita — o que falha vai para uma tabela de quarentena, o que passa é
gravado normalmente na bronze. Ver `imperativo/autoloader/README.md` para o pipeline original (com
Auto Loader) e o histórico completo de decisões que antecedem esta divisão.

> **Convenção:** este README deve ser atualizado a cada alteração relevante neste pipeline
> (`imperativo/batch/`) — inclusive mudanças pequenas de organização de arquivos, imports ou
> nomes. O pipeline `autoloader/` tem seu próprio README (`imperativo/autoloader/README.md`) e é
> atualizado separadamente. Ver também `imperativo/VALIDACAO_BRONZE.md` para o registro das
> opções avaliadas antes de decidir esta implementação (onde a validação deveria viver, critério
> de "JSON inválido", critério de "campo ausente").

## Estrutura

```
imperativo/batch/
├── common/
│   ├── config.py               # INPUT_PATH, BRONZE_TABLE, BRONZE_REJECTED_TABLE
│   └── spark.py                 # obtenção/criação da SparkSession (idêntico ao autoloader/)
└── bronze/
    ├── bronze_transactions_current.py   # pipeline (leitura, validação, split, write)
    └── tables_bronze_config.py          # schema UC, payload StructType e DDL das tabelas bronze/rechaço
```

## Por que existe, além do pipeline com Auto Loader

Surgiu de duas perguntas sobre o pipeline `autoloader/`: (1) dava pra validar se o JSON de entrada
é válido e se tem todas as colunas do schema, rejeitando o que falhar antes de chegar na bronze; e
(2) dava pra detectar **chave ausente** no JSON, não só valor nulo (`client_id: null` explícito vs.
`client_id` nunca enviado são coisas diferentes). A resposta pra (2) exige trocar o schema de
leitura de `StructType` (que colapsa as duas situações em `NULL`) para algo baseado em `Map`, que
preserva o conjunto de chaves do JSON de origem — ver `imperativo/VALIDACAO_BRONZE.md`, seção 4.

Implementar isso greenfield, sem tentar encaixar no sink nativo do `autoloader/` (que foi
deliberadamente simplificado para "aceitar tudo", sem `foreachBatch` — ver item 10 do
`imperativo/autoloader/README.md`), pareceu mais simples do que reverter aquele design. Por isso
este pipeline nasceu isolado, sem Auto Loader: os DataFrames de leitura/validação são construídos
diretamente com `spark.read`, sem streaming.

## O que foi feito

1. **Leitura em lote via `spark.read.text`, não Auto Loader**
   - `read_raw_files()`: `spark.read.option("wholetext", "true").option("pathGlobFilter",
     "*.json").text(INPUT_PATH)` — cada arquivo vira **uma linha** com o conteúdo bruto inteiro na
     coluna `value` (equivalente ao `multiline: true` do Auto Loader, mas via leitor de texto
     batch em vez de `cloudFiles`).
   - `source_file` vem de `F.input_file_name()` (equivalente batch do `_metadata.file_path`
     usado no streaming); `ingestion_ts`/`ingestion_date` calculados do mesmo jeito que no
     `autoloader/`.
   - Sem Auto Loader não há checkpoint/tracking de arquivos já processados — rodar o script duas
     vezes reprocessa tudo e duplica linhas na bronze e na rechaço. Nenhum controle de
     idempotência foi implementado ainda (ver "Pendências" abaixo).

2. **Dois `from_json` sobre o mesmo texto bruto, em vez de um `.schema()` só**
   - Motivo: um único `StructType` rígido não permite diferenciar "chave ausente no JSON" de
     "chave presente com valor `null`" — essa informação se perde no parse (ver
     `imperativo/VALIDACAO_BRONZE.md`, seção 1). Para recuperar isso, o mesmo texto (`value`) é
     parseado duas vezes, com schemas diferentes:
     - `STRUCT_PARSE_SCHEMA` (`SCHEMA` original + `_corrupt_record` + `_rescued_data`): dá os
       valores tipados das colunas de negócio, igual ao `autoloader/`.
       - `columnNameOfCorruptRecord: "_corrupt_record"` marca quando o documento inteiro não é
         JSON sintaticamente válido (nem chega a parsear) — é o critério de "JSON inválido".
       - `rescuedDataColumn: "_rescued_data"` replica o comportamento nativo do
         `cloudFiles.format=json` do `autoloader/`: campos que não estão no schema (colunas
         novas) caem em `_rescued_data` em vez de virar rejeição — **schema drift não é motivo de
         quarentena aqui**, só JSON inválido ou campo do schema faltando.
     - `FIELDS_PARSE_SCHEMA` (`data` tipado como `array<map<string,string>>` em vez de
       `array<transaction_schema>`): cada transação vira um `Map<String,String>`. Quando o Spark
       parseia um objeto JSON pra `Map`, toda chave que existe no JSON vira uma entrada (mesmo
       com valor `null`); chave ausente simplesmente não aparece no mapa. Isso permite
       `map_contains_key(mapa, 'clientId')` para checar presença de chave de verdade, sem se
       importar com o valor.
   - **Restrito ao campo pai** em campos aninhados: os 7 campos monetários (`transactionQuotaPrice`,
     `transactionValue`, `transactionGrossValue`, `incomeTax`, `financialTransactionTax`,
     `transactionExitFee`, `transactionNetValue`, cada um com `{amount, currency}`) são checados
     só pela existência da chave-pai no JSON — a checagem não desce em `amount`/`currency`
     internos. `TRANSACTION_FIELDS` é derivado dinamicamente de
     `SCHEMA["data"].dataType.elementType.fieldNames()` (as 14 chaves de topo de cada transação),
     em vez de hardcoded, pra nunca ficar dessincronizado do schema real.
   - Os dois arrays (`_typed.data` e `_fields.data`) vêm da mesma posição do mesmo array JSON de
     origem, então são zipados com `F.arrays_zip(...)` e explodidos juntos
     (`F.explode_outer("_transactions")`) — garante alinhamento por posição entre valor tipado e
     mapa de chaves sem precisar de join.

3. **Quarentena: `_valid_json` + `_missing_fields` → `_failure_reason`**
   - `_split(df)`: monta `_failure_reason` combinando `invalid_json` (quando `_corrupt_record`
     não é nulo) e `missing_fields: <lista>` (quando alguma chave obrigatória do schema não está
     presente no mapa) — linhas com os dois vazios vão pra `valid_df`, o resto pra `rejected_df`.
   - `valid_df` grava direto na `BRONZE_TABLE` (mesmo shape do `autoloader/`: 21 colunas de
     negócio + `source_file`/`ingestion_ts`/`ingestion_date`/`_rescued_data`, tudo `STRING`).
   - `rejected_df` serializa o payload em JSON (`F.to_json(F.struct(*payload_columns))`) e grava
     em `BRONZE_REJECTED_TABLE` (nova: `bronze_transactions_current_rechaco`), mesmo shape enxuto
     da `silver_transactions_current_rechaco` do `autoloader/`: `data`/`failure_reason`/
     `rejected_at`/`rejected_at_month`, particionada por `rejected_at_month`.

4. **`common/config.py` e `tables_bronze_config.py` enxutos, sem o que só faz sentido com Auto Loader**
   - `common/config.py` deste pipeline tem só `INPUT_PATH`, `BRONZE_TABLE`, `BRONZE_REJECTED_TABLE`
     — sem `CLOUDFILES_OPTIONS`/`cloudfiles_reader` (não há `cloudFiles` aqui) e sem checkpoint
     paths (não há streaming, não há `checkpointLocation`).
   - `tables_bronze_config.py` tem `create_bronze_schema()`, `transactions_current_schema()`,
     `create_bronze_table()` e `create_bronze_rejected_table()` — sem
     `create_bronze_checkpoints_volume()`/`BRONZE_CHECKPOINTS_VOLUME` (não existe volume de
     checkpoint pra criar sem streaming).
   - `run()` chama `create_bronze_table()` + `create_bronze_rejected_table()` e executa o
     pipeline uma vez (sem `start_*_stream()`/`awaitTermination()` — não é uma `StreamingQuery`).

5. **`transaction_conversion_date` passou de `STRING` para `DATE` já na bronze — replicado do
   `autoloader/` (item 25 do changelog dele), pedido explícito do usuário**
   - `bronze/bronze_transactions_current.py` (`_parse()`):
     `F.col("_item.transaction.transactionConversionDate").alias("transaction_conversion_date")`
     ganhou `.cast("date")` antes do `.alias(...)`.
   - DDL de `create_bronze_table()` (`tables_bronze_config.py`): `transaction_conversion_date`
     mudou de `STRING` para `DATE`.
   - Este pipeline não tem camada silver nem coluna `transaction_conversion_month` (bronze é só
     landing, item 4 acima), então não há mais nenhum ajuste a fazer.
   - Risco (mesmo padrão de qualquer mudança de schema neste pipeline): se `BRONZE_TABLE` já
     existir fisicamente com `transaction_conversion_date` como `STRING`, a próxima escrita falha
     por incompatibilidade de schema — recriar a tabela (ou migrar a coluna) antes de rodar.

6. **Item 27 do changelog do autoloader (pruning do `MERGE` da silver por `client_id`) — não se
   aplica a este pipeline**
   - O item 27 do `autoloader/` trocou a poda do `MERGE` da silver de `transaction_conversion_month`
     (mutável) para `client_id` (imutável), fechando um risco de business key duplicada quando a
     origem corrige a data de conversão, e alinhou o `CLUSTER BY` da `SILVER_TABLE` para
     `(transaction_conversion_month, client_id)`.
   - Este pipeline **não tem camada silver, nem `MERGE`, nem tabela deduplicada** — a bronze é só
     landing em `append` (item 4 acima), duplicatas são aceitáveis nela e a única idempotência
     discutida é por `source_file` (ver Pendências). Não há nada a replicar aqui.

## Diferenças em relação ao `autoloader/`

| Aspecto | Autoloader (`imperativo/autoloader/bronze`) | Batch (`imperativo/batch/bronze`) |
|---|---|---|
| Leitura | `cloudFiles` (Auto Loader) + Structured Streaming | `spark.read.text(..., wholetext=true)`, batch puro |
| Execução | `writeStream...trigger(availableNow=True)` + `awaitTermination()` | chamada direta de `run()`, sem streaming |
| Checkpoint / idempotência | `checkpointLocation` do Auto Loader rastreia arquivos já lidos | nenhum — rerun reprocessa tudo (pendência, ver abaixo) |
| Validação | nenhuma — tudo é gravado na bronze (item 10 do README do autoloader) | JSON inválido (`_corrupt_record`) e campo do schema ausente (`map_contains_key`) vão para `BRONZE_REJECTED_TABLE` |
| Campo novo / schema drift | cai em `_rescued_data`, não bloqueia escrita | idem — `rescuedDataColumn` replica o mesmo comportamento, não é motivo de rejeição |
| Tabela de rejeitados | não existe na bronze | `BRONZE_REJECTED_TABLE` (`bronze_transactions_current_rechaco`) |

## Pendências / pontos de atenção

- **Idempotência não implementada.** Sem Auto Loader não há controle de "arquivo já processado";
  rodar `run()` duas vezes duplica linhas em `BRONZE_TABLE` e `BRONZE_REJECTED_TABLE`. Direção
  discutida (não implementada): tabela de controle Delta com `source_file` já processado, filtrado
  via anti-join antes de parsear, atualizada só após a escrita ter sucesso.
- **`rescuedDataColumn` como opção de `from_json` não foi validada em cluster real.** É
  documentada pela Databricks, mas quem já foi testado neste projeto foi só a via nativa do Auto
  Loader (`cloudFiles.format=json`). Vale rodar num arquivo de amostra com campo extra pra
  confirmar que ele cai em `_rescued_data` e não em `missing_fields`/rejeição antes de rodar em
  produção.
- **Overwrite de arquivo no mesmo caminho não é tratado.** Ao contrário do `autoloader/`
  (`cloudFiles.allowOverwrites: true`), este pipeline não tem esse conceito — é batch puro sobre
  o que está em `INPUT_PATH` no momento da execução. Quando a idempotência por `source_file` for
  implementada, um arquivo reescrito no mesmo caminho não vai ser reprocessado a menos que o
  controle leve em conta hash/mtime, não só o nome do arquivo.
- Presença de chave (item 2 acima) cobre só os 14 campos de topo de cada transação — não desce
  nos campos monetários aninhados (`amount`/`currency`), por decisão explícita de escopo.
- Ausência das chaves de topo `data`/`meta` do documento inteiro (ex.: arquivo `{}` ou
  `{"foo": 1}`, JSON válido mas sem a chave `data`) não gera um motivo de `missing_fields`
  específico — vira uma linha com tudo nulo sem explicação clara do porquê. Conhecido, não
  tratado (fora do escopo pedido, que era por campo de transação).
