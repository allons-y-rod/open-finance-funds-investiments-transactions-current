# Validação estrutural de JSON na bronze — opções avaliadas

Este documento registra as opções discutidas para adicionar, em `imperativo/bronze/
bronze_transactions_current.py`, uma validação de (1) arquivo JSON malformado e (2) ausência de
colunas/chaves esperadas em `transactions_current_schema`, desviando as linhas que falharem para
uma tabela de quarentena (nos moldes de `silver_transactions_current_rechaco`). **Nada aqui foi
implementado ainda** — é um registro de decisão para quando a implementação for decidida. Ver
`imperativo/README.md` (item 10) para o histórico de por que a bronze hoje "aceita tudo", e
`imperativo/FOREACHBATCH.md` para o porquê da bronze não usar `foreachBatch` atualmente.

## 1. Comportamento atual (linha de base)

A leitura em `read_bronze_stream()` usa `.schema(SCHEMA)` explícito (um `StructType` rígido) +
`cloudFiles.schemaEvolutionMode: "rescue"`. Isso já garante que **o stream nunca quebra** por
conteúdo que não bate com o schema — nem por JSON sintaticamente inválido, nem por campos
faltando/sobrando. Hoje:

- Campo extra ou tipo incompatível → sobra em `_rescued_data`; resto do registro parseia normal.
- JSON sem sintaxe válida (arquivo inteiro, já que `multiline: true`) → colunas de topo (`data`,
  `meta`) voltam `null`; conteúdo bruto do arquivo cai em `_rescued_data`.
- `explode_outer("data")` (em vez de `explode`) já evita que a linha desapareça: um `data` nulo
  vira uma linha com `transaction` inteiro nulo, em vez de a linha sumir.
- **Não existe quarentena**: essas linhas (quase todas nulas) entram normalmente em
  `bronze_transactions_current`, junto com as válidas.
- **Chave ausente vs. chave presente e nula**: com `StructType`, o parser do Spark resolve as duas
  situações para o mesmo `NULL` do SQL — essa informação se perde no parse. Não existe
  `IS NOT NULL`/`get_json_object`/`json_tuple` que recupere isso depois, porque todos herdam essa
  mesma limitação do parser subjacente.

## 2. Decisão A — onde a validação deveria viver

| Opção | Prós | Contras |
|---|---|---|
| **A1. Na bronze, bloqueando a escrita** (o que foi pedido) | Quarentena o mais cedo possível — nada de "lixo" chega nem na bronze; mais fácil auditar arquivos de origem problemáticos antes de qualquer transformação. | Reverte a decisão do item 10 do README ("bronze = cópia fiel do que chegou, nunca rejeita"). Exige trazer `foreachBatch`/`_write_batch` de volta para a bronze (hoje usa sink nativo `.toTable()` — ver `FOREACHBATCH.md`), aumentando a complexidade de uma camada que foi deliberadamente simplificada. |
| **A2. Manter na silver** (onde já existe `EXPECTATIONS`/`_split_batch`) | Não mexe na bronze; reaproveita o padrão já existente (`_split_batch`, `SILVER_REJECTED_TABLE`, `foreachBatch` já presente). Mantém a separação de responsabilidades do medallion (bronze = landing, silver = validação). | A linha malformada já entrou na bronze antes de ser barrada — não resolve o pedido original de "não deixar entrar na bronze". Se o objetivo é auditar arquivos de origem (não linhas já processadas), fica mais indireto. |
| **A3. Job de auditoria separado, só leitura** (não bloqueia escrita) | Não altera o sink nativo da bronze nem sua simplicidade; roda em paralelo/depois, só loga ou grava um relatório de arquivos suspeitos. | Não atende ao pedido — a linha malformada continua entrando em `bronze_transactions_current` normalmente; quarentena vira só um espelho informativo, não um filtro real. |

**Observação:** o pedido original ("se falhar a validação, o dado não vai para a bronze") só é
atendido de fato pela opção **A1** — as opções A2/A3 deixam a linha malformada entrar na bronze de
qualquer forma.

## 3. Decisão B — critério para "JSON inválido"

| Opção | Prós | Contras |
|---|---|---|
| **B1. `_rescued_data IS NOT NULL`** | Já disponível sem nenhum parse adicional — é a coluna que o Auto Loader já popula hoje. Cobre tanto JSON sintaticamente quebrado quanto campo extra/tipo incompatível. | Também marca como "inválido" qualquer drift inofensivo de schema (ex.: provedor adiciona um campo novo não usado) — pode gerar falsos positivos se o schema evoluir com frequência. |
| **B2. Leitura dedicada com `columnNameOfCorruptRecord`** | Distingue explicitamente "documento não é JSON válido" de "campo extra/mismatch de tipo" (duas causas diferentes hoje misturadas em `_rescued_data`). | Exige uma segunda leitura/parse do mesmo arquivo (custo extra); mais código para manter dois caminhos de erro em paralelo. |

## 4. Decisão C — critério para "coluna ausente" (a pergunta mais recente)

O ponto central: um `StructType` rígido **não permite** diferenciar "chave ausente no JSON" de
"chave presente com valor `null`" — essa informação já se perde no parse. Para recuperar isso é
preciso trocar (ou complementar) o schema de leitura.

| Opção | Prós | Contras |
|---|---|---|
| **C1. Não diferenciar — checar só nulidade dos campos obrigatórios** (`client_id`, `investiment_id`, `transaction_id` — os 3 únicos `nullable=False` em `transactions_current_schema`) | Zero mudança de schema/leitura; reaproveita exatamente o padrão já usado em `EXPECTATIONS["valid_business_key"]` na silver. Simples de implementar e de entender. | Não responde à pergunta literal "o campo existe no JSON?" — um `client_id: null` explícito e um `client_id` ausente são tratados igual. Se a origem eventualmente distinguir os dois casos por algum motivo de negócio, essa opção não captura a diferença. |
| **C2. Parse paralelo com `MapType(StringType, StringType)`** — ler `data` também como `ArrayType(MapType(StringType, StringType))` (via `posexplode` para casar a mesma posição/transação do array), e checar `map_contains_key(mapa, 'clientId')`. Quando o Spark parseia um objeto JSON para `Map`, cada chave que existe no JSON vira uma entrada (mesmo com valor `null`); chave ausente simplesmente não aparece no mapa. | É a única forma correta de detectar presença literal de chave em Spark — resolve exatamente o que foi perguntado, incluindo o caso `campo: null` explícito vs. campo nunca enviado. | Só funciona de forma direta para os campos "achatados" no nível do objeto de transação (`clientId`, `investimentId`, `transactionId`, `type`, `transactionType`, `transactionTypeAdditionalInfo`, `transactionConversionDate`, `transactionQuotaQuantity`). Exige uma segunda leitura/parse em paralelo à leitura tipada atual (custo e complexidade extra); mais um schema pra manter sincronizado com `transactions_current_schema`. |
| **C3. C2 estendido aos campos monetários aninhados** (`transactionQuotaPrice`, `transactionValue`, `transactionGrossValue`, `incomeTax`, `financialTransactionTax`, `transactionExitFee`, `transactionNetValue` — cada um com `{amount, currency}`) | Cobertura completa de presença de chave para 100% do schema, não só os campos de topo. | `MapType(StringType, StringType)` não segura um valor aninhado de forma uniforme (o tipo de valor do mapa é único) — seria necessário um nível adicional de `Map` por campo monetário (ex.: `MapType(StringType, MapType(StringType, StringType))` não é diretamente aplicável porque o `data` já é heterogêneo linha a linha); a leitura fica sensivelmente mais pesada e verbosa para um ganho que hoje não tem uso conhecido (nenhuma expectation atual valida esses sub-campos). |

## 5. Recomendação (não implementada)

Combinação sugerida, caso a decisão A seja **A1**:

- **B1** (`_rescued_data IS NOT NULL`) para "JSON inválido/malformado" — já disponível, sem custo
  extra de leitura.
- **C1** (nulidade dos 3 campos obrigatórios) para "faltando campo", por ora — mesmo padrão já
  validado em produção na silver (`EXPECTATIONS`). Migrar para **C2** somente se surgir um caso de
  negócio real em que "campo `null` explícito" e "campo ausente" precisem de tratamento diferente
  — caso contrário é complexidade sem benefício observável.
- Nova tabela `bronze_transactions_current_rechaco`, mesmo shape enxuto de
  `silver_transactions_current_rechaco` (`data` JSON + `failure_reason`/`rejected_at`/
  `rejected_at_month`), e reintrodução de `foreachBatch`/`_write_batch` na bronze (revertendo
  parte do item 10 do README — documentar essa reversão explicitamente se for adiante).

## 6. Pendência

Nenhuma dessas opções foi implementada. Antes de codar, falta decidir explicitamente as questões A
e B/C acima — em especial se vale reverter o design "bronze nunca rejeita" (item 10 do
`README.md`) e se a diferenciação chave-ausente-vs-nula (C2/C3) tem algum caso de uso concreto que
justifique o custo, ou se C1 já é suficiente.
