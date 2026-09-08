# open-finance-funds-investiments-transactions-current

Pipelines de dados para o recurso Open Finance **Funds Investments / Transactions Current**:
ingestão de arquivos JSON de landing até tabelas Delta em bronze e silver no Unity Catalog, com
tipagem de negócio, deduplicação por chave de negócio e quarentena de linhas inválidas.

Este repositório reúne duas implementações do mesmo pipeline, usando técnicas diferentes:

- **[`declarativa/`](declarativa/README.md)** — Lakeflow Declarative Pipelines (`pyspark.pipelines`,
  `@dp.table`/`@dp.view`/`@dp.expect`), com upsert incremental via `create_auto_cdc_flow`.
- **[`imperativo/`](imperativo/README.md)** — Auto Loader + Structured Streaming puro
  (`writeStream`/`foreachBatch`/`MERGE` explícitos no código, sem framework declarativo).

Cada pasta tem seu próprio README com a estrutura de arquivos e o comportamento atual de cada
camada (bronze/silver).

## Estrutura

```
.
├── declarativa/            # implementação declarativa (Lakeflow Declarative Pipelines)
├── imperativo/             # implementação imperativa (Auto Loader + Structured Streaming)
└── examples/                # payloads JSON de exemplo do recurso Open Finance
    ├── payload.json                         # payload válido
    ├── payload_error.json                   # payload válido estruturalmente, com chave de negócio vazia (clientId "")
    ├── payload_mal_formatado.json           # JSON malformado (sintaxe inválida)
    ├── payload_multiplos_registros.json     # múltiplas transações no mesmo array `data` (ENTRADA/SAIDA, clientes distintos)
    ├── payload_vazio.json                   # array `data` vazio
    ├── payload_transaction_id_vazio.json     # payload válido estruturalmente, com chave de negócio vazia (transactionId "")
    ├── payload_campo_desconhecido.json       # payload válido com campo extra não mapeado no schema (`settlementDate`)
    ├── payload_client_id_null.json           # chave de negócio nula em JSON (clientId: null, não string vazia)
    ├── payload_transaction_id_null.json      # chave de negócio nula em JSON (transactionId: null, não string vazia)
    ├── payload_data_ausente.json             # chave `data` ausente no documento inteiro (só `links`/`meta`) — explode_outer gera uma linha toda nula
    ├── payload_data_conversao_invalida.json  # transactionConversionDate com string não parseável como data
    ├── payload_valor_monetario_invalido.json # transactionValue.amount com string não numérica
    └── payload_chave_duplicada.json          # mesma business key de payload.json (client_id/transaction_id), data/valores diferentes
```

## Payload de origem

Cada arquivo de landing é um JSON com um array `data` de transações (`clientId`,
`investimentId`, `transactionId`, `transactionConversionDate`, valores monetários aninhados
como `{amount, currency}`, etc.) — ver `examples/payload.json` para o shape completo.
