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
    ├── payload.json                 # payload válido
    ├── payload_error.json           # payload válido estruturalmente, com chave de negócio vazia (clientId "")
    └── payload_mal_formatado.json   # JSON malformado (sintaxe inválida)
```

## Payload de origem

Cada arquivo de landing é um JSON com um array `data` de transações (`clientId`,
`investimentId`, `transactionId`, `transactionConversionDate`, valores monetários aninhados
como `{amount, currency}`, etc.) — ver `examples/payload.json` para o shape completo.
