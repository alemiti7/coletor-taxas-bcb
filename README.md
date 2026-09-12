# 📈 Coletor de Taxas BCB (`coletor-taxas-bcb`)

Aplicação em **Go (Golang)** de alta performance voltada para a extração paralela de dados e séries temporais financeiras diretamente da API REST do **SGS (Sistema Gerenciador de Séries Temporais) do Banco Central do Brasil**, persistindo as informações de forma idempotente em banco de dados SQLite local.

---

## 🚀 Diferenciais da Arquitetura

* **Concorrência Nativa (Goroutines & Channels):** Requisições HTTP disparadas simultaneamente para múltiplas séries financeiras, otimizando o tempo total de resposta de ponta a ponta.
* **Go Puro (Zero CGO Dependency):** Utilização do driver `glebarez/go-sqlite`, eliminando a necessidade de compiladores C externos (como MinGW ou GCC) e facilitando o *build* e *cross-compilation* no Windows.
* **Persistência Idempotente (Upsert):** Modelagem de chave primária composta (`codigo_serie`, `data`) aliada a cláusulas `ON CONFLICT`, evitando dados duplicados em re-execuções.
* **Alta Velocidade em Escrita:** Processamento de inserção em lote (*batching*) encapsulado em transações SQL (`BEGIN` / `COMMIT`).

---

## 📊 Séries Coletadas

O colector busca nativamente indicadores econômicos essenciais:

| Código SGS | Indicador / Série | Descrição |
| :---: | :--- | :--- |
| **11** | SELIC (Diária) | Taxa média ponderada dos financiamentos diários apurados no Selic |
| **432** | SELIC (Meta) | Meta para a taxa Selic definida pelo COPOM |
| **4389** | CDI | Taxa DI diária acumulada |
| **433** | IPCA | Índice Nacional de Preços ao Consumidor Amplo |
| **189** | IGP-M | Índice Geral de Preços do Mercado (FGV) |
| **1** | Dólar PTAX (Venda) | Cotação diária do Dólar americano |
| **21619** | Euro PTAX (Venda) | Cotação diária do Euro |
| **24363** | IBC-Br | Prévia do Produto Interno Bruto calculada pelo BCB |

---

## 🛠️ Tecnologias Utilizadas

* **Linguagem:** Go (v1.27+)
* **Driver Banco de Dados:** `github.com/glebarez/go-sqlite` (Engine SQLite transpilada para Pure Go)
* **Biblioteca Nativa:** `net/http`, `encoding/json`, `sync`, `database/sql`

---

## 📁 Estrutura do Projeto

```text
meu-projeto-go/
├── main.go           # Pipeline principal (HTTP client, Goroutines e SQLite)
├── go.mod            # Gerenciador de módulos Go
├── go.sum            # Checksum das dependências
├── taxas_bcb.db      # Banco de dados SQLite gerado localmente (criado na 1ª execução)
└── README.md         # Documentação técnica do repositório 
 