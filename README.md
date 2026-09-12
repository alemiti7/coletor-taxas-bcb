# 📈 Coletor de Taxas BCB (`coletor-taxas-bcb`)

![Go Version](https://img.shields.io/badge/Go-1.27%2B-00ADD8?style=for-the-badge&logo=go)
![Database](https://img.shields.io/badge/SQLite-Pure_Go-003B57?style=for-the-badge&logo=sqlite)
![CGO](https://img.shields.io/badge/CGO__ENABLED-0-lightgrey?style=for-the-badge)
![License](https://img.shields.io/badge/License-GPL--3.0-blue?style=for-the-badge)

Aplicação concorrente escrita em **Go** para extração paralela de séries temporais da API REST do **SGS (Sistema Gerenciador de Séries Temporais) do Banco Central do Brasil**, com persistência **idempotente e incremental** em um banco SQLite local — sem CGO, sem instaladores, um único binário.

**Para quem é útil:** equipes de compras e contratos públicos que precisam de índices oficiais (IPCA, IGP-M, INPC) para reajustes e atualizações monetárias, analistas financeiros e desenvolvedores que queiram uma base local e consultável de indicadores macroeconômicos.

---

## 📑 Sumário

- [Como Funciona](#-como-funciona)
- [Diferenciais Técnicos](#-diferenciais-técnicos)
- [Séries Coletadas](#-séries-coletadas)
- [Pré-requisitos](#-pré-requisitos)
- [Instalação e Execução](#-instalação-e-execução)
- [Opções de Linha de Comando](#-opções-de-linha-de-comando)
- [Schema do Banco de Dados](#-schema-do-banco-de-dados-taxas_bcbdb)
- [Consultando os Dados](#-consultando-os-dados)
- [Adicionando ou Removendo Séries](#-adicionando-ou-removendo-séries)
- [Problemas Conhecidos](#-problemas-conhecidos)
- [Estrutura do Projeto](#-estrutura-do-projeto)
- [Roadmap](#-roadmap)
- [Licença](#-licença)

---

## ⚙️ Como Funciona

```text
               ┌────────────────────────┐
               │  API REST (BCB / SGS)  │
               └───────────┬────────────┘
                           │ HTTP GET concorrente (JSON) — até N simultâneas
         ┌─────────────────┼─────────────────┐
         ▼                 ▼                 ▼
  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
  │ Goroutine 1 │   │ Goroutine 2 │   │ Goroutine N │   ← semáforo (-workers)
  │ (Série 11)  │   │ (Série 433) │   │ (Série 1)   │   ← retry com backoff
  └──────┬──────┘   └──────┬──────┘   └──────┬──────┘
         │                 │                 │
         └─────────────────┼─────────────────┘
                           ▼
                 ┌──────────────────┐
                 │ Buffered Channel │  ← sync.WaitGroup fecha o canal ao término
                 └─────────┬────────┘
                           ▼
             ┌───────────────────────────┐
             │ SQLite (Pure Go / CGO=0)  │
             │  WAL + Transação + Upsert │
             └───────────────────────────┘
```

1. O banco é aberto e a tabela criada **antes** de qualquer requisição.
2. Para cada série do catálogo, o coletor determina o ponto de partida: a flag `-inicio`, ou a última data já gravada (modo incremental), ou o histórico completo.
3. Séries **diárias** são fatiadas em janelas de 10 anos — limite da API do SGS por requisição.
4. Uma goroutine por série faz as requisições (limitadas por um semáforo), com até 4 tentativas e *backoff* exponencial em falhas de rede, HTTP 429/5xx ou respostas não-JSON.
5. Cada resposta é sanitizada (data validada, valor convertido para `float64`) e enviada a um canal bufferizado.
6. Um único consumidor grava cada série em uma transação própria com `INSERT ... ON CONFLICT DO UPDATE`.
7. Ao final, o programa registra o total gravado e retorna **código de saída 1** se alguma série falhou.

---

## 🚀 Diferenciais Técnicos

- **Coleta incremental:** cada execução continua da última data gravada por série. A primeira carga leva ~2 min; as seguintes, segundos.
- **Paginação automática para séries diárias:** contorna o limite de 10 anos por requisição da API sem intervenção manual.
- **Concorrência controlada:** uma goroutine por série, sincronizadas por `sync.WaitGroup`, canal bufferizado e semáforo configurável.
- **Resiliência:** `context` + timeout por requisição, *retry* com *backoff* e detecção de páginas HTML devolvidas com HTTP 200 (comportamento ocasional do SGS).
- **Go puro (`CGO_ENABLED=0`):** o driver [`glebarez/go-sqlite`](https://github.com/glebarez/go-sqlite) dispensa GCC/MinGW e permite *cross-compilation* trivial.
- **Idempotência por *upsert*:** chave primária composta (`codigo_serie`, `data`) + `ON CONFLICT DO UPDATE`. Re-executar nunca duplica e ainda absorve revisões retroativas do BCB.
- **Datas legíveis e ordenáveis:** `data` em `dd/mm/aaaa` para leitura, `data_iso` (coluna gerada pelo SQLite) em `aaaa-mm-dd` para ordenação e filtros.
- **Código de saída significativo:** apto a rodar em cron / Agendador de Tarefas com alerta em falha.

---

## 📊 Séries Coletadas

As séries são definidas no slice `catalogo` em `main.go`. O nome é gravado na coluna `nome_serie`.

### Juros e Rendimentos

| Código SGS | Nome no banco | Descrição | Periodicidade |
|:---:|:---|:---|:---:|
| **11** | `SELIC_DIARIA` | Taxa média ponderada apurada no Selic | Diária |
| **432** | `SELIC_META` | Meta definida pelo COPOM (% a.a.) | Diária |
| **4389** | `CDI` | Taxa DI acumulada | Diária |
| **196** | `POUPANCA` | Rendimento da caderneta de poupança | Mensal |

### Inflação

| Código SGS | Nome no banco | Descrição | Periodicidade |
|:---:|:---|:---|:---:|
| **433** | `IPCA` | Índice Nacional de Preços ao Consumidor Amplo | Mensal |
| **10844** | `IPCA_EX` | IPCA – núcleo por exclusão | Mensal |
| **188** | `INPC` | Índice Nacional de Preços ao Consumidor | Mensal |
| **189** | `IGP_M` | Índice Geral de Preços do Mercado (FGV) | Mensal |
| **190** | `IGP_DI` | Índice Geral de Preços – Disponibilidade Interna (FGV) | Mensal |

### Câmbio (PTAX)

| Código SGS | Nome no banco | Descrição | Periodicidade |
|:---:|:---|:---|:---:|
| **1** | `DOLAR_VENDA` | Cotação oficial de venda (USD/BRL) | Diária |
| **10813** | `DOLAR_COMPRA` | Cotação oficial de compra (USD/BRL) | Diária |
| **21619** | `EURO_VENDA` | Cotação oficial de venda (EUR/BRL) | Diária |
| **21618** | `EURO_COMPRA` | Cotação oficial de compra (EUR/BRL) | Diária ⚠️ |

### Atividade, Emprego, Setor Externo e Fiscal

| Código SGS | Nome no banco | Descrição | Periodicidade |
|:---:|:---|:---|:---:|
| **24363** | `IBC_BR` | Prévia mensal do PIB calculada pelo BCB | Mensal |
| **28763** | `CAGED_SALDO` | Saldo líquido de postos formais de trabalho | Mensal |
| **24369** | `DESEMPREGO_PNADC` | Taxa de desocupação – PNAD Contínua (IBGE) | Mensal |
| **3546** | `RESERVAS_INTERNACIONAIS` | Reservas internacionais (milhões de USD) | Mensal |
| **22701** | `BALANCA_COMERCIAL` | Saldo da balança comercial (FOB, USD) | Mensal |
| **4649** | `DIVIDA_PUBLICA_PIB` | Dívida consolidada do setor público / PIB | Mensal |
| **4642** | `RESULTADO_PRIMARIO` | NFSP – resultado primário | Mensal |

> Séries diárias sem histórico no banco são carregadas com os **últimos 10 anos** por padrão; use `-inicio` para outro ponto de partida.
>
> 💡 Metadados de qualquer série (unidade, fonte, início, periodicidade) podem ser consultados em `https://www3.bcb.gov.br/sgspub/`.

---

## 📋 Pré-requisitos

- **Go 1.27 ou superior** — [go.dev/dl](https://go.dev/dl/)
- Acesso à internet para `api.bcb.gov.br`

```bash
go version
```

---

## 💻 Instalação e Execução

### 1. Clonar o repositório

```bash
git clone https://github.com/alemiti7/coletor-taxas-bcb.git
cd coletor-taxas-bcb
```

### 2. Sincronizar dependências

```bash
go mod tidy
```

### 3. Executar

```bash
go run main.go
```

O arquivo `taxas_bcb.db` é criado no diretório atual na primeira execução. Saída típica de uma execução incremental:

```text
19:08:55 Iniciando coleta de 20 séries (até 5 simultâneas)...
19:08:56 ✔ IPCA                     (  433)      1 registros
19:08:56 ✔ SELIC_DIARIA             (   11)      1 registros
19:08:56 ✔ DOLAR_VENDA              (    1)      1 registros
...
19:08:57 Concluído em 1.9s: 20 registros gravados, 0 série(s) com falha.
```

### 4. Compilar o executável nativo

**Windows (PowerShell):**

```powershell
$env:CGO_ENABLED=0; go build -ldflags="-s -w" -o coletor-taxas.exe main.go
.\coletor-taxas.exe
```

**Linux / macOS:**

```bash
CGO_ENABLED=0 go build -ldflags="-s -w" -o coletor-taxas main.go
./coletor-taxas
```

**Cross-compilation** (gerar binário Windows a partir de macOS/Linux):

```bash
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -ldflags="-s -w" -o coletor-taxas.exe main.go
```

### 5. Agendar execuções (opcional)

O código de saída permite monitorar falhas em agendadores:

- **Windows:** Agendador de Tarefas → ação `coletor-taxas.exe`, "Iniciar em" = pasta do projeto.
- **Linux/macOS (cron, dias úteis às 19h):** `0 19 * * 1-5 cd /caminho/projeto && ./coletor-taxas >> coletor.log 2>&1`

---

## 🎛️ Opções de Linha de Comando

| Flag | Padrão | Descrição |
|:---|:---|:---|
| `-db` | `taxas_bcb.db` | Caminho do banco SQLite |
| `-inicio` | *(vazio)* | Data inicial `dd/mm/aaaa`. Vazio = incremental, ou histórico completo na primeira carga |
| `-fim` | hoje | Data final `dd/mm/aaaa` |
| `-incremental` | `true` | Continuar a partir da última data gravada de cada série |
| `-workers` | `5` | Máximo de requisições simultâneas |
| `-timeout` | `60s` | Timeout por requisição HTTP |

Exemplos:

```bash
go run main.go                                      # incremental (uso diário)
go run main.go -incremental=false                   # recarrega o histórico completo
go run main.go -inicio 01/01/2020 -fim 31/12/2020   # intervalo específico
go run main.go -db /dados/taxas.db -workers 3       # banco em outro local, menos paralelismo
```

---

## 💾 Schema do Banco de Dados (`taxas_bcb.db`)

```sql
CREATE TABLE IF NOT EXISTS taxas (
    codigo_serie INTEGER NOT NULL,
    nome_serie   TEXT    NOT NULL,
    data         TEXT    NOT NULL,   -- dd/mm/aaaa (leitura)
    valor        REAL    NOT NULL,
    data_iso     TEXT GENERATED ALWAYS AS (
        substr(data, 7, 4) || '-' || substr(data, 4, 2) || '-' || substr(data, 1, 2)
    ) STORED,                        -- aaaa-mm-dd (ordenação e filtros)
    PRIMARY KEY (codigo_serie, data)
);

CREATE INDEX IF NOT EXISTS idx_taxas_serie_data_iso ON taxas (codigo_serie, data_iso);
```

`data_iso` é mantida automaticamente pelo SQLite a partir de `data`; nunca precisa ser preenchida manualmente. O banco opera em modo **WAL**, por isso podem aparecer arquivos `taxas_bcb.db-wal` e `-shm` durante a execução — são consolidados ao encerrar.

---

## 🔍 Consultando os Dados

Qualquer cliente SQLite funciona: `sqlite3` CLI, DBeaver, DB Browser for SQLite, extensão SQLite Viewer do VS Code, Python, LibreOffice Base. Regra prática: **exiba `data`, ordene e filtre por `data_iso`**.

**Último valor de cada série:**

```sql
SELECT t.codigo_serie, t.nome_serie, t.data, t.valor
FROM taxas t
JOIN (SELECT codigo_serie, MAX(data_iso) AS ult FROM taxas GROUP BY codigo_serie) u
  ON u.codigo_serie = t.codigo_serie AND u.ult = t.data_iso
ORDER BY t.nome_serie;
```

**IPCA acumulado nos últimos 12 meses (reajuste contratual):**

```sql
SELECT ROUND((EXP(SUM(LN(1 + valor / 100.0))) - 1) * 100, 4) AS ipca_12m
FROM (
    SELECT valor FROM taxas
    WHERE codigo_serie = 433
    ORDER BY data_iso DESC
    LIMIT 12
);
```

**Dólar PTAX de venda em um intervalo:**

```sql
SELECT data, valor
FROM taxas
WHERE codigo_serie = 1
  AND data_iso BETWEEN '2026-01-01' AND '2026-06-30'
ORDER BY data_iso;
```

**Exportar uma série para CSV:**

```bash
sqlite3 -header -csv taxas_bcb.db "SELECT data, valor FROM taxas WHERE codigo_serie = 189 ORDER BY data_iso;" > igpm.csv
```

---

## ➕ Adicionando ou Removendo Séries

Edite o slice `catalogo` em `main.go`. O terceiro campo indica se a série é **diária** (ativa a paginação em janelas de 10 anos):

```go
var catalogo = []Serie{
    {433, "IPCA", false},
    {11,  "SELIC_DIARIA", true},
    // ...
    {13621, "RESERVAS_DIARIAS", true}, // ← nova série diária
}
```

O restante do pipeline é genérico. Como `nome_serie` também é atualizado no `ON CONFLICT`, renomear uma série propaga o novo nome para os registros existentes na próxima execução.

---

## ⚠️ Problemas Conhecidos

- **Série 21618 (Euro PTAX compra):** o SGS tem respondido com uma página HTML (HTTP 200) após ~30 s, em todas as tentativas, enquanto as demais 19 séries respondem em ~1 s. O coletor registra a falha e segue; investigação em andamento (testar janela menor com `-inicio` e confirmar o código no portal do SGS).
- **Tempo da primeira carga:** ~2 min, dominado pelas séries diárias com janela de 10 anos no lado do BCB. Execuções seguintes são incrementais e levam segundos.
- **Instabilidade da API:** sob carga o SGS pode devolver HTML ou 5xx; o *retry* cobre a maioria dos casos, mas se falhas persistirem tente `-workers 3`.

---

## 📁 Estrutura do Projeto

```text
coletor-taxas-bcb/
├── main.go           # Pipeline completo: catálogo, HTTP client, goroutines e SQLite
├── go.mod            # Módulo e dependências
├── go.sum            # Checksum das dependências
├── .gitignore        # Ignora binários, .env, editor e o banco (*.db, -wal, -shm)
└── README.md         # Este documento
```

O banco `taxas_bcb.db` é gerado localmente e **não** é versionado.

---

## 🗺️ Roadmap

- [x] Paginação de séries diárias em janelas de 10 anos
- [x] Coleta incremental a partir da última data gravada
- [x] Retry com *backoff* e detecção de respostas HTML
- [x] Semáforo de concorrência, timeout configurável e código de saída
- [x] Coluna `data_iso` gerada para ordenação cronológica
- [ ] Resolver a série 21618 (Euro compra)
- [ ] Catálogo de séries em arquivo externo (JSON/YAML) em vez de código
- [ ] Renomear o módulo em `go.mod` para `github.com/alemiti7/coletor-taxas-bcb`
- [ ] Exportação direta para CSV/XLSX
- [ ] Testes unitários com `net/http/httptest`
- [ ] Publicação de binários via GitHub Releases

---

## 📄 Licença

Este projeto é distribuído sob a **GNU General Public License v3.0** — veja o arquivo [`LICENSE`](./LICENSE) para o texto completo.

Em resumo: você pode usar, estudar, modificar e redistribuir o código, inclusive comercialmente, desde que versões modificadas sejam também disponibilizadas sob a GPL-3.0 e mantenham os avisos de autoria. O software é fornecido **sem qualquer garantia**.

Copyright (C) 2026 Alexandre Mitsuru Nikaitow