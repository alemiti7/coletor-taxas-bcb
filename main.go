package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	// Import do driver SQLite em Go Puro (sem CGO) com alias anônimo (_)
	_ "github.com/glebarez/go-sqlite"
)

// Estrutura para mapear diretamente os campos do payload JSON retornado pela API do Banco Central
type DadoBCB struct {
	Data  string `json:"data"`  // Data no formato "DD/MM/AAAA"
	Valor string `json:"valor"` // Valor numérico recebido originalmente como string
}

// Estrutura auxiliar para transportar os dados coletados e eventuais erros através do Channel
type ResultadoSerie struct {
	Codigo int       // Código numérico da série no SGS/BCB
	Nome   string    // Identificador amigável da série (ex: SELIC, IPCA)
	Dados  []DadoBCB // Fatia (slice) contendo os pontos históricos coletados
	Err    error     // Captura falhas de HTTP ou Parsing isoladas por Goroutine
}

func main() {
	// Dicionário (Map) expandido com as principais séries oficiais do SGS/BCB
	series := map[int]string{
		// Taxas de Juros e Rendimentos
		11:   "SELIC_DIARIA",
		432:  "SELIC_META",
		4389: "CDI",
		196:  "POUPANCA",

		// Índices de Inflação
		433:   "IPCA",
		10844: "IPCA_EX", // IPCA Núcleo
		189:   "IGP_M",
		190:   "IGP_DI",
		188:   "INPC",

		// Câmbio e Paridades (Fechamento PTAX)
		1:     "DOLAR_VENDA",
		10813: "DOLAR_COMPRA",
		21619: "EURO_VENDA",
		21618: "EURO_COMPRA",

		// Atividade Econômica e Emprego
		24363: "IBC_BR",      // Prévia do PIB calculada pelo BCB
		28763: "CAGED_SALDO", // Saldo de Empregos Formais
		10777: "DESEMPREGO",  // Taxa de desocupação (PNAD)

		// Setor Externo e Reservas
		3545: "RESERVAS_INTERNACIONAIS", // Em milhões de USD
		2270: "BALANCA_COMERCIAL",       // Saldo quinzenal/mensal em USD

		// Indicadores do Setor Público
		4649: "DIVIDA_PUBLICA_PIB", // Dívida Consolidada / PIB
		4642: "RESULTADO_PRIMARIO", // NFSP Sem desvalorização cambial
	}

	inicio := time.Now()

	// -------------------------------------------------------------------------
	// 1. INICIALIZAÇÃO DA CONCORRÊNCIA (Goroutines + Buffer Channel)
	// -------------------------------------------------------------------------

	// Cria um canal com buffer igual à quantidade de séries para evitar bloqueio de escrita
	ch := make(chan ResultadoSerie, len(series))
	var wg sync.WaitGroup

	log.Println("Iniciando a coleta concorrente de taxas no Banco Central do Brasil...")

	// Dispara uma Goroutine paralela para cada série configurada no Map
	for codigo, nome := range series {
		wg.Add(1)

		// Passagem explícita de variáveis por parâmetro para evitar problemas de escopo de loop
		go func(cod int, n string) {
			defer wg.Done()

			// Executa a requisição HTTP para a API do BCB
			dados, err := buscarSerieBCB(cod)

			// Envia o payload encapsulado no canal
			ch <- ResultadoSerie{
				Codigo: cod,
				Nome:   n,
				Dados:  dados,
				Err:    err,
			}
		}(codigo, nome)
	}

	// Goroutine orquestradora: aguarda todas as requisições terminarem e fecha o canal
	go func() {
		wg.Wait()
		close(ch)
	}()

	// -------------------------------------------------------------------------
	// 2. CONEXÃO E PREPARAÇÃO DO BANCO DE DADOS SQLITE (GO PURO)
	// -------------------------------------------------------------------------

	// Abre (ou cria) o arquivo físico do banco de dados local "taxas_bcb.db"
	db, err := sql.Open("sqlite", "taxas_bcb.db")
	if err != nil {
		log.Fatalf("Erro fatal ao abrir o arquivo do SQLite: %v", err)
	}
	defer db.Close()

	// Criação da tabela com chave primária composta (codigo_serie + data) para garantir idempotência
	queryTabela := `
		CREATE TABLE IF NOT EXISTS taxas (
			codigo_serie INTEGER,
			nome_serie TEXT,
			data TEXT,
			valor REAL,
			PRIMARY KEY (codigo_serie, data)
		);
	`
	if _, err = db.Exec(queryTabela); err != nil {
		log.Fatalf("Erro ao preparar a tabela 'taxas' no SQLite: %v", err)
	}

	// -------------------------------------------------------------------------
	// 3. PROCESSAMENTO DOS RESULTADOS E PERSISTÊNCIA EM BANCO
	// -------------------------------------------------------------------------

	// O loop for-range consome os dados do canal à medida que as requisições finalizam
	for res := range ch {
		if res.Err != nil {
			log.Printf("[ERRO] Falha ao consultar a série %d (%s): %v\n", res.Codigo, res.Nome, res.Err)
			continue
		}

		// Grava o lote da série no banco SQLite em bloco único via transação
		err := salvarTaxasSQLite(db, res.Codigo, res.Nome, res.Dados)
		if err != nil {
			log.Printf("[ERRO] Falha ao gravar a série %d no banco: %v\n", res.Codigo, err)
		} else {
			fmt.Printf("✔ Série %d (%s) processada: %d registros inseridos/atualizados com sucesso.\n",
				res.Codigo, res.Nome, len(res.Dados))
		}
	}

	fmt.Printf("\nPipeline executado com sucesso em %v!\n", time.Since(inicio))
}

// -----------------------------------------------------------------------------
// FUNÇÃO AUXILIAR: CONSUMO DA API REST DO BANCO CENTRAL
// -----------------------------------------------------------------------------
func buscarSerieBCB(codigo int) ([]DadoBCB, error) {
	url := fmt.Sprintf("https://api.bcb.gov.br/dados/serie/bcdata.sgs.%d/dados?formato=json", codigo)

	// Configuração do cliente HTTP nativo com Timeout global de proteção
	client := http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("falha na requisição HTTP: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("resposta inválida do servidor: HTTP %d", resp.StatusCode)
	}

	// Leitura completa do corpo da resposta HTTP
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("falha ao ler stream de resposta: %w", err)
	}

	// Desserialização do payload JSON na struct Go
	var dados []DadoBCB
	if err := json.Unmarshal(body, &dados); err != nil {
		return nil, fmt.Errorf("falha na conversão de JSON para Go Struct: %w", err)
	}

	return dados, nil
}

// -----------------------------------------------------------------------------
// FUNÇÃO AUXILIAR: GRAVAÇÃO EM LOTE E UPSERT NO SQLITE
// -----------------------------------------------------------------------------
func salvarTaxasSQLite(db *sql.DB, codigo int, nome string, dados []DadoBCB) error {
	// Inicia uma transação explícita para máxima velocidade de escrita (I/O)
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("erro ao iniciar transação: %w", err)
	}
	// Em caso de erro prematuro ou panic, a transação será desfeita com segurança
	defer tx.Rollback()

	// Statement SQL compilado com cláusula ON CONFLICT para tratar atualizações de registros já existentes
	queryInsert := `
		INSERT INTO taxas (codigo_serie, nome_serie, data, valor)
		VALUES ($1, $2, $3, CAST($4 AS REAL))
		ON CONFLICT (codigo_serie, data) DO UPDATE SET 
			valor = EXCLUDED.valor,
			nome_serie = EXCLUDED.nome_serie
	`

	stmt, err := tx.Prepare(queryInsert)
	if err != nil {
		return fmt.Errorf("erro ao preparar instrução SQL: %w", err)
	}
	defer stmt.Close()

	// Itera sobre a lista de pontos históricos aplicando os registros na transação ativa
	for _, d := range dados {
		if d.Valor == "" {
			continue // Ignora valores nulos/vazios
		}

		_, err := stmt.Exec(codigo, nome, d.Data, d.Valor)
		if err != nil {
			log.Printf("Aviso: falha na gravação do registro (%s - %s): %v", d.Data, d.Valor, err)
		}
	}

	// Efetiva a gravação física dos dados no arquivo do banco de dados
	return tx.Commit()
}
