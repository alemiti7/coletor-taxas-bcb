// 1. DECLARAÇÃO DO PACOTE
// Todo arquivo Go deve começar declarando a qual pacote pertence.
// O pacote 'main' indica que este arquivo é um ponto de entrada executável.
package main

// 2. IMPORTAÇÃO DE DEPENDÊNCIAS
// Importamos os pacotes da biblioteca padrão do Go que serão utilizados.
import (
	"encoding/json" // Para converter dados JSON da API em structs Go (Unmarshal)
	"fmt"           // Para formatação e impressão de texto no terminal (print/printf)
	"io"            // Para leitura de streams de dados (como a resposta HTTP)
	"net/http"      // Para realizar requisições web (GET, POST, etc.)
	"sync"          // Fornece primitivas de sincronização, como o WaitGroup
	"time"          // Para controlar timeouts, medir tempo de execução e lidar com datas
)

// 3. ESTRUTURAS DE DADOS (STRUCTS) E TAGS DE MAPEAMENTO
// Em Go não usamos classes, usamos structs. As tags `json:"..."` informam
// ao decodificador como mapear as chaves do JSON para os campos da struct.

// TaxaBCB representa a estrutura exata de um item retornado pelo JSON da API do Banco Central.
type TaxaBCB struct {
	Data  string `json:"data"`  // Mapeia a chave "data" do JSON para o campo Data
	Valor string `json:"valor"` // Mapeia a chave "valor" do JSON para o campo Valor
}

// ResultadoTaxa é uma struct personalizada que agrupa o resultado de uma busca.
// Ela permite trafegar tanto os dados quanto eventuais erros através de um canal.
type ResultadoTaxa struct {
	Nome string  // Nome amigável da taxa (ex: "SELIC")
	Taxa TaxaBCB // A struct com os dados retornados
	Erro error   // Armazena o erro caso ocorra uma falha (em Go, erros são valores)
}

// 4. VARIÁVEIS GLOBAIS
// Mapa no formato [chave]valor relacionando o nome da taxa ao código da série no Banco Central.
var codigosSeries = map[string]string{
	// Taxas de juros e inflação que você já usa
	"SELIC": "11",  // Selic diária
	"CDI":   "12",  // CDI diário (B3)
	"IPCA":  "433", // IPCA mensal (%)

	// Outros indicadores de inflação
	"INPC":   "188", // INPC mensal (IBGE)
	"IGP-M":  "189", // IGP-M mensal (FGV)
	"IGP-DI": "190", // IGP-DI mensal (FGV)

	// Taxas de juros de referência e poupança
	"TR":       "226",   // Taxa Referencial (diária)
	"POUPANCA": "195",   // Rendimento da Poupança (mensal)
	"TJLP":     "256",   // Taxa de Juros de Longo Prazo (trimestral)
	"TLP":      "27574", // Taxa de Longo Prazo (mensal)

	// Câmbio / Moedas
	"DOLAR_VENDA": "10813", // Dólar comercial (venda - fechamento)
	"EURO_VENDA":  "21619", // Euro (venda - fechamento)

	// Metas do COPOM
	"META_SELIC": "432", // Meta da taxa Selic definida pelo COPOM (% a.a.)
}

// 5. FUNÇÃO DE BUSCA CONCORRENTE
// Esta função faz a requisição HTTP. Ela recebe dois ponteiros/referências especiais:
// - wg *sync.WaitGroup: Usado para notificar a função principal quando o trabalho terminar.
// - ch chan<- ResultadoTaxa: Um canal exclusivo para envio (chan<-) dos resultados.
func buscarTaxa(nome string, codigo string, wg *sync.WaitGroup, ch chan<- ResultadoTaxa) {
	// 'defer' agenda a execução de uma instrução para o momento em que a função retornar.
	// Garante que wg.Done() seja chamado ao final, mesmo que ocorra um erro antes.
	defer wg.Done()

	// Monta a URL de consulta dinâmica com base no código da série
	url := fmt.Sprintf("https://api.bcb.gov.br/dados/serie/bcdata.sgs.%s/dados/ultimos/1?formato=json", codigo)

	// Configura um cliente HTTP com tempo limite (Timeout) de 5 segundos para evitar travamentos
	client := http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)

	// Em Go, tratamento de erro é explícito: testamos se 'err != nil' imediatamente
	if err != nil {
		ch <- ResultadoTaxa{Nome: nome, Erro: fmt.Errorf("falha na requisição HTTP: %w", err)}
		return
	}
	// 'defer' fecha o corpo da resposta HTTP automaticamente ao sair da função (libera memória e conexões)
	defer resp.Body.Close()

	// Valida se o servidor respondeu com código de sucesso 200 OK
	if resp.StatusCode != http.StatusOK {
		ch <- ResultadoTaxa{Nome: nome, Erro: fmt.Errorf("status HTTP retornado: %d", resp.StatusCode)}
		return
	}

	// Lê todo o conteúdo textual retornado no corpo da resposta
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		ch <- ResultadoTaxa{Nome: nome, Erro: fmt.Errorf("erro ao ler o corpo da resposta: %w", err)}
		return
	}

	// Decodifica o JSON em um slice (array dinâmico) de structs TaxaBCB
	var taxas []TaxaBCB
	if err := json.Unmarshal(body, &taxas); err != nil || len(taxas) == 0 {
		ch <- ResultadoTaxa{Nome: nome, Erro: fmt.Errorf("erro ao converter JSON: %w", err)}
		return
	}

	// Envia a struct preenchida com o sucesso para dentro do canal
	ch <- ResultadoTaxa{Nome: nome, Taxa: taxas[0], Erro: nil}
}

// 6. FUNÇÃO PRINCIPAL (ENTRYPOINT)
func main() {
	// Captura o momento atual para calcular a duração total no final
	inicio := time.Now()

	// WaitGroup serve para esperar que um conjunto de goroutines termine a execução
	var wg sync.WaitGroup

	// Cria um canal com buffer para trafegar dados do tipo ResultadoTaxa.
	// O tamanho do buffer é exatamente a quantidade de taxas no mapa (evita bloqueios).
	canalResultados := make(chan ResultadoTaxa, len(codigosSeries))

	fmt.Println("Iniciando coleta concorrente de taxas financeiras (BCB)...")

	// 7. DISPARO DAS GOROUTINES
	// Iteramos sobre o mapa. Para cada taxa, iniciamos uma execução em paralelo.
	for nome, codigo := range codigosSeries {
		wg.Add(1) // Incrementa o contador do WaitGroup em +1 para cada tarefa agendada

		// A palavra-chave 'go' inicia uma nova Goroutine (thread ultra-leve gerenciada pelo Go)
		go buscarTaxa(nome, codigo, &wg, canalResultados)
	}

	// 8. GERENCIADOR DE FECHAMENTO DO CANAL
	// Disparamos uma goroutine anônima encarregada de fechar o canal assim que todas as tarefas concluírem.
	go func() {
		wg.Wait()              // Bloqueia a execução aqui até que o contador do WaitGroup volte a zero (via wg.Done)
		close(canalResultados) // Fecha o canal para avisar o loop de leitura que não haverá mais dados
	}()

	// 9. LEITURA E PROCESSAMENTO DOS RESULTADOS
	// O loop 'range' em um canal lê os itens à medida que chegam de forma concorrente.
	// O loop é interrompido automaticamente quando o canal é fechado (close).
	fmt.Println("\n--- Resultados Obtidos ---")
	for res := range canalResultados {
		// Se a goroutine enviou um objeto com Erro preenchido, tratamos aqui
		if res.Erro != nil {
			fmt.Printf("[ERRO] %s: %v\n", res.Nome, res.Erro)
			continue
		}

		// Impressão formatada dos dados recuperados
		fmt.Printf("Taxa %-5s | Data: %s | Valor: %s%%\n", res.Nome, res.Taxa.Data, res.Taxa.Valor)
	}

	// Exibe o tempo total gasto (normalmente < 500ms devido ao paralelismo)
	fmt.Printf("\nProcesso concluído em: %v\n", time.Since(inicio))
}
