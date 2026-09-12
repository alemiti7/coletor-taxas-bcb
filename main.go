// Coletor de Taxas BCB
//
// Extrai séries temporais da API SGS do Banco Central do Brasil de forma
// concorrente e as persiste, de forma idempotente, em um banco SQLite local.
//
// Uso:
//
//	go run main.go                              # coleta incremental (padrão)
//	go run main.go -incremental=false           # recoleta o histórico completo
//	go run main.go -inicio 01/01/2020 -fim 31/12/2020
//	go run main.go -db /dados/taxas.db -workers 3 -timeout 90s
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/glebarez/go-sqlite" // driver SQLite em Go puro (sem CGO)
)

// -----------------------------------------------------------------------------
// CATÁLOGO DE SÉRIES
// -----------------------------------------------------------------------------

// Serie descreve uma série do SGS. O campo Diaria controla a paginação: a API
// limita séries diárias a 10 anos por requisição, então elas são buscadas em
// janelas sucessivas.
type Serie struct {
	Codigo int
	Nome   string
	Diaria bool
}

var catalogo = []Serie{
	// Juros e rendimentos
	{11, "SELIC_DIARIA", true},
	{432, "SELIC_META", true},
	{4389, "CDI", true},
	{196, "POUPANCA", false},

	// Inflação
	{433, "IPCA", false},
	{10844, "IPCA_EX", false},
	{188, "INPC", false},
	{189, "IGP_M", false},
	{190, "IGP_DI", false},

	// Câmbio (PTAX)
	{1, "DOLAR_VENDA", true},
	{10813, "DOLAR_COMPRA", true},
	{21619, "EURO_VENDA", true},
	{21618, "EURO_COMPRA", true},

	// Atividade e emprego
	{24363, "IBC_BR", false},
	{28763, "CAGED_SALDO", false},
	{24369, "DESEMPREGO_PNADC", false}, // substitui 10777 (PME, descontinuada em 2015)

	// Setor externo
	{3546, "RESERVAS_INTERNACIONAIS", false}, // mensal; 3545 é a série anual
	{22701, "BALANCA_COMERCIAL", false},      // substitui 2270 (descontinuada em 2014)

	// Fiscal
	{4649, "DIVIDA_PUBLICA_PIB", false},
	{4642, "RESULTADO_PRIMARIO", false},
}

// -----------------------------------------------------------------------------
// TIPOS AUXILIARES
// -----------------------------------------------------------------------------

const (
	layoutAPI   = "02/01/2006" // formato usado pela API do BCB
	layoutISO   = "2006-01-02" // formato gravado no banco
	janelaAnos  = 10           // limite da API para séries diárias
	maxRetries  = 4
	backoffBase = 2 * time.Second // 2s, 4s, 8s
)

// dadoAPI mapeia o payload JSON da API. A API retorna ambos os campos como string.
type dadoAPI struct {
	Data  string `json:"data"`
	Valor string `json:"valor"`
}

// Ponto é um registro já sanitizado, pronto para persistência.
type Ponto struct {
	Data  string // ISO 8601
	Valor float64
}

// Resultado transporta o lote de uma série (ou o erro) pelo canal.
type Resultado struct {
	Serie  Serie
	Pontos []Ponto
	Err    error
}

// -----------------------------------------------------------------------------
// MAIN
// -----------------------------------------------------------------------------

func main() {
	var (
		dbPath      = flag.String("db", "taxas_bcb.db", "caminho do banco SQLite")
		inicioFlag  = flag.String("inicio", "", "data inicial (dd/mm/aaaa); vazio = histórico completo ou incremental")
		fimFlag     = flag.String("fim", "", "data final (dd/mm/aaaa); vazio = hoje")
		workers     = flag.Int("workers", 5, "número máximo de requisições simultâneas")
		timeout     = flag.Duration("timeout", 60*time.Second, "timeout por requisição HTTP")
		incremental = flag.Bool("incremental", true, "continuar a partir da última data gravada de cada série")
	)
	flag.Parse()

	inicioExec := time.Now()
	log.SetFlags(log.Ltime)

	// Intervalo global de coleta -------------------------------------------------
	fim := time.Now()
	if *fimFlag != "" {
		t, err := time.Parse(layoutAPI, *fimFlag)
		if err != nil {
			log.Fatalf("flag -fim inválida (%q): use dd/mm/aaaa", *fimFlag)
		}
		fim = t
	}
	var inicioGlobal time.Time // zero = sem restrição
	if *inicioFlag != "" {
		t, err := time.Parse(layoutAPI, *inicioFlag)
		if err != nil {
			log.Fatalf("flag -inicio inválida (%q): use dd/mm/aaaa", *inicioFlag)
		}
		inicioGlobal = t
	}

	// Banco de dados: aberto e validado ANTES de disparar qualquer requisição -----
	db, err := abrirBanco(*dbPath)
	if err != nil {
		log.Fatalf("erro ao preparar o banco %s: %v", *dbPath, err)
	}
	defer func() {
		// Consolida o WAL no arquivo principal, esvaziando os -wal/-shm.
		db.Exec("PRAGMA wal_checkpoint(TRUNCATE);")
		db.Close()
	}()

	// Coleta concorrente ----------------------------------------------------------
	client := &http.Client{Timeout: *timeout}
	ctx := context.Background()

	ch := make(chan Resultado, len(catalogo))
	sem := make(chan struct{}, *workers) // semáforo: limita requisições em voo
	var wg sync.WaitGroup

	log.Printf("Iniciando coleta de %d séries (até %d simultâneas)...", len(catalogo), *workers)

	for _, s := range catalogo {
		// Ponto de partida desta série: -inicio, ou última data gravada, ou padrão.
		inicio := inicioGlobal
		if inicio.IsZero() && *incremental {
			if ultima, ok := ultimaData(db, s.Codigo); ok {
				inicio = ultima // upsert absorve a sobreposição de um dia/mês
			}
		}
		if inicio.IsZero() && s.Diaria {
			inicio = fim.AddDate(-janelaAnos, 0, 1) // diárias sem histórico: últimos 10 anos
		}

		wg.Add(1)
		go func(s Serie, inicio, fim time.Time) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			pontos, err := coletarSerie(ctx, client, s, inicio, fim)
			ch <- Resultado{Serie: s, Pontos: pontos, Err: err}
		}(s, inicio, fim)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	// Persistência ------------------------------------------------------------------
	var falhas, totalRegistros int
	for r := range ch {
		if r.Err != nil {
			falhas++
			log.Printf("[ERRO] série %d (%s): %v", r.Serie.Codigo, r.Serie.Nome, r.Err)
			continue
		}
		if err := salvarPontos(db, r.Serie, r.Pontos); err != nil {
			falhas++
			log.Printf("[ERRO] gravação da série %d (%s): %v", r.Serie.Codigo, r.Serie.Nome, err)
			continue
		}
		totalRegistros += len(r.Pontos)
		log.Printf("✔ %-24s (%5d) %6d registros", r.Serie.Nome, r.Serie.Codigo, len(r.Pontos))
	}

	log.Printf("Concluído em %s: %d registros gravados, %d série(s) com falha.",
		time.Since(inicioExec).Round(time.Millisecond), totalRegistros, falhas)

	if falhas > 0 {
		os.Exit(1)
	}
}

// -----------------------------------------------------------------------------
// BANCO DE DADOS
// -----------------------------------------------------------------------------

func abrirBanco(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}

	// WAL melhora a concorrência de leitura durante a escrita; synchronous=NORMAL
	// é seguro com WAL e reduz fsyncs.
	pragmas := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=NORMAL;",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			return nil, fmt.Errorf("%s: %w", p, err)
		}
	}

	ddl := `
		CREATE TABLE IF NOT EXISTS taxas (
			codigo_serie INTEGER NOT NULL,
			nome_serie   TEXT    NOT NULL,
			data         TEXT    NOT NULL,   -- ISO 8601 (aaaa-mm-dd)
			valor        REAL    NOT NULL,
			PRIMARY KEY (codigo_serie, data)
		);`
	if _, err := db.Exec(ddl); err != nil {
		return nil, err
	}
	return db, nil
}

// ultimaData retorna a maior data gravada para a série, se houver.
// Como a coluna está em ISO 8601, MAX() textual equivale a MAX() cronológico.
func ultimaData(db *sql.DB, codigo int) (time.Time, bool) {
	var s sql.NullString
	err := db.QueryRow(`SELECT MAX(data) FROM taxas WHERE codigo_serie = ?`, codigo).Scan(&s)
	if err != nil || !s.Valid {
		return time.Time{}, false
	}
	t, err := time.Parse(layoutISO, s.String)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

func salvarPontos(db *sql.DB, s Serie, pontos []Ponto) error {
	if len(pontos) == 0 {
		return nil
	}
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback() // no-op após Commit bem-sucedido

	stmt, err := tx.Prepare(`
		INSERT INTO taxas (codigo_serie, nome_serie, data, valor)
		VALUES (?, ?, ?, ?)
		ON CONFLICT (codigo_serie, data) DO UPDATE SET
			valor      = excluded.valor,
			nome_serie = excluded.nome_serie`)
	if err != nil {
		return fmt.Errorf("prepare: %w", err)
	}
	defer stmt.Close()

	for _, p := range pontos {
		if _, err := stmt.Exec(s.Codigo, s.Nome, p.Data, p.Valor); err != nil {
			return fmt.Errorf("insert %s: %w", p.Data, err)
		}
	}
	return tx.Commit()
}

// -----------------------------------------------------------------------------
// COLETA
// -----------------------------------------------------------------------------

// coletarSerie busca a série no intervalo [inicio, fim]. Para séries diárias o
// intervalo é fatiado em janelas de no máximo 10 anos, conforme exige a API.
func coletarSerie(ctx context.Context, client *http.Client, s Serie, inicio, fim time.Time) ([]Ponto, error) {
	var todos []Ponto
	for _, j := range janelas(s, inicio, fim) {
		brutos, err := buscarComRetry(ctx, client, s.Codigo, j[0], j[1])
		if err != nil {
			return nil, err
		}
		todos = append(todos, sanitizar(s, brutos)...)
	}
	return todos, nil
}

// janelas devolve os pares [inicio, fim] a consultar. Um par com inicio zero
// significa "sem filtro de data" (histórico completo).
func janelas(s Serie, inicio, fim time.Time) [][2]time.Time {
	if inicio.IsZero() {
		return [][2]time.Time{{time.Time{}, time.Time{}}}
	}
	if !s.Diaria {
		return [][2]time.Time{{inicio, fim}}
	}
	var out [][2]time.Time
	for ini := inicio; !ini.After(fim); {
		fimJanela := ini.AddDate(janelaAnos, 0, -1)
		if fimJanela.After(fim) {
			fimJanela = fim
		}
		out = append(out, [2]time.Time{ini, fimJanela})
		ini = fimJanela.AddDate(0, 0, 1)
	}
	return out
}

func montarURL(codigo int, inicio, fim time.Time) string {
	url := fmt.Sprintf("https://api.bcb.gov.br/dados/serie/bcdata.sgs.%d/dados?formato=json", codigo)
	if !inicio.IsZero() {
		url += fmt.Sprintf("&dataInicial=%s&dataFinal=%s",
			inicio.Format(layoutAPI), fim.Format(layoutAPI))
	}
	return url
}

// buscarComRetry repete a requisição em falhas de rede, HTTP 429 ou 5xx,
// com backoff exponencial (2s, 4s, 8s).
func buscarComRetry(ctx context.Context, client *http.Client, codigo int, inicio, fim time.Time) ([]dadoAPI, error) {
	var ultimoErr error
	for tentativa := 0; tentativa < maxRetries; tentativa++ {
		if tentativa > 0 {
			espera := backoffBase << (tentativa - 1)
			select {
			case <-time.After(espera):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		dados, err := buscar(ctx, client, montarURL(codigo, inicio, fim))
		if err == nil {
			return dados, nil
		}
		ultimoErr = err
		var he *httpErr
		if errors.As(err, &he) && !he.retryable() {
			break // 4xx (exceto 429) não vai melhorar tentando de novo
		}
		log.Printf("[AVISO] série %d: tentativa %d falhou (%v); repetindo...", codigo, tentativa+1, err)
	}
	return nil, fmt.Errorf("após %d tentativa(s): %w", maxRetries, ultimoErr)
}

type httpErr struct{ status int }

func (e *httpErr) Error() string   { return fmt.Sprintf("HTTP %d", e.status) }
func (e *httpErr) retryable() bool { return e.status == 429 || e.status >= 500 }

func buscar(ctx context.Context, client *http.Client, url string) ([]dadoAPI, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("requisição: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body) // permite reuso da conexão
		return nil, &httpErr{status: resp.StatusCode}
	}

	// O SGS ocasionalmente devolve HTTP 200 com uma página HTML de erro em vez
	// de JSON (normalmente sob carga). Tratamos como falha transitória.
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "json") {
		amostra, _ := io.ReadAll(io.LimitReader(resp.Body, 120))
		return nil, &respostaInvalida{contentType: ct, amostra: string(amostra)}
	}

	var dados []dadoAPI
	if err := json.NewDecoder(resp.Body).Decode(&dados); err != nil {
		return nil, &respostaInvalida{contentType: "json malformado", amostra: err.Error()}
	}
	return dados, nil
}

// respostaInvalida sinaliza corpo não-JSON com status 200; é sempre retryable.
type respostaInvalida struct{ contentType, amostra string }

func (e *respostaInvalida) Error() string {
	return fmt.Sprintf("resposta não-JSON (%s): %.80q", e.contentType, e.amostra)
}

// sanitizar converte data para ISO 8601 e valor para float64, descartando
// registros vazios ou malformados (com aviso no log).
func sanitizar(s Serie, brutos []dadoAPI) []Ponto {
	pontos := make([]Ponto, 0, len(brutos))
	for _, d := range brutos {
		if d.Valor == "" {
			continue
		}
		t, err := time.Parse(layoutAPI, d.Data)
		if err != nil {
			log.Printf("[AVISO] série %d: data inválida %q ignorada", s.Codigo, d.Data)
			continue
		}
		v, err := strconv.ParseFloat(d.Valor, 64)
		if err != nil {
			log.Printf("[AVISO] série %d (%s): valor inválido %q ignorado", s.Codigo, d.Data, d.Valor)
			continue
		}
		pontos = append(pontos, Ponto{Data: t.Format(layoutISO), Valor: v})
	}
	return pontos
}
