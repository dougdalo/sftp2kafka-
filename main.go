package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Line struct {
	JSON []byte
}

type Config struct {
	LocalDir        string
	HeadersFileName string
	ArchiveDir      string
	KafkaBrokers    []string
	KafkaTopic      string
	PollInterval    time.Duration
	NumWorkers      int
	BatchSize       int
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatal("Erro ao carregar .env")
	}

	cfg := loadEnvVars()
	printEnvDebug(cfg)

	for {
		err := processAllTxtFilesLocal(cfg)
		if err != nil {
			log.Printf("Processamento falhou: %v", err)
		}
		time.Sleep(cfg.PollInterval)
	}
}

func loadEnvVars() Config {
	return Config{
		LocalDir:        os.Getenv("LOCAL_DIR"),
		HeadersFileName: os.Getenv("HEADERS_FILENAME"),
		ArchiveDir:      os.Getenv("LOCAL_ARCHIVE_DIR"),
		KafkaBrokers:    splitComma(os.Getenv("KAFKA_BROKERS")),
		KafkaTopic:      os.Getenv("KAFKA_TOPIC"),
		PollInterval:    10 * time.Second,
		NumWorkers:      8,    // ajuste conforme CPU/infra
		BatchSize:       2000, // ajuste conforme teste
	}
}

func printEnvDebug(cfg Config) {
	fmt.Printf("\nDEBUG VARS:\n"+
		"LOCAL_DIR: %s\nHEADERS_FILENAME: %s\nLOCAL_ARCHIVE_DIR: %s\nKAFKA_BROKERS: %v\nKAFKA_TOPIC: %s\nNUM_WORKERS: %d\nBATCH_SIZE: %d\n\n",
		cfg.LocalDir, cfg.HeadersFileName, cfg.ArchiveDir, cfg.KafkaBrokers, cfg.KafkaTopic, cfg.NumWorkers, cfg.BatchSize)
}

func processAllTxtFilesLocal(cfg Config) error {
	files, err := os.ReadDir(cfg.LocalDir)
	if err != nil {
		return fmt.Errorf("erro lendo diretório local: %w", err)
	}

	countFiles := 0
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if strings.HasSuffix(strings.ToLower(file.Name()), ".txt") {
			countFiles++
			err := processFileLocalConcurrent(cfg, file.Name())
			if err != nil {
				log.Printf("Erro processando arquivo [%s]: %v", file.Name(), err)
			}
		}
	}

	if countFiles == 0 {
		log.Printf("Nenhum arquivo .txt encontrado em %s", cfg.LocalDir)
	}
	return nil
}

func processFileLocalConcurrent(cfg Config, fileName string) error {
	filePath := filepath.Join(cfg.LocalDir, fileName)
	headersPath := filepath.Join(cfg.LocalDir, cfg.HeadersFileName)

	log.Printf("DEBUG: LOCAL_DIR=%s, HEADERS_FILENAME=%s, ARQUIVO ATUAL: %s", cfg.LocalDir, cfg.HeadersFileName, filePath)

	// Abre o arquivo local
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("erro abrindo arquivo [%s]: %w", fileName, err)
	}
	defer file.Close()

	// Lê headers (do headers.txt ou gera automático)
	var header []string
	header, err = loadHeadersLocal(headersPath)
	if err != nil {
		log.Printf("Não encontrou headers.txt, usando headers automáticos para [%s]...", fileName)
		reader := csv.NewReader(file)
		reader.Comma = ';'
		firstLine, err2 := reader.Read()
		if err2 != nil {
			return fmt.Errorf("erro lendo primeira linha para gerar headers: %w", err2)
		}
		header = make([]string, len(firstLine))
		for i := range firstLine {
			header[i] = fmt.Sprintf("field%d", i+1)
		}
		_, errSeek := file.Seek(0, io.SeekStart)
		if errSeek != nil {
			return fmt.Errorf("erro ao voltar ponteiro do arquivo: %w", errSeek)
		}
	}

	log.Printf("Headers em uso para [%s]: %+v", fileName, header)
	log.Printf("Processando arquivo [%s] em modo concorrente...", filePath)

	linesCh := make(chan Line, 10000)
	var wg sync.WaitGroup

	// Inicia workers
	for i := 0; i < cfg.NumWorkers; i++ {
		wg.Add(1)
		go workerKafka(cfg, linesCh, &wg, i)
	}

	reader := csv.NewReader(file)
	reader.Comma = ';'

	linha := 1
	start := time.Now()
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Erro lendo linha %d: %v", linha, err)
			continue
		}
		rowMap := mapLine(header, record)
		jsonBytes, err := json.Marshal(rowMap)
		if err != nil {
			log.Printf("Erro convertendo linha %d pra JSON: %v", linha, err)
			continue
		}
		linesCh <- Line{JSON: jsonBytes}
		linha++
		if linha%500000 == 0 {
			log.Printf("[%s] Linhas lidas até agora: %d", fileName, linha-1)
		}
	}
	close(linesCh)
	wg.Wait()

	log.Printf("Total de linhas lidas do arquivo [%s]: %d", fileName, linha-1)
	log.Printf("Tempo total para processar arquivo [%s]: %s", fileName, time.Since(start))

	// Move para archive
	err = os.MkdirAll(cfg.ArchiveDir, 0777)
	if err != nil {
		log.Printf("Erro criando diretório archive: %v", err)
	}
	archivePath, err := findAvailableArchiveNameLocal(cfg.ArchiveDir, fileName)
	if err != nil {
		log.Printf("Erro ao encontrar nome disponível para archive: %v", err)
		return err
	}

	err = os.Rename(filePath, archivePath)
	if err != nil {
		log.Printf("Erro movendo arquivo pra archive: %v", err)
		log.Printf("Tentando copiar + deletar como fallback...")

		src, err1 := os.Open(filePath)
		if err1 != nil {
			log.Printf("Erro abrindo arquivo para cópia: %v", err1)
			return err
		}
		defer src.Close()
		dst, err2 := os.Create(archivePath)
		if err2 != nil {
			log.Printf("Erro criando arquivo de destino na archive: %v", err2)
			return err
		}
		defer dst.Close()
		_, err3 := io.Copy(dst, src)
		if err3 != nil {
			log.Printf("Erro copiando arquivo: %v", err3)
			return err
		}
		err4 := os.Remove(filePath)
		if err4 != nil {
			log.Printf("Erro deletando arquivo original após cópia: %v", err4)
			return err
		}
		log.Printf("Arquivo copiado para archive e removido do diretório original! (%s)", archivePath)
	} else {
		log.Printf("Arquivo [%s] movido para %s", fileName, archivePath)
	}
	return nil
}

func workerKafka(cfg Config, linesCh <-chan Line, wg *sync.WaitGroup, id int) {
	defer wg.Done()
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      cfg.KafkaBrokers,
		Topic:        cfg.KafkaTopic,
		Async:        true,
		BatchSize:    cfg.BatchSize,
		BatchTimeout: 2 * time.Second,
	})
	defer writer.Close()

	messages := make([]kafka.Message, 0, cfg.BatchSize)
	for line := range linesCh {
		messages = append(messages, kafka.Message{Value: line.JSON})
		if len(messages) >= cfg.BatchSize {
			err := writer.WriteMessages(context.Background(), messages...)
			if err != nil {
				log.Printf("[Worker %d] Erro ao escrever batch: %v", id, err)
			}
			messages = messages[:0]
		}
	}
	// Envia o resto do batch
	if len(messages) > 0 {
		err := writer.WriteMessages(context.Background(), messages...)
		if err != nil {
			log.Printf("[Worker %d] Erro ao escrever batch final: %v", id, err)
		}
	}
}

func findAvailableArchiveNameLocal(archiveDir, fileName string) (string, error) {
	ext := filepath.Ext(fileName)
	base := strings.TrimSuffix(fileName, ext)
	for i := 0; ; i++ {
		var name string
		if i == 0 {
			name = fmt.Sprintf("%s%s", base, ext)
		} else {
			name = fmt.Sprintf("%s%d%s", base, i, ext)
		}
		path := filepath.Join(archiveDir, name)
		_, err := os.Stat(path)
		if os.IsNotExist(err) {
			return path, nil
		}
		if err != nil && !os.IsNotExist(err) {
			return "", err
		}
	}
}

func loadHeadersLocal(headerPath string) ([]string, error) {
	log.Printf("DEBUG: Tentando abrir headers: [%s]", headerPath)
	file, err := os.Open(headerPath)
	if err != nil {
		log.Printf("ERRO: Não conseguiu abrir [%s]: %v", headerPath, err)
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = ';'
	headers, err := reader.Read()
	if err != nil {
		log.Printf("ERRO: Falha lendo headers do arquivo [%s]: %v", headerPath, err)
		return nil, err
	}

	log.Printf("DEBUG: Headers carregados: %+v", headers)
	return headers, nil
}

func splitComma(str string) []string {
	parts := strings.Split(str, ",")
	var out []string
	for _, p := range parts {
		out = append(out, strings.TrimSpace(p))
	}
	return out
}

func mapLine(header, record []string) map[string]string {
	row := make(map[string]string)
	for i, campo := range header {
		if i < len(record) {
			row[campo] = record[i]
		}
	}
	return row
}
