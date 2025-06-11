package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/joho/godotenv"
	"github.com/pkg/sftp"
	"github.com/segmentio/kafka-go"
	"golang.org/x/crypto/ssh"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	// Carrega variáveis do .env
	if err := godotenv.Load(); err != nil {
		log.Fatal("Erro ao carregar .env")
	}

	cfg := loadEnvVars()
	printEnvDebug(cfg)

	for {
		err := processFileIfExists(cfg)
		if err != nil {
			log.Printf("Processamento falhou: %v", err)
		}
		time.Sleep(cfg.PollInterval)
	}
}

type Config struct {
	SFTPUser        string
	SFTPPassword    string
	SFTPHost        string
	SFTPPort        string
	SFTPDir         string
	FileName        string
	HeadersFileName string
	ArchiveDir      string
	KafkaBrokers    string
	KafkaTopic      string
	PollInterval    time.Duration
}

func loadEnvVars() Config {
	return Config{
		SFTPUser:        os.Getenv("SFTP_USER"),
		SFTPPassword:    os.Getenv("SFTP_PASSWORD"),
		SFTPHost:        os.Getenv("SFTP_HOST"),
		SFTPPort:        os.Getenv("SFTP_PORT"),
		SFTPDir:         os.Getenv("SFTP_DIR"),
		FileName:        os.Getenv("SFTP_FILENAME"),
		HeadersFileName: os.Getenv("SFTP_HEADERS_FILENAME"),
		ArchiveDir:      os.Getenv("SFTP_ARCHIVE_DIR"),
		KafkaBrokers:    os.Getenv("KAFKA_BROKERS"),
		KafkaTopic:      os.Getenv("KAFKA_TOPIC"),
		PollInterval:    10 * time.Second,
	}
}

func printEnvDebug(cfg Config) {
	fmt.Printf("\nDEBUG VARS:\n"+
		"SFTP_USER: %s\nSFTP_PASSWORD: %s\nSFTP_HOST: %s\nSFTP_PORT: %s\nSFTP_DIR: %s\nSFTP_FILENAME: %s\nSFTP_HEADERS_FILENAME: %s\nSFTP_ARCHIVE_DIR: %s\nKAFKA_BROKERS: %s\nKAFKA_TOPIC: %s\n\n",
		cfg.SFTPUser, cfg.SFTPPassword, cfg.SFTPHost, cfg.SFTPPort, cfg.SFTPDir, cfg.FileName, cfg.HeadersFileName, cfg.ArchiveDir, cfg.KafkaBrokers, cfg.KafkaTopic)
}

func processFileIfExists(cfg Config) error {
	sftpClient, err := connectSFTP(cfg)
	if err != nil {
		return fmt.Errorf("erro conectando SFTP: %w", err)
	}
	defer sftpClient.Close()

	filePath := cfg.SFTPDir + cfg.FileName
	headersPath := cfg.SFTPDir + cfg.HeadersFileName

	log.Printf("DEBUG: SFTP_DIR=%s, HEADERS_FILENAME=%s, JUNTANDO: %s", cfg.SFTPDir, cfg.HeadersFileName, headersPath)

	header, err := loadHeaders(sftpClient, headersPath)
	if err != nil {
		return fmt.Errorf("erro lendo headers.txt: %w", err)
	}

	file, err := sftpClient.Open(filePath)
	if err != nil {
		if strings.Contains(err.Error(), "no such file") {
			log.Printf("Arquivo %s não encontrado, esperando...", filePath)
			return nil
		}
		return fmt.Errorf("erro abrindo arquivo: %w", err)
	}
	defer file.Close()

	log.Printf("Processando arquivo %s", filePath)

	brokers := splitComma(cfg.KafkaBrokers)
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      brokers,
		Topic:        cfg.KafkaTopic,
		Async:        true,
		BatchSize:    1000,
		BatchTimeout: 500 * time.Millisecond,
	})
	defer writer.Close()

	reader := csv.NewReader(file)
	reader.Comma = ';' // DELIMITADOR

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
		err = writer.WriteMessages(context.Background(),
			kafka.Message{Value: jsonBytes})
		if err != nil {
			log.Printf("Erro enviando pro Kafka linha %d: %v", linha, err)
		}
		linha++
	}

	log.Printf("Total de linhas lidas do arquivo: %d", linha-1)
	log.Printf("Tempo total para processar arquivo: %s", time.Since(start))

	// Move para archive com nome incremental!
	err = sftpClient.MkdirAll(cfg.ArchiveDir)
	if err != nil {
		log.Printf("Erro criando diretório archive: %v", err)
	}
	archivePath, err := findAvailableArchiveName(sftpClient, cfg.ArchiveDir, cfg.FileName)
	if err != nil {
		log.Printf("Erro ao encontrar nome disponível para archive: %v", err)
		return err
	}

	// Tenta renomear
	err = sftpClient.Rename(filePath, archivePath)
	if err != nil {
		log.Printf("Erro movendo arquivo pra archive: %v", err)
		log.Printf("Tentando copiar + deletar como fallback...")

		// Faz a cópia
		src, err1 := sftpClient.Open(filePath)
		if err1 != nil {
			log.Printf("Erro abrindo arquivo para cópia: %v", err1)
			return err
		}
		defer src.Close()
		dst, err2 := sftpClient.Create(archivePath)
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
		err4 := sftpClient.Remove(filePath)
		if err4 != nil {
			log.Printf("Erro deletando arquivo original após cópia: %v", err4)
			return err
		}
		log.Printf("Arquivo copiado para archive e removido do diretório original! (%s)", archivePath)
	} else {
		log.Printf("Arquivo %s movido para %s", filePath, archivePath)
	}
	return nil
}

// Função para achar um nome disponível (arquivo.txt, arquivo1.txt, arquivo2.txt, ...)
func findAvailableArchiveName(sftpClient *sftp.Client, archiveDir, fileName string) (string, error) {
	ext := filepath.Ext(fileName)
	base := strings.TrimSuffix(fileName, ext)
	for i := 0; ; i++ {
		var name string
		if i == 0 {
			name = fmt.Sprintf("%s%s", base, ext)
		} else {
			name = fmt.Sprintf("%s%d%s", base, i, ext)
		}
		path := archiveDir + name
		_, err := sftpClient.Stat(path)
		if os.IsNotExist(err) {
			return path, nil
		}
		if err != nil && !os.IsNotExist(err) {
			return "", err
		}
	}
}

func loadHeaders(sftpClient *sftp.Client, headerPath string) ([]string, error) {
	log.Printf("DEBUG: Tentando abrir headers: [%s]", headerPath)
	file, err := sftpClient.Open(headerPath)
	if err != nil {
		log.Printf("ERRO: Não conseguiu abrir [%s] via SFTP: %v", headerPath, err)
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

func connectSFTP(cfg Config) (*sftp.Client, error) {
	config := &ssh.ClientConfig{
		User: cfg.SFTPUser,
		Auth: []ssh.AuthMethod{
			ssh.Password(cfg.SFTPPassword),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         10 * time.Second,
	}
	addr := net.JoinHostPort(cfg.SFTPHost, cfg.SFTPPort)
	conn, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		return nil, fmt.Errorf("SSH dial erro: %w", err)
	}
	client, err := sftp.NewClient(conn)
	if err != nil {
		return nil, fmt.Errorf("SFTP client erro: %w", err)
	}
	return client, nil
}
