package cmd

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math/big"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"
)

type genConfig struct {
	host           string
	port           int
	user           string
	password       string
	numDBs         int
	minSizeMB      int
	maxSizeMB      int
	maxParallel    int
	prefix         string
	nonInteractive bool
}

var genCfg genConfig

var generateCmd = &cobra.Command{
	Use:   "generate",
	Short: "Generate fake databases with random data for testing",
	Long: `Creates multiple PostgreSQL databases filled with random data.
Each database gets a single table with random text payloads.
Useful for testing the migrate command at scale.`,
	RunE: runGenerate,
}

func init() {
	rootCmd.AddCommand(generateCmd)

	generateCmd.Flags().StringVar(&genCfg.host, "host", "", "PostgreSQL host (or GEN_HOST env)")
	generateCmd.Flags().IntVar(&genCfg.port, "port", 0, "PostgreSQL port (default 5432, or GEN_PORT env)")
	generateCmd.Flags().StringVar(&genCfg.user, "user", "", "PostgreSQL username (or GEN_USER env)")
	generateCmd.Flags().IntVar(&genCfg.numDBs, "num-dbs", 350, "Number of databases to create")
	generateCmd.Flags().IntVar(&genCfg.minSizeMB, "min-size", 1, "Minimum database size in MB")
	generateCmd.Flags().IntVar(&genCfg.maxSizeMB, "max-size", 100, "Maximum database size in MB")
	generateCmd.Flags().IntVarP(&genCfg.maxParallel, "parallel", "p", 10, "Max parallel DB creation jobs")
	generateCmd.Flags().StringVar(&genCfg.prefix, "prefix", "testdb_", "Database name prefix")
	generateCmd.Flags().BoolVar(&genCfg.nonInteractive, "non-interactive", false, "Never prompt; fail if any required value is missing")
}

func promptGenConfig() error {
	reader := bufio.NewReader(os.Stdin)

	if genCfg.host == "" {
		fmt.Print("Host: ")
		line, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("read host: %w", err)
		}
		genCfg.host = strings.TrimSpace(line)
	}
	if genCfg.port == 0 {
		fmt.Printf("Port [%d]: ", defaultPort)
		line, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("read port: %w", err)
		}
		line = strings.TrimSpace(line)
		if line == "" {
			genCfg.port = defaultPort
		} else if port, err := strconv.Atoi(line); err == nil {
			genCfg.port = port
		} else {
			genCfg.port = defaultPort
		}
	}
	if genCfg.user == "" {
		fmt.Print("Username: ")
		line, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("read user: %w", err)
		}
		genCfg.user = strings.TrimSpace(line)
	}
	if genCfg.password == "" {
		genCfg.password = promptPassword("Password: ")
	}

	return nil
}

func runGenerate(cmd *cobra.Command, args []string) error {
	// Resolve from env
	if genCfg.host == "" {
		genCfg.host = os.Getenv("GEN_HOST")
	}
	if genCfg.port == 0 {
		if p := os.Getenv("GEN_PORT"); p != "" {
			if port, err := strconv.Atoi(p); err == nil {
				genCfg.port = port
			}
		}
	}
	if genCfg.user == "" {
		genCfg.user = os.Getenv("GEN_USER")
	}
	if genCfg.password == "" {
		genCfg.password = os.Getenv("GEN_PGPASSWORD")
		if genCfg.password == "" {
			genCfg.password = os.Getenv("PGPASSWORD")
		}
	}

	// Interactive prompts
	if !genCfg.nonInteractive {
		if err := promptGenConfig(); err != nil {
			return err
		}
	}

	if genCfg.port == 0 {
		genCfg.port = defaultPort
	}

	if genCfg.host == "" || genCfg.user == "" {
		return fmt.Errorf("missing required config: set flags/env or run interactively (see --help)")
	}

	if genCfg.minSizeMB > genCfg.maxSizeMB {
		return fmt.Errorf("min-size (%d) cannot be greater than max-size (%d)", genCfg.minSizeMB, genCfg.maxSizeMB)
	}

	ctx := context.Background()

	// Verify connectivity
	connStr := buildConnStr(genCfg.host, genCfg.port, genCfg.user, genCfg.password, "postgres")
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return fmt.Errorf("connect to server: %w", err)
	}
	conn.Close(ctx)

	log("[%s] Connected to %s:%d", time.Now().Format(time.RFC3339), genCfg.host, genCfg.port)
	log("[%s] Generating %d databases (%s%03d..%s%03d), size %d-%d MB each, parallelism %d",
		time.Now().Format(time.RFC3339), genCfg.numDBs,
		genCfg.prefix, 1, genCfg.prefix, genCfg.numDBs,
		genCfg.minSizeMB, genCfg.maxSizeMB, genCfg.maxParallel)

	sem := make(chan struct{}, genCfg.maxParallel)
	var wg sync.WaitGroup
	var totalSize atomic.Int64
	var created atomic.Int32
	var errCount atomic.Int32

	for i := 1; i <= genCfg.numDBs; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			dbName := fmt.Sprintf("%s%03d", genCfg.prefix, idx)

			// Random target size
			targetMB, err := randIntRange(genCfg.minSizeMB, genCfg.maxSizeMB)
			if err != nil {
				log("[%s] ERROR %s: random size: %v", time.Now().Format(time.RFC3339), dbName, err)
				errCount.Add(1)
				return
			}
			targetBytes := int64(targetMB) * 1024 * 1024

			if err := generateDB(ctx, dbName, targetBytes); err != nil {
				log("[%s] ERROR %s: %v", time.Now().Format(time.RFC3339), dbName, err)
				errCount.Add(1)
				return
			}

			totalSize.Add(targetBytes)
			n := created.Add(1)
			log("[%s] Created %s (target: %dMB) [%d/%d]",
				time.Now().Format(time.RFC3339), dbName, targetMB, n, genCfg.numDBs)
		}(i)
	}
	wg.Wait()

	fmt.Println()
	log("[%s] Generation complete", time.Now().Format(time.RFC3339))
	fmt.Printf("  Databases created: %d / %d\n", created.Load(), genCfg.numDBs)
	if errCount.Load() > 0 {
		fmt.Printf("  Errors: %d\n", errCount.Load())
	}
	fmt.Printf("  Total target size: %s\n", formatSize(totalSize.Load()))

	return nil
}

func generateDB(ctx context.Context, dbName string, targetBytes int64) error {
	// Create the database
	adminConn, err := pgx.Connect(ctx, buildConnStr(genCfg.host, genCfg.port, genCfg.user, genCfg.password, "postgres"))
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	// CREATE DATABASE cannot run inside a transaction
	_, err = adminConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", pgIdentifier(dbName)))
	adminConn.Close(ctx)
	if err != nil {
		// Ignore "already exists" errors
		if !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("create database: %w", err)
		}
	}

	// Connect to the new database
	conn, err := pgx.Connect(ctx, buildConnStr(genCfg.host, genCfg.port, genCfg.user, genCfg.password, dbName))
	if err != nil {
		return fmt.Errorf("connect to %s: %w", dbName, err)
	}
	defer conn.Close(ctx)

	// Create table
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS data (
			id SERIAL PRIMARY KEY,
			payload TEXT,
			value DOUBLE PRECISION,
			created_at TIMESTAMP DEFAULT NOW()
		)`)
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	// Fill with data using COPY protocol
	const batchSize = 1000
	const checkInterval = 1000

	rowCount := 0
	for {
		// Build batch of rows for COPY
		rows := make([][]interface{}, batchSize)
		for i := range rows {
			payload, err := randomPayload()
			if err != nil {
				return fmt.Errorf("generate payload: %w", err)
			}
			val, err := randFloat()
			if err != nil {
				return fmt.Errorf("generate value: %w", err)
			}
			rows[i] = []interface{}{payload, val, time.Now()}
		}

		_, err := conn.CopyFrom(
			ctx,
			pgx.Identifier{"data"},
			[]string{"payload", "value", "created_at"},
			pgx.CopyFromRows(rows),
		)
		if err != nil {
			return fmt.Errorf("copy data: %w", err)
		}

		rowCount += batchSize

		// Check size periodically
		if rowCount%checkInterval == 0 {
			var size int64
			err := conn.QueryRow(ctx, "SELECT pg_database_size(current_database())").Scan(&size)
			if err != nil {
				return fmt.Errorf("check size: %w", err)
			}
			if size >= targetBytes {
				return nil
			}
		}
	}
}

// randomPayload returns ~1KB of random hex text.
func randomPayload() (string, error) {
	b := make([]byte, 512)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// randIntRange returns a random int in [min, max] inclusive.
func randIntRange(min, max int) (int, error) {
	if min == max {
		return min, nil
	}
	n, err := rand.Int(rand.Reader, big.NewInt(int64(max-min+1)))
	if err != nil {
		return 0, err
	}
	return min + int(n.Int64()), nil
}

// randFloat returns a random float64 in [0, 1000000).
func randFloat() (float64, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(1000000))
	if err != nil {
		return 0, err
	}
	return float64(n.Int64()) + float64(n.Int64()%100)/100.0, nil
}

// pgIdentifier quotes a PostgreSQL identifier to prevent SQL injection.
func pgIdentifier(name string) string {
	// Double any existing double quotes, then wrap in double quotes
	escaped := strings.ReplaceAll(name, `"`, `""`)
	return `"` + escaped + `"`
}
