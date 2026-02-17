package cmd

import (
	"bufio"
	"context"
	"crypto/rand"
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
	minTables      int
	maxTables      int
	totalSize      string // e.g. "10GB", "500MB"
	totalSizeBytes int64  // parsed from totalSize
	maxParallel    int
	prefix         string
	nonInteractive bool
}

var genCfg genConfig

var generateCmd = &cobra.Command{
	Use:   "generate",
	Short: "Generate fake databases with realistic schemas and data for testing",
	Long: `Creates multiple PostgreSQL databases with realistic multi-table schemas.
Each database gets 3-120 tables with foreign keys, indexes, and varied column types.
Supports fixed database count or total size target mode.`,
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
	generateCmd.Flags().IntVar(&genCfg.minTables, "min-tables", 3, "Minimum tables per database")
	generateCmd.Flags().IntVar(&genCfg.maxTables, "max-tables", 25, "Maximum tables per database")
	generateCmd.Flags().StringVar(&genCfg.totalSize, "total-size", "", "Total size target (e.g., 500MB, 10GB). Overrides --num-dbs")
	generateCmd.Flags().IntVarP(&genCfg.maxParallel, "parallel", "p", 10, "Max parallel DB creation jobs")
	generateCmd.Flags().StringVar(&genCfg.prefix, "prefix", "testdb_", "Database name prefix")
	generateCmd.Flags().BoolVar(&genCfg.nonInteractive, "non-interactive", false, "Never prompt; fail if any required value is missing")
}

// parseSize parses human-readable size strings like "500MB", "10GB", "1TB" into bytes.
func parseSize(input string) (int64, error) {
	input = strings.TrimSpace(strings.ToUpper(input))
	if input == "" {
		return 0, fmt.Errorf("empty size string")
	}

	// Check longer suffixes first to avoid "B" matching "GB", "MB", etc.
	type sizeSuffix struct {
		suffix string
		mult   int64
	}
	suffixes := []sizeSuffix{
		{"TB", 1024 * 1024 * 1024 * 1024},
		{"GB", 1024 * 1024 * 1024},
		{"MB", 1024 * 1024},
		{"KB", 1024},
		{"B", 1},
	}

	for _, s := range suffixes {
		if strings.HasSuffix(input, s.suffix) {
			numStr := strings.TrimSuffix(input, s.suffix)
			numStr = strings.TrimSpace(numStr)
			val, err := strconv.ParseFloat(numStr, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid number in size %q: %w", input, err)
			}
			return int64(val * float64(s.mult)), nil
		}
	}

	// Try plain number (assume GB for convenience)
	val, err := strconv.ParseFloat(input, 64)
	if err != nil {
		return 0, fmt.Errorf("unrecognized size format %q (use e.g. 500MB, 10GB)", input)
	}
	return int64(val * float64(1024*1024*1024)), nil
}

func promptGenConfig() error {
	reader := bufio.NewReader(os.Stdin)

	fmt.Println()
	fmt.Println("═══════════════════════════════════════════════")
	fmt.Println("  Database Generator Setup")
	fmt.Println("═══════════════════════════════════════════════")

	// ── Connection ──
	fmt.Println()
	fmt.Println("── Connection ──────────────────────────────────")

	if genCfg.host == "" {
		fmt.Print("  Host: ")
		line, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("read host: %w", err)
		}
		genCfg.host = strings.TrimSpace(line)
	}
	if genCfg.port == 0 {
		fmt.Printf("  Port [%d]: ", defaultPort)
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
		fmt.Print("  Username: ")
		line, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("read user: %w", err)
		}
		genCfg.user = strings.TrimSpace(line)
	}
	if genCfg.password == "" {
		genCfg.password = promptPassword("  Password: ")
	}

	// ── Schema Complexity ──
	fmt.Println()
	fmt.Println("── Schema Complexity ───────────────────────────")

	fmt.Printf("  Min tables per database [%d]: ", genCfg.minTables)
	if line, err := reader.ReadString('\n'); err == nil {
		if v := strings.TrimSpace(line); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				genCfg.minTables = n
			}
		}
	}

	fmt.Printf("  Max tables per database [%d]: ", genCfg.maxTables)
	if line, err := reader.ReadString('\n'); err == nil {
		if v := strings.TrimSpace(line); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				genCfg.maxTables = n
			}
		}
	}

	// ── Data Sizing ──
	fmt.Println()
	fmt.Println("── Data Sizing ─────────────────────────────────")
	fmt.Println("  How to control generation?")
	fmt.Println("    1) Fixed number of databases")
	fmt.Println("    2) Total size target (keeps creating until reached)")

	sizingMode := 1
	if genCfg.totalSize != "" {
		sizingMode = 2
	}
	fmt.Printf("  Choice [%d]: ", sizingMode)
	if line, err := reader.ReadString('\n'); err == nil {
		if v := strings.TrimSpace(line); v != "" {
			if n, err := strconv.Atoi(v); err == nil && (n == 1 || n == 2) {
				sizingMode = n
			}
		}
	}

	if sizingMode == 1 {
		genCfg.totalSize = ""
		genCfg.totalSizeBytes = 0

		fmt.Printf("  Number of databases [%d]: ", genCfg.numDBs)
		if line, err := reader.ReadString('\n'); err == nil {
			if v := strings.TrimSpace(line); v != "" {
				if n, err := strconv.Atoi(v); err == nil && n > 0 {
					genCfg.numDBs = n
				}
			}
		}
	} else {
		defaultTotal := genCfg.totalSize
		if defaultTotal == "" {
			defaultTotal = "1GB"
		}
		fmt.Printf("  Total target size [%s]: ", defaultTotal)
		if line, err := reader.ReadString('\n'); err == nil {
			if v := strings.TrimSpace(line); v != "" {
				genCfg.totalSize = v
			} else {
				genCfg.totalSize = defaultTotal
			}
		}
	}

	fmt.Printf("  Min database size in MB [%d]: ", genCfg.minSizeMB)
	if line, err := reader.ReadString('\n'); err == nil {
		if v := strings.TrimSpace(line); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				genCfg.minSizeMB = n
			}
		}
	}

	fmt.Printf("  Max database size in MB [%d]: ", genCfg.maxSizeMB)
	if line, err := reader.ReadString('\n'); err == nil {
		if v := strings.TrimSpace(line); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				genCfg.maxSizeMB = n
			}
		}
	}

	// ── Performance ──
	fmt.Println()
	fmt.Println("── Performance ─────────────────────────────────")

	fmt.Printf("  Database name prefix [%s]: ", genCfg.prefix)
	if line, err := reader.ReadString('\n'); err == nil {
		if v := strings.TrimSpace(line); v != "" {
			genCfg.prefix = v
		}
	}

	fmt.Printf("  Max parallel jobs [%d]: ", genCfg.maxParallel)
	if line, err := reader.ReadString('\n'); err == nil {
		if v := strings.TrimSpace(line); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				genCfg.maxParallel = n
			}
		}
	}

	// ── Summary ──
	fmt.Println()
	fmt.Println("═══════════════════════════════════════════════")
	fmt.Println("  Summary:")
	fmt.Printf("    Host:       %s:%d\n", genCfg.host, genCfg.port)
	fmt.Printf("    User:       %s\n", genCfg.user)
	if genCfg.totalSize != "" {
		fmt.Printf("    Mode:       Total size target: %s\n", genCfg.totalSize)
	} else {
		fmt.Printf("    Mode:       Fixed count: %d databases\n", genCfg.numDBs)
	}
	fmt.Printf("    Tables:     %d-%d per database\n", genCfg.minTables, genCfg.maxTables)
	fmt.Printf("    DB sizes:   %d-%d MB each\n", genCfg.minSizeMB, genCfg.maxSizeMB)
	fmt.Printf("    Prefix:     %s\n", genCfg.prefix)
	fmt.Printf("    Parallel:   %d\n", genCfg.maxParallel)
	fmt.Println()

	fmt.Print("  Proceed? [Y/n]: ")
	if line, err := reader.ReadString('\n'); err == nil {
		v := strings.TrimSpace(strings.ToLower(line))
		if v == "n" || v == "no" {
			return fmt.Errorf("aborted by user")
		}
	}
	fmt.Println("═══════════════════════════════════════════════")
	fmt.Println()

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

	if genCfg.minTables > genCfg.maxTables {
		return fmt.Errorf("min-tables (%d) cannot be greater than max-tables (%d)", genCfg.minTables, genCfg.maxTables)
	}

	// Parse total size if set
	if genCfg.totalSize != "" {
		parsed, err := parseSize(genCfg.totalSize)
		if err != nil {
			return fmt.Errorf("invalid --total-size: %w", err)
		}
		genCfg.totalSizeBytes = parsed
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

	if genCfg.totalSizeBytes > 0 {
		return runGenerateTotalSize(ctx)
	}
	return runGenerateFixedCount(ctx)
}

func runGenerateFixedCount(ctx context.Context) error {
	log("[%s] Generating %d databases (%s%03d..%s%03d), size %d-%d MB each, %d-%d tables, parallelism %d",
		time.Now().Format(time.RFC3339), genCfg.numDBs,
		genCfg.prefix, 1, genCfg.prefix, genCfg.numDBs,
		genCfg.minSizeMB, genCfg.maxSizeMB,
		genCfg.minTables, genCfg.maxTables,
		genCfg.maxParallel)

	pool := allTableTemplates()
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

			targetMB, err := randIntRange(genCfg.minSizeMB, genCfg.maxSizeMB)
			if err != nil {
				log("[%s] ERROR %s: random size: %v", time.Now().Format(time.RFC3339), dbName, err)
				errCount.Add(1)
				return
			}
			targetBytes := int64(targetMB) * 1024 * 1024

			tableCount, err := randIntRange(genCfg.minTables, genCfg.maxTables)
			if err != nil {
				tableCount = genCfg.minTables
			}

			tables := selectTables(pool, tableCount)

			if err := generateDB(ctx, dbName, targetBytes, tables); err != nil {
				log("[%s] ERROR %s: %v", time.Now().Format(time.RFC3339), dbName, err)
				errCount.Add(1)
				return
			}

			totalSize.Add(targetBytes)
			n := created.Add(1)
			log("[%s] Created %s (target: %dMB, tables: %d) [%d/%d]",
				time.Now().Format(time.RFC3339), dbName, targetMB, len(tables), n, genCfg.numDBs)
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

func runGenerateTotalSize(ctx context.Context) error {
	log("[%s] Generating databases until total size reaches %s, size %d-%d MB each, %d-%d tables, parallelism %d",
		time.Now().Format(time.RFC3339), formatSize(genCfg.totalSizeBytes),
		genCfg.minSizeMB, genCfg.maxSizeMB,
		genCfg.minTables, genCfg.maxTables,
		genCfg.maxParallel)

	pool := allTableTemplates()
	sem := make(chan struct{}, genCfg.maxParallel)
	var wg sync.WaitGroup
	var cumulativeSize atomic.Int64
	var created atomic.Int32
	var errCount atomic.Int32

	dbIndex := 0
	for cumulativeSize.Load() < genCfg.totalSizeBytes {
		remaining := genCfg.totalSizeBytes - cumulativeSize.Load()
		if remaining <= 0 {
			break
		}

		dbIndex++

		// Determine target for this DB
		targetMB, err := randIntRange(genCfg.minSizeMB, genCfg.maxSizeMB)
		if err != nil {
			targetMB = genCfg.minSizeMB
		}
		targetBytes := int64(targetMB) * 1024 * 1024

		// For the last DB, clamp to remaining
		if targetBytes > remaining {
			targetBytes = remaining
			if targetBytes < int64(genCfg.minSizeMB)*1024*1024 {
				targetBytes = int64(genCfg.minSizeMB) * 1024 * 1024
			}
		}

		tableCount, err := randIntRange(genCfg.minTables, genCfg.maxTables)
		if err != nil {
			tableCount = genCfg.minTables
		}
		tables := selectTables(pool, tableCount)

		// Pre-add the target to prevent over-shooting with parallel jobs
		cumulativeSize.Add(targetBytes)

		wg.Add(1)
		idx := dbIndex
		tbytes := targetBytes
		tbls := tables
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			dbName := fmt.Sprintf("%s%03d", genCfg.prefix, idx)

			if err := generateDB(ctx, dbName, tbytes, tbls); err != nil {
				log("[%s] ERROR %s: %v", time.Now().Format(time.RFC3339), dbName, err)
				errCount.Add(1)
				return
			}

			// Query actual size and adjust cumulative
			actualSize := queryDBSize(ctx, dbName)
			if actualSize > 0 {
				// Replace target estimate with actual
				cumulativeSize.Add(actualSize - tbytes)
			}

			n := created.Add(1)
			log("[%s] Created %s (target: %dMB, actual: %s, tables: %d) [%d created, %s / %s]",
				time.Now().Format(time.RFC3339), dbName,
				tbytes/(1024*1024), formatSize(actualSize),
				len(tbls), n,
				formatSize(cumulativeSize.Load()), formatSize(genCfg.totalSizeBytes))
		}()
	}
	wg.Wait()

	fmt.Println()
	log("[%s] Generation complete", time.Now().Format(time.RFC3339))
	fmt.Printf("  Databases created: %d\n", created.Load())
	if errCount.Load() > 0 {
		fmt.Printf("  Errors: %d\n", errCount.Load())
	}
	fmt.Printf("  Total cumulative size: %s (target: %s)\n",
		formatSize(cumulativeSize.Load()), formatSize(genCfg.totalSizeBytes))

	return nil
}

func queryDBSize(ctx context.Context, dbName string) int64 {
	conn, err := pgx.Connect(ctx, buildConnStr(genCfg.host, genCfg.port, genCfg.user, genCfg.password, dbName))
	if err != nil {
		return 0
	}
	defer conn.Close(ctx)

	var size int64
	err = conn.QueryRow(ctx, "SELECT pg_database_size(current_database())").Scan(&size)
	if err != nil {
		return 0
	}
	return size
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

// pgIdentifier quotes a PostgreSQL identifier to prevent SQL injection.
func pgIdentifier(name string) string {
	escaped := strings.ReplaceAll(name, `"`, `""`)
	return `"` + escaped + `"`
}

func generateDB(ctx context.Context, dbName string, targetBytes int64, tables []TableTemplate) error {
	// Create the database
	adminConn, err := pgx.Connect(ctx, buildConnStr(genCfg.host, genCfg.port, genCfg.user, genCfg.password, "postgres"))
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	_, err = adminConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", pgIdentifier(dbName)))
	adminConn.Close(ctx)
	if err != nil {
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

	// Create tables in topological order (tables are already sorted)
	for _, tmpl := range tables {
		ddlStatements := generateDDL(tmpl, dbName)
		for _, stmt := range ddlStatements {
			if _, err := conn.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("execute DDL for %s: %w", tmpl.Name, err)
			}
		}
	}

	// Truncate all tables before populating (handles re-runs on existing databases).
	// Reverse order to respect FK constraints, then cascade to be safe.
	for i := len(tables) - 1; i >= 0; i-- {
		_, _ = conn.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s CASCADE", pgIdentifier(tables[i].Name)))
	}

	// Populate tables
	if err := populateTables(ctx, conn, tables, targetBytes); err != nil {
		return fmt.Errorf("populate tables: %w", err)
	}

	return nil
}
