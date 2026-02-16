package cmd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

const defaultPort = 5432
const maxStderrBytes = 4096

type config struct {
	sourceHost     string
	sourcePort     int
	sourceUser     string
	sourcePassword string
	destHost       string
	destPort       int
	destUser       string
	destPassword   string
	dumpDir        string
	maxParallel    int
	noPause        bool
	nonInteractive bool
	summaryFile    string
	keepDumps      bool
	retries        int
	autoParallel   bool
}

type tableVerification struct {
	TableName  string `json:"table_name"`
	SourceRows int64  `json:"source_rows"`
	DestRows   int64  `json:"dest_rows"`
	Match      bool   `json:"match"`
}

type dbResult struct {
	Database         string              `json:"database"`
	Status           string              `json:"status"` // "success", "failed", "verify_mismatch"
	StartTime        time.Time           `json:"start_time"`
	EndTime          time.Time           `json:"end_time"`
	DurationSecs     float64             `json:"duration_secs"`
	DumpSizeBytes    int64               `json:"dump_size_bytes"`
	SourceTableCount int                 `json:"source_table_count"`
	DestTableCount   int                 `json:"dest_table_count"`
	Tables           []tableVerification `json:"tables,omitempty"`
	ErrorMessage     string              `json:"error_message,omitempty"`
	Stderr           string              `json:"stderr,omitempty"`
}

type migrationSummary struct {
	StartTime    time.Time  `json:"start_time"`
	EndTime      time.Time  `json:"end_time"`
	DurationSecs float64    `json:"duration_secs"`
	TotalDBs     int        `json:"total_dbs"`
	SucceededDBs int        `json:"succeeded_dbs"`
	FailedDBs    int        `json:"failed_dbs"`
	Results      []dbResult `json:"results"`
}

var cfg config

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Run the full migration: dump from source, restore to destination",
	Long: `Runs the complete migration workflow per database:
1. Fetch database list from source
2. Pause for you to put app in maintenance mode
3. For each database in parallel: dump → createdb → restore → verify → cleanup
4. Write JSON + CSV summary
5. Print final instructions`,
	RunE: runMigrate,
}

func init() {
	rootCmd.AddCommand(migrateCmd)

	migrateCmd.Flags().StringVarP(&cfg.sourceHost, "source-host", "s", "", "Source PostgreSQL host (or SOURCE_HOST env)")
	migrateCmd.Flags().IntVar(&cfg.sourcePort, "source-port", 0, "Source PostgreSQL port (default 5432, or SOURCE_PORT env)")
	migrateCmd.Flags().StringVar(&cfg.sourceUser, "source-user", "", "Source PostgreSQL username (or SOURCE_USER env)")
	migrateCmd.Flags().StringVarP(&cfg.destHost, "dest-host", "d", "", "Destination PostgreSQL host (or DEST_HOST env)")
	migrateCmd.Flags().IntVar(&cfg.destPort, "dest-port", 0, "Destination PostgreSQL port (default 5432, or DEST_PORT env)")
	migrateCmd.Flags().StringVar(&cfg.destUser, "dest-user", "", "Destination PostgreSQL username (or DEST_USER env)")
	migrateCmd.Flags().StringVarP(&cfg.dumpDir, "dump-dir", "o", "/tmp/pg_migration", "Directory for dump files")
	migrateCmd.Flags().IntVarP(&cfg.maxParallel, "parallel", "p", 4, "Max parallel migration jobs")
	migrateCmd.Flags().BoolVar(&cfg.noPause, "no-pause", false, "Skip maintenance mode pause (for scripting)")
	migrateCmd.Flags().BoolVar(&cfg.nonInteractive, "non-interactive", false, "Never prompt; fail if any required value is missing")
	migrateCmd.Flags().StringVar(&cfg.summaryFile, "summary-file", "migration_summary", "Base name for summary files (.json/.csv appended)")
	migrateCmd.Flags().BoolVar(&cfg.keepDumps, "keep-dumps", false, "Keep dump files after successful restore")
	migrateCmd.Flags().IntVar(&cfg.retries, "retries", 3, "Max retry attempts for transient errors")
	migrateCmd.Flags().BoolVar(&cfg.autoParallel, "auto-parallel", false, "Auto-tune --parallel based on CPUs and DB connection limits")
}

// --- Config resolution ---

func promptForConfig() error {
	reader := bufio.NewReader(os.Stdin)

	if cfg.sourceHost == "" {
		fmt.Print("Source host: ")
		line, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("read source host: %w", err)
		}
		cfg.sourceHost = strings.TrimSpace(line)
	}
	if cfg.sourcePort == 0 {
		fmt.Printf("Source port [%d]: ", defaultPort)
		line, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("read source port: %w", err)
		}
		line = strings.TrimSpace(line)
		if line == "" {
			cfg.sourcePort = defaultPort
		} else if port, err := strconv.Atoi(line); err == nil {
			cfg.sourcePort = port
		} else {
			cfg.sourcePort = defaultPort
		}
	}
	if cfg.sourceUser == "" {
		fmt.Print("Source username: ")
		line, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("read source user: %w", err)
		}
		cfg.sourceUser = strings.TrimSpace(line)
	}
	if cfg.sourcePassword == "" {
		cfg.sourcePassword = promptPassword("Source password: ")
	}

	fmt.Println()
	if cfg.destHost == "" {
		fmt.Print("Destination host: ")
		line, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("read dest host: %w", err)
		}
		cfg.destHost = strings.TrimSpace(line)
	}
	if cfg.destPort == 0 {
		fmt.Printf("Destination port [%d]: ", defaultPort)
		line, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("read dest port: %w", err)
		}
		line = strings.TrimSpace(line)
		if line == "" {
			cfg.destPort = defaultPort
		} else if port, err := strconv.Atoi(line); err == nil {
			cfg.destPort = port
		} else {
			cfg.destPort = defaultPort
		}
	}
	if cfg.destUser == "" {
		fmt.Print("Destination username: ")
		line, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("read dest user: %w", err)
		}
		cfg.destUser = strings.TrimSpace(line)
	}
	if cfg.destPassword == "" {
		cfg.destPassword = promptPassword("Destination password: ")
	}

	return nil
}

func promptPassword(prompt string) string {
	fmt.Print(prompt)
	pass, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Println()
	if err != nil {
		reader := bufio.NewReader(os.Stdin)
		line, _ := reader.ReadString('\n')
		return strings.TrimSpace(line)
	}
	return string(pass)
}

// --- Main orchestrator ---

var requiredTools = []string{"pg_dump", "pg_restore", "createdb"}

func checkRequiredTools() error {
	var missing []string
	for _, tool := range requiredTools {
		if _, err := exec.LookPath(tool); err != nil {
			missing = append(missing, tool)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("required PostgreSQL client tools not found: %s\n\n"+
		"Install them for your platform:\n"+
		"  macOS (Homebrew):   brew install libpq && brew link --force libpq\n"+
		"  macOS (Postgres.app): Add to PATH: export PATH=\"/Applications/Postgres.app/Contents/Versions/latest/bin:$PATH\"\n"+
		"  Debian/Ubuntu:      sudo apt-get install postgresql-client\n"+
		"  RHEL/Fedora:        sudo dnf install postgresql\n"+
		"  Amazon Linux:       sudo yum install postgresql15\n"+
		"  Alpine:             apk add postgresql-client\n"+
		"  Windows (Chocolatey): choco install postgresql\n"+
		"  Docker:             docker run --rm -it postgres:16 pg_dump --version",
		strings.Join(missing, ", "))
}

func runMigrate(cmd *cobra.Command, args []string) error {
	if err := checkRequiredTools(); err != nil {
		return err
	}

	resolveEnvConfig()

	if !cfg.nonInteractive {
		if err := promptForConfig(); err != nil {
			return err
		}
	}

	if cfg.sourcePort == 0 {
		cfg.sourcePort = defaultPort
	}
	if cfg.destPort == 0 {
		cfg.destPort = defaultPort
	}

	if cfg.sourceHost == "" || cfg.sourceUser == "" || cfg.destHost == "" || cfg.destUser == "" {
		return fmt.Errorf("missing required config: set flags/env or run interactively (see --help)")
	}

	ctx := context.Background()
	migrationStart := time.Now()

	if err := os.MkdirAll(cfg.dumpDir, 0755); err != nil {
		return fmt.Errorf("create dump dir: %w", err)
	}
	log("[%s] Using dump dir: %s", time.Now().Format(time.RFC3339), cfg.dumpDir)

	// Connect to source and list databases
	sourceConnStr := buildConnStr(cfg.sourceHost, cfg.sourcePort, cfg.sourceUser, cfg.sourcePassword, "postgres")
	conn, err := connectWithRetry(ctx, sourceConnStr)
	if err != nil {
		return fmt.Errorf("connect to source: %w", err)
	}
	defer conn.Close(ctx)

	log("[%s] Fetching database list...", time.Now().Format(time.RFC3339))
	dbs, err := listDatabases(ctx, conn)
	if err != nil {
		return fmt.Errorf("list databases: %w", err)
	}
	log("[%s] Found %d databases to migrate", time.Now().Format(time.RFC3339), len(dbs))

	// Close the listing connection — each goroutine opens its own
	conn.Close(ctx)

	// Auto-tune parallelism if requested
	if cfg.autoParallel {
		parallelFlagSet := cmd.Flags().Changed("parallel")
		tuned, err := autoTuneParallel(ctx)
		if err != nil {
			return fmt.Errorf("auto-tune parallelism: %w", err)
		}
		if parallelFlagSet && cfg.maxParallel < tuned {
			log("[%s] Auto-tuned to %d, but capped by --parallel=%d", time.Now().Format(time.RFC3339), tuned, cfg.maxParallel)
		} else {
			cfg.maxParallel = tuned
		}
	}

	// Maintenance pause
	if !cfg.noPause {
		fmt.Println()
		fmt.Println("============================================")
		fmt.Println(" START DOWNTIME NOW")
		fmt.Println(" Put your app in maintenance mode, then press Enter")
		fmt.Println("============================================")
		reader := bufio.NewReader(os.Stdin)
		if _, err := reader.ReadString('\n'); err != nil {
			return fmt.Errorf("read stdin: %w", err)
		}
	}

	// Per-DB pipeline
	results := make([]dbResult, len(dbs))
	sem := make(chan struct{}, cfg.maxParallel)
	var wg sync.WaitGroup

	for i, db := range dbs {
		wg.Add(1)
		go func(idx int, database string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			results[idx] = migrateDatabase(ctx, database)
		}(i, db)
	}
	wg.Wait()

	// Build summary
	migrationEnd := time.Now()
	summary := migrationSummary{
		StartTime:    migrationStart,
		EndTime:      migrationEnd,
		DurationSecs: migrationEnd.Sub(migrationStart).Seconds(),
		TotalDBs:     len(dbs),
		Results:      results,
	}
	for _, r := range results {
		if r.Status == "success" {
			summary.SucceededDBs++
		} else {
			summary.FailedDBs++
		}
	}

	// Console summary
	fmt.Println()
	fmt.Println("============================================")
	fmt.Println(" MIGRATION SUMMARY")
	fmt.Println("============================================")
	fmt.Printf("Total databases:  %d\n", summary.TotalDBs)
	fmt.Printf("Succeeded:        %d\n", summary.SucceededDBs)
	fmt.Printf("Failed:           %d\n", summary.FailedDBs)
	fmt.Printf("Duration:         %.1fs\n", summary.DurationSecs)
	fmt.Println()

	for _, r := range results {
		icon := "OK"
		if r.Status == "failed" {
			icon = "FAIL"
		} else if r.Status == "verify_mismatch" {
			icon = "MISMATCH"
		}
		line := fmt.Sprintf("  [%s] %s (%.1fs", icon, r.Database, r.DurationSecs)
		if r.DumpSizeBytes > 0 {
			line += fmt.Sprintf(", dump %s", formatSize(r.DumpSizeBytes))
		}
		line += ")"
		if r.ErrorMessage != "" {
			line += fmt.Sprintf(" — %s", r.ErrorMessage)
		}
		fmt.Println(line)
	}

	// Write summary files
	jsonPath := filepath.Join(cfg.dumpDir, cfg.summaryFile+".json")
	csvPath := filepath.Join(cfg.dumpDir, cfg.summaryFile+".csv")
	if err := writeSummaryJSON(summary, jsonPath); err != nil {
		log("[%s] Warning: failed to write JSON summary: %v", time.Now().Format(time.RFC3339), err)
	} else {
		fmt.Printf("\nJSON summary: %s\n", jsonPath)
	}
	if err := writeSummaryCSV(summary, csvPath); err != nil {
		log("[%s] Warning: failed to write CSV summary: %v", time.Now().Format(time.RFC3339), err)
	} else {
		fmt.Printf("CSV summary:  %s\n", csvPath)
	}

	// Final instructions
	fmt.Println()
	fmt.Println("============================================")
	fmt.Println(" UPDATE YOUR CONNECTION STRINGS NOW")
	fmt.Printf(" Point everything to destination: %s\n", cfg.destHost)
	fmt.Println(" Then bring the app out of maintenance mode")
	fmt.Println("============================================")
	fmt.Println()

	if summary.FailedDBs > 0 {
		return fmt.Errorf("%d of %d databases failed migration", summary.FailedDBs, summary.TotalDBs)
	}
	return nil
}

func resolveEnvConfig() {
	if cfg.sourceHost == "" {
		cfg.sourceHost = os.Getenv("SOURCE_HOST")
	}
	if cfg.sourcePort == 0 {
		if p := os.Getenv("SOURCE_PORT"); p != "" {
			if port, err := strconv.Atoi(p); err == nil {
				cfg.sourcePort = port
			}
		}
	}
	if cfg.sourceUser == "" {
		cfg.sourceUser = os.Getenv("SOURCE_USER")
	}
	if cfg.sourcePassword == "" {
		cfg.sourcePassword = os.Getenv("SOURCE_PGPASSWORD")
		if cfg.sourcePassword == "" {
			cfg.sourcePassword = os.Getenv("PGPASSWORD")
		}
	}
	if cfg.destHost == "" {
		cfg.destHost = os.Getenv("DEST_HOST")
	}
	if cfg.destPort == 0 {
		if p := os.Getenv("DEST_PORT"); p != "" {
			if port, err := strconv.Atoi(p); err == nil {
				cfg.destPort = port
			}
		}
	}
	if cfg.destUser == "" {
		cfg.destUser = os.Getenv("DEST_USER")
	}
	if cfg.destPassword == "" {
		cfg.destPassword = os.Getenv("DEST_PGPASSWORD")
		if cfg.destPassword == "" {
			cfg.destPassword = os.Getenv("PGPASSWORD")
		}
	}
}

// --- Per-DB pipeline ---

func migrateDatabase(ctx context.Context, database string) (result dbResult) {
	result = dbResult{
		Database:  database,
		StartTime: time.Now(),
		Status:    "failed",
	}
	defer func() {
		result.EndTime = time.Now()
		result.DurationSecs = result.EndTime.Sub(result.StartTime).Seconds()
	}()

	dumpPath := filepath.Join(cfg.dumpDir, database+".dump")

	// Dump
	log("[%s] Dumping: %s", time.Now().Format(time.RFC3339), database)
	stderr, err := runPgDump(database)
	if err != nil {
		result.ErrorMessage = fmt.Sprintf("dump failed: %v", err)
		result.Stderr = truncateStderr(stderr)
		removeDumpFile(database)
		return result
	}

	// Record dump size
	if info, err := os.Stat(dumpPath); err == nil {
		result.DumpSizeBytes = info.Size()
	}

	// Create destination DB
	log("[%s] Creating dest DB: %s", time.Now().Format(time.RFC3339), database)
	stderr, err = runCreatedb(database)
	if err != nil {
		// Ignore createdb errors — DB may already exist
		_ = stderr
	}

	// Restore
	log("[%s] Restoring: %s", time.Now().Format(time.RFC3339), database)
	stderr, err = runPgRestore(database)
	if err != nil {
		// pg_restore exit code 1 with only WARNINGs is acceptable
		if !isRestoreWarningOnly(stderr) {
			result.ErrorMessage = fmt.Sprintf("restore failed: %v", err)
			result.Stderr = truncateStderr(stderr)
			dropDestDatabase(ctx, database)
			removeDumpFile(database)
			return result
		}
	}

	// Verify
	log("[%s] Verifying: %s", time.Now().Format(time.RFC3339), database)
	srcCount, dstCount, tables, verifyErr := verifyDatabase(ctx, database)
	result.SourceTableCount = srcCount
	result.DestTableCount = dstCount
	result.Tables = tables

	if verifyErr != nil {
		result.Status = "verify_mismatch"
		result.ErrorMessage = fmt.Sprintf("verification: %v", verifyErr)
		// Do NOT drop — data exists, user investigates
	} else {
		result.Status = "success"
	}

	// Cleanup dump file
	if !cfg.keepDumps {
		removeDumpFile(database)
	}

	log("[%s] Done: %s [%s]", time.Now().Format(time.RFC3339), database, result.Status)
	return result
}

// --- External tools ---

func runPgDump(database string) (string, error) {
	var lastStderr string
	err := withRetry(context.Background(), cfg.retries, "pg_dump "+database, func() error {
		path := filepath.Join(cfg.dumpDir, database+".dump")
		var stderrBuf bytes.Buffer
		c := exec.Command("pg_dump",
			"-h", cfg.sourceHost,
			"-p", strconv.Itoa(cfg.sourcePort),
			"-U", cfg.sourceUser,
			"-d", database,
			"--no-owner", "--no-privileges", "--no-acl",
			"-Fc", "-Z3", "-f", path)
		c.Env = append(os.Environ(), "PGPASSWORD="+cfg.sourcePassword)
		c.Stderr = &stderrBuf
		err := c.Run()
		lastStderr = stderrBuf.String()
		if err != nil && isTransientError(fmt.Errorf("%v: %s", err, lastStderr)) {
			return err // retryable
		}
		return err
	})
	return lastStderr, err
}

func runCreatedb(database string) (string, error) {
	var stderrBuf bytes.Buffer
	c := exec.Command("createdb",
		"-h", cfg.destHost,
		"-p", strconv.Itoa(cfg.destPort),
		"-U", cfg.destUser,
		database)
	c.Env = append(os.Environ(), "PGPASSWORD="+cfg.destPassword)
	c.Stderr = &stderrBuf
	err := c.Run()
	return stderrBuf.String(), err
}

func runPgRestore(database string) (string, error) {
	var lastStderr string
	err := withRetry(context.Background(), cfg.retries, "pg_restore "+database, func() error {
		path := filepath.Join(cfg.dumpDir, database+".dump")
		var stderrBuf bytes.Buffer
		c := exec.Command("pg_restore",
			"-h", cfg.destHost,
			"-p", strconv.Itoa(cfg.destPort),
			"-U", cfg.destUser,
			"-d", database,
			"--no-owner", "--no-privileges", "--no-acl",
			"-j", "4", path)
		c.Env = append(os.Environ(), "PGPASSWORD="+cfg.destPassword)
		c.Stderr = &stderrBuf
		err := c.Run()
		lastStderr = stderrBuf.String()
		if err != nil && isTransientError(fmt.Errorf("%v: %s", err, lastStderr)) {
			return err
		}
		return err
	})
	return lastStderr, err
}

func isRestoreWarningOnly(stderr string) bool {
	if stderr == "" {
		return true
	}
	for _, line := range strings.Split(stderr, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "WARNING:") && !strings.HasPrefix(line, "pg_restore: warning:") {
			return false
		}
	}
	return true
}

// --- Verification ---

func verifyDatabase(ctx context.Context, database string) (int, int, []tableVerification, error) {
	srcCounts, err := getTableRowCounts(ctx, cfg.sourceHost, cfg.sourcePort, cfg.sourceUser, cfg.sourcePassword, database)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("source table counts: %w", err)
	}

	dstCounts, err := getTableRowCounts(ctx, cfg.destHost, cfg.destPort, cfg.destUser, cfg.destPassword, database)
	if err != nil {
		return len(srcCounts), 0, nil, fmt.Errorf("dest table counts: %w", err)
	}

	var tables []tableVerification
	var mismatches []string

	// Check all source tables
	for table, srcRows := range srcCounts {
		dstRows, exists := dstCounts[table]
		match := exists && srcRows == dstRows
		tables = append(tables, tableVerification{
			TableName:  table,
			SourceRows: srcRows,
			DestRows:   dstRows,
			Match:      match,
		})
		if !match {
			mismatches = append(mismatches, fmt.Sprintf("%s (src=%d, dst=%d)", table, srcRows, dstRows))
		}
	}

	// Check for extra tables in dest
	for table, dstRows := range dstCounts {
		if _, exists := srcCounts[table]; !exists {
			tables = append(tables, tableVerification{
				TableName:  table,
				SourceRows: 0,
				DestRows:   dstRows,
				Match:      false,
			})
			mismatches = append(mismatches, fmt.Sprintf("%s (extra in dest, rows=%d)", table, dstRows))
		}
	}

	if len(mismatches) > 0 {
		return len(srcCounts), len(dstCounts), tables, fmt.Errorf("mismatched tables: %s", strings.Join(mismatches, "; "))
	}

	return len(srcCounts), len(dstCounts), tables, nil
}

func getTableRowCounts(ctx context.Context, host string, port int, user, password, database string) (map[string]int64, error) {
	connStr := buildConnStr(host, port, user, password, database)
	conn, err := connectWithRetry(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)

	// Get all user tables
	rows, err := conn.Query(ctx, `
		SELECT schemaname, tablename
		FROM pg_tables
		WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
		ORDER BY schemaname, tablename`)
	if err != nil {
		return nil, fmt.Errorf("query tables: %w", err)
	}
	defer rows.Close()

	type tableRef struct {
		schema, table string
	}
	var tableRefs []tableRef
	for rows.Next() {
		var schema, table string
		if err := rows.Scan(&schema, &table); err != nil {
			return nil, fmt.Errorf("scan table: %w", err)
		}
		tableRefs = append(tableRefs, tableRef{schema, table})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	counts := make(map[string]int64, len(tableRefs))
	for _, ref := range tableRefs {
		qualifiedName := pgx.Identifier{ref.schema, ref.table}.Sanitize()
		var count int64
		err := conn.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", qualifiedName)).Scan(&count)
		if err != nil {
			return nil, fmt.Errorf("count %s: %w", qualifiedName, err)
		}
		counts[ref.schema+"."+ref.table] = count
	}

	return counts, nil
}

// --- Retry and reconnect ---

func withRetry(ctx context.Context, maxAttempts int, label string, fn func() error) error {
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if !isTransientError(lastErr) {
			return lastErr
		}
		if attempt < maxAttempts {
			backoff := time.Duration(math.Pow(2, float64(attempt))) * time.Second
			log("[%s] Transient error on %s (attempt %d/%d), retrying in %v: %v",
				time.Now().Format(time.RFC3339), label, attempt, maxAttempts, backoff, lastErr)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return fmt.Errorf("after %d attempts: %w", maxAttempts, lastErr)
}

func isTransientError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	transientPatterns := []string{
		"connection refused",
		"connection reset",
		"connection timed out",
		"broken pipe",
		"unexpected eof",
		"i/o timeout",
		"server closed the connection unexpectedly",
		"could not connect to server",
		"the database system is starting up",
		"too many connections",
	}
	for _, pattern := range transientPatterns {
		if strings.Contains(msg, pattern) {
			return true
		}
	}
	return false
}

func connectWithRetry(ctx context.Context, connStr string) (*pgx.Conn, error) {
	var conn *pgx.Conn
	err := withRetry(ctx, cfg.retries, "connect", func() error {
		var err error
		conn, err = pgx.Connect(ctx, connStr)
		return err
	})
	return conn, err
}

// --- Cleanup ---

func dropDestDatabase(ctx context.Context, database string) {
	connStr := buildConnStr(cfg.destHost, cfg.destPort, cfg.destUser, cfg.destPassword, "postgres")
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		log("[%s] Warning: could not connect to drop %s: %v", time.Now().Format(time.RFC3339), database, err)
		return
	}
	defer conn.Close(ctx)

	// Terminate active connections to the database
	_, _ = conn.Exec(ctx, fmt.Sprintf(
		"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '%s' AND pid <> pg_backend_pid()",
		strings.ReplaceAll(database, "'", "''")))

	_, err = conn.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s",
		pgx.Identifier{database}.Sanitize()))
	if err != nil {
		log("[%s] Warning: failed to drop %s: %v", time.Now().Format(time.RFC3339), database, err)
	}
}

func removeDumpFile(database string) {
	path := filepath.Join(cfg.dumpDir, database+".dump")
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		log("[%s] Warning: failed to remove %s: %v", time.Now().Format(time.RFC3339), path, err)
	}
}

// --- Summary output ---

func writeSummaryJSON(summary migrationSummary, path string) error {
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func writeSummaryCSV(summary migrationSummary, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	header := []string{
		"database", "status", "duration_secs", "dump_size_bytes",
		"source_table_count", "dest_table_count", "tables_match", "rows_match", "error_message",
	}
	if err := w.Write(header); err != nil {
		return err
	}

	for _, r := range summary.Results {
		tablesMatch := r.SourceTableCount == r.DestTableCount && r.SourceTableCount > 0
		rowsMatch := true
		for _, t := range r.Tables {
			if !t.Match {
				rowsMatch = false
				break
			}
		}
		// If no tables were checked, rows_match is vacuously true only if successful
		if len(r.Tables) == 0 && r.Status == "failed" {
			rowsMatch = false
		}

		row := []string{
			r.Database,
			r.Status,
			fmt.Sprintf("%.1f", r.DurationSecs),
			strconv.FormatInt(r.DumpSizeBytes, 10),
			strconv.Itoa(r.SourceTableCount),
			strconv.Itoa(r.DestTableCount),
			strconv.FormatBool(tablesMatch),
			strconv.FormatBool(rowsMatch),
			r.ErrorMessage,
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}

	return nil
}

// --- Auto-tune parallelism ---

func queryConnectionLimits(ctx context.Context, connStr string) (maxConns int, activeConns int, err error) {
	conn, err := connectWithRetry(ctx, connStr)
	if err != nil {
		return 0, 0, fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)

	var maxConnsStr string
	if err := conn.QueryRow(ctx, "SHOW max_connections").Scan(&maxConnsStr); err != nil {
		return 0, 0, fmt.Errorf("query max_connections: %w", err)
	}
	maxConns, err = strconv.Atoi(maxConnsStr)
	if err != nil {
		return 0, 0, fmt.Errorf("parse max_connections %q: %w", maxConnsStr, err)
	}

	if err := conn.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity").Scan(&activeConns); err != nil {
		return 0, 0, fmt.Errorf("query active connections: %w", err)
	}

	return maxConns, activeConns, nil
}

func autoTuneParallel(ctx context.Context) (int, error) {
	log("[%s] Auto-tuning parallelism...", time.Now().Format(time.RFC3339))

	cpuLimit := runtime.NumCPU()

	sourceConnStr := buildConnStr(cfg.sourceHost, cfg.sourcePort, cfg.sourceUser, cfg.sourcePassword, "postgres")
	srcMax, srcActive, err := queryConnectionLimits(ctx, sourceConnStr)
	if err != nil {
		return 0, fmt.Errorf("source connection limits: %w", err)
	}
	srcUsable := srcMax - srcActive - srcMax/5 // reserve 20% headroom
	if srcUsable < 1 {
		srcUsable = 1
	}
	srcLimit := srcUsable / 1 // 1 conn per slot for pg_dump

	destConnStr := buildConnStr(cfg.destHost, cfg.destPort, cfg.destUser, cfg.destPassword, "postgres")
	dstMax, dstActive, err := queryConnectionLimits(ctx, destConnStr)
	if err != nil {
		return 0, fmt.Errorf("dest connection limits: %w", err)
	}
	dstUsable := dstMax - dstActive - dstMax/5 // reserve 20% headroom
	if dstUsable < 1 {
		dstUsable = 1
	}
	dstLimit := dstUsable / 5 // pg_restore -j 4 + control conn

	optimal := cpuLimit
	bottleneck := "local CPUs"
	if srcLimit < optimal {
		optimal = srcLimit
		bottleneck = "source connections"
	}
	if dstLimit < optimal {
		optimal = dstLimit
		bottleneck = "dest connections"
	}
	if optimal < 1 {
		optimal = 1
	}
	if optimal > 32 {
		optimal = 32
	}

	log("  Local CPUs:             %d", cpuLimit)
	log("  Source max_connections:  %d (%d in use, %d usable → %d slots)", srcMax, srcActive, srcUsable, srcLimit)
	log("  Dest max_connections:   %d (%d in use, %d usable → %d slots)", dstMax, dstActive, dstUsable, dstLimit)
	log("  Optimal --parallel:     %d (bottleneck: %s)", optimal, bottleneck)

	return optimal, nil
}

// --- Utility ---

func buildConnStr(host string, port int, user, password, db string) string {
	hostPort := host
	if port > 0 {
		hostPort = fmt.Sprintf("%s:%d", host, port)
	}
	u := &url.URL{
		Scheme:   "postgres",
		Host:     hostPort,
		Path:     "/" + db,
		RawQuery: "sslmode=require",
	}
	if password != "" {
		u.User = url.UserPassword(user, password)
	} else {
		u.User = url.User(user)
	}
	return u.String()
}

func listDatabases(ctx context.Context, conn *pgx.Conn) ([]string, error) {
	rows, err := conn.Query(ctx, `
		SELECT datname FROM pg_database
		WHERE datname NOT IN ('template0','template1','postgres')
		ORDER BY datname`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var dbs []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		dbs = append(dbs, name)
	}
	return dbs, rows.Err()
}

func truncateStderr(s string) string {
	if len(s) > maxStderrBytes {
		return s[:maxStderrBytes] + "\n... (truncated)"
	}
	return s
}

func formatSize(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for u := n / unit; u >= unit; u /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "KMGTPE"[exp])
}

func log(format string, args ...interface{}) {
	fmt.Printf(format+"\n", args...)
}
