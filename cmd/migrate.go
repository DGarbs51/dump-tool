package cmd

import (
	"bufio"
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

const defaultPort = 5432

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
}

var cfg config

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Run the full migration: dump from source, restore to destination",
	Long: `Runs the complete migration workflow:
1. Fetch database list from source
2. Pause for you to put app in maintenance mode
3. Dump all databases in parallel
4. Restore all databases to destination in parallel
5. Verify counts and spot-check row counts`,
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
	migrateCmd.Flags().IntVarP(&cfg.maxParallel, "parallel", "p", 10, "Max parallel dump/restore jobs")
	migrateCmd.Flags().BoolVar(&cfg.noPause, "no-pause", false, "Skip maintenance mode pause (for scripting)")
	migrateCmd.Flags().BoolVar(&cfg.nonInteractive, "non-interactive", false, "Never prompt; fail if any required value is missing (for scripting)")
}

func promptForConfig() error {
	reader := bufio.NewReader(os.Stdin)

	// Source database
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

	// Destination database
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
		// Fallback when stdin isn't a TTY (e.g. piped)
		reader := bufio.NewReader(os.Stdin)
		line, _ := reader.ReadString('\n')
		return strings.TrimSpace(line)
	}
	return string(pass)
}

func runMigrate(cmd *cobra.Command, args []string) error {
	// Resolve config from env if not set
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
			cfg.sourcePassword = os.Getenv("PGPASSWORD") // fallback for same password
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
			cfg.destPassword = os.Getenv("PGPASSWORD") // fallback for same password
		}
	}

	// Apply port defaults and prompt for missing values if interactive
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

	// Create dump dir
	if err := os.MkdirAll(cfg.dumpDir, 0755); err != nil {
		return fmt.Errorf("create dump dir: %w", err)
	}
	log("[%s] Using dump dir: %s", time.Now().Format(time.RFC3339), cfg.dumpDir)

	// Fetch database list
	sourceConnStr := buildConnStr(cfg.sourceHost, cfg.sourcePort, cfg.sourceUser, cfg.sourcePassword, "postgres")
	conn, err := pgx.Connect(ctx, sourceConnStr)
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

	// Step 1: Pause for maintenance mode
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

	// Step 2: Dump all databases in parallel
	log("[%s] Dumping all databases...", time.Now().Format(time.RFC3339))
	sem := make(chan struct{}, cfg.maxParallel)
	var wg sync.WaitGroup
	for _, db := range dbs {
		wg.Add(1)
		go func(database string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := runPgDump(database); err != nil {
				log("[%s] Dump error for %s: %v", time.Now().Format(time.RFC3339), database, err)
			} else {
				log("[%s] Dumped: %s", time.Now().Format(time.RFC3339), database)
			}
		}(db)
	}
	wg.Wait()
	log("[%s] All dumps complete", time.Now().Format(time.RFC3339))

	// Step 3: Create databases and restore in parallel
	log("[%s] Restoring all databases...", time.Now().Format(time.RFC3339))
	for _, db := range dbs {
		wg.Add(1)
		go func(database string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			_ = runCreatedb(database) // ignore error (db may exist)
			if err := runPgRestore(database); err != nil {
				log("[%s] Restore error for %s: %v", time.Now().Format(time.RFC3339), database, err)
			} else {
				log("[%s] Restored: %s", time.Now().Format(time.RFC3339), database)
			}
		}(db)
	}
	wg.Wait()
	log("[%s] All restores complete", time.Now().Format(time.RFC3339))

	// Step 4: Verify
	log("[%s] Verifying database counts...", time.Now().Format(time.RFC3339))
	destConnStr := buildConnStr(cfg.destHost, cfg.destPort, cfg.destUser, cfg.destPassword, "postgres")
	destConn, err := pgx.Connect(ctx, destConnStr)
	if err != nil {
		return fmt.Errorf("connect to dest: %w", err)
	}
	defer destConn.Close(ctx)

	sourceCount, err := countDatabases(ctx, conn)
	if err != nil {
		return fmt.Errorf("count source DBs: %w", err)
	}
	destCount, err := countDatabases(ctx, destConn)
	if err != nil {
		return fmt.Errorf("count dest DBs: %w", err)
	}
	fmt.Printf("Source databases:      %d\n", sourceCount)
	fmt.Printf("Destination databases: %d\n", destCount)
	if sourceCount != destCount {
		return fmt.Errorf("MISMATCH — database count differs (source: %d, dest: %d); investigate before proceeding", sourceCount, destCount)
	}
	log("✓ Database count matches")

	// Spot-check row counts
	fmt.Println()
	fmt.Println("Spot-checking row counts on first 5 databases...")
	spotCount := 5
	if len(dbs) < spotCount {
		spotCount = len(dbs)
	}
	for i := 0; i < spotCount; i++ {
		db := dbs[i]
		sourceRows := getRowCount(ctx, cfg.sourceHost, cfg.sourcePort, cfg.sourceUser, cfg.sourcePassword, db)
		destRows := getRowCount(ctx, cfg.destHost, cfg.destPort, cfg.destUser, cfg.destPassword, db)
		status := "✗"
		if sourceRows == destRows {
			status = "✓"
		}
		fmt.Printf("  %s %s — Source: %d rows, Destination: %d rows\n", status, db, sourceRows, destRows)
	}

	// Step 5: Final instructions
	fmt.Println()
	fmt.Println("============================================")
	fmt.Println(" UPDATE YOUR CONNECTION STRINGS NOW")
	fmt.Printf(" Point everything to destination: %s\n", cfg.destHost)
	fmt.Println(" Then bring the app out of maintenance mode")
	fmt.Println("============================================")
	fmt.Println()

	var dumpSize string
	if info, err := os.Stat(cfg.dumpDir); err == nil && info.IsDir() {
		if sz, err := dirSize(cfg.dumpDir); err == nil {
			dumpSize = formatSize(sz)
		}
	}
	log("[%s] Migration complete", time.Now().Format(time.RFC3339))
	if dumpSize != "" {
		fmt.Printf("Total dump size: %s\n", dumpSize)
	}

	return nil
}

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

func countDatabases(ctx context.Context, conn *pgx.Conn) (int, error) {
	var n int
	err := conn.QueryRow(ctx,
		"SELECT count(*) FROM pg_database WHERE datname NOT IN ('template0','template1','postgres')").Scan(&n)
	return n, err
}

func getRowCount(ctx context.Context, host string, port int, user, password, db string) int64 {
	connStr := buildConnStr(host, port, user, password, db)
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return 0
	}
	defer conn.Close(ctx)
	var sum int64
	err = conn.QueryRow(ctx, "SELECT COALESCE(sum(n_live_tup), 0) FROM pg_stat_user_tables").Scan(&sum)
	if err != nil {
		return 0
	}
	return sum
}

func runPgDump(db string) error {
	path := filepath.Join(cfg.dumpDir, db+".dump")
	c := exec.Command("pg_dump",
		"-h", cfg.sourceHost,
		"-p", strconv.Itoa(cfg.sourcePort),
		"-U", cfg.sourceUser,
		"-d", db,
		"--no-owner", "--no-privileges", "--no-acl",
		"-Fc", "-Z3", "-f", path)
	c.Env = append(os.Environ(), "PGPASSWORD="+cfg.sourcePassword)
	c.Stderr = nil
	c.Stdout = nil
	return c.Run()
}

func runCreatedb(db string) error {
	c := exec.Command("createdb",
		"-h", cfg.destHost,
		"-p", strconv.Itoa(cfg.destPort),
		"-U", cfg.destUser,
		db)
	c.Env = append(os.Environ(), "PGPASSWORD="+cfg.destPassword)
	c.Stderr = nil
	c.Stdout = nil
	return c.Run()
}

func runPgRestore(db string) error {
	path := filepath.Join(cfg.dumpDir, db+".dump")
	c := exec.Command("pg_restore",
		"-h", cfg.destHost,
		"-p", strconv.Itoa(cfg.destPort),
		"-U", cfg.destUser,
		"-d", db,
		"--no-owner", "--no-privileges", "--no-acl",
		"-j", "4", path)
	c.Env = append(os.Environ(), "PGPASSWORD="+cfg.destPassword)
	c.Stderr = nil
	c.Stdout = nil
	return c.Run()
}

func dirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
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
