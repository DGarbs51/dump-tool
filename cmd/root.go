package cmd

import (
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "dump-tool [command]",
	Short: "PostgreSQL migration tool: dump from source, restore to destination",
	Long:  `Dump all databases from a source PostgreSQL instance and restore them to a destination instance with parallel execution and verification.`,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		panic(err)
	}
}
