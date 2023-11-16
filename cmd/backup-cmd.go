package cmd

import (
	"db-incremental-backup/internals"
	"log"
	"time"

	"github.com/spf13/cobra"
)

var backupCMD = &cobra.Command{
	Use:   "backup service",
	Short: "This is a program that helps backup database table",
	Long:  `This is a program that helps backup database table. `,
	Run:   handleBackup,
}

func init() {
	rootCmd.AddCommand(backupCMD)
}

func handleBackup(cmd *cobra.Command, args []string) {
	log.Println(">>>>>>>>>>>Starting BackUp Service<<<<<<<<<<<")
	log.Println("DEV: thrillee")
	defer log.Println(">>>>>>>>>>>Starting BackUp Stoped<<<<<<<<<<<")

	for {
		// time.Sleep(1 * time.Hour)
		internals.BackUpReceiver()
		time.Sleep(20 * time.Second)
	}
}
