package cmd

import (
	"db-incremental-backup/internals"
	"log"

	"github.com/spf13/cobra"
)

var restoreCMD = &cobra.Command{
	Use:   "restore",
	Short: "This is a program that helps run restore database table",
	Long:  `This is a program that helps run restore database table. `,
	Run:   handleManualBackUp,
}

func init() {
	rootCmd.AddCommand(restoreCMD)

	restoreCMD.Flags().StringP("manifest", "m", "", "Manifest path")
	restoreCMD.Flags().StringP("backup", "b", "", "csv dir")
}

func handleRestore(cmd *cobra.Command, args []string) {
	log.Println(">>>>>>>>>>>Starting Restore Service<<<<<<<<<<<")
	log.Println("DEV: thrillee")
	defer log.Println(">>>>>>>>>>>Restore Service Completed<<<<<<<<<<<")

	startTimeStr, err := cmd.Flags().GetString("start")
	if err != nil {
		log.Fatal(err)
	}

	startTime, err := fromDBTime(startTimeStr)
	if err != nil {
		log.Fatal(err)
	}

	duration, err := cmd.Flags().GetDuration("duration")
	if err != nil {
		log.Fatal(err)
	}

	internals.ManualBackup(startTime, int(duration))
}
