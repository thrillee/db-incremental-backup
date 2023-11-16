package cmd

import (
	"db-incremental-backup/internals"
	"log"

	"github.com/spf13/cobra"
)

var manualBackUpCMD = &cobra.Command{
	Use:   "manual backup",
	Short: "This is a program that helps run manual backup database table",
	Long:  `This is a program that helps run manual backup database table. `,
	Run:   handleManualBackUp,
}

func init() {
	rootCmd.AddCommand(manualBackUpCMD)

	manualBackUpCMD.Flags().StringP("start", "s", "", "Enter the starting time for backup e.g 2023-01-01 00:00:00")
	manualBackUpCMD.Flags().DurationP("duration", "d", 1, "Duration in hours")
}

func handleManualBackUp(cmd *cobra.Command, args []string) {
	log.Println(">>>>>>>>>>>Starting Manual BackUp<<<<<<<<<<<")
	log.Println("DEV: thrillee")
	defer log.Println(">>>>>>>>>>>Manual BackUp Completed<<<<<<<<<<<")

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
