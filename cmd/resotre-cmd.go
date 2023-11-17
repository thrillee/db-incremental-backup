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
	Run:   handleRestore,
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

	manifest, err := cmd.Flags().GetString("manifest")
	if err != nil {
		log.Fatal(err)
	}

	backup, err := cmd.Flags().GetString("backup")
	if err != nil {
		log.Fatal(err)
	}

	internals.HandleBackUpRead(manifest, backup)
}
