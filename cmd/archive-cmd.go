package cmd

import (
	"db-incremental-backup/internals"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

var archiveTableCMD = &cobra.Command{
	Use:   "archive",
	Short: "This is a program that helps run archive database table",
	Long:  `This is a program that helps run archive database table. `,
	Run:   handleArchive,
}

func init() {
	rootCmd.AddCommand(archiveTableCMD)

	archiveTableCMD.Flags().StringP("table", "t", "",
		`Enter the table you want to archive 
You can archive multiple tables by passing -t table1,table2,table3 
You can also archive all the tables in the table setup config by pass -t __all__
You can also archive a single table by passing -t tableName`)

	archiveTableCMD.Flags().StringP("startTime", "s", "", "Enter the starting time for archive e.g 2023-01-01 00:00:00")
	archiveTableCMD.Flags().StringP("endTime", "e", "", "Enter the end time for archive e.g 2023-01-01 00:00:00")

	archiveTableCMD.Flags().StringP("tag", "x", "", "Add a Tag name on the archive output folder")
	archiveTableCMD.Flags().BoolP("skip-backup", "b", false, "Skip Backup process, Just go ahead with delete the records")

	archiveTableCMD.Flags().BoolP("delete-records", "d", true, "Delete the records speicifed within date range. Set to false to skip deleting records")

	archiveTableCMD.MarkFlagRequired("table")
	archiveTableCMD.MarkFlagsRequiredTogether("startTime", "endTime")

	archiveTableCMD.Flags().BoolP("zip-output", "z", true, "Zip output")
}

func handleArchive(cmd *cobra.Command, args []string) {
	log.Println(">>>>>>>>>>>Database Table Archive Service<<<<<<<<<<<")
	log.Print("developed by: @_thrillee\n\n")
	defer log.Print(">>>>>>>>>>>Database Table Archive Service Completed<<<<<<<<<<<\n\n")

	startTimeStr, err := cmd.Flags().GetString("startTime")
	if err != nil {
		log.Fatal(err)
	}

	startTime, err := fromDBTime(startTimeStr)
	if err != nil {
		log.Fatal(err)
	}

	endTimeStr, err := cmd.Flags().GetString("endTime")
	if err != nil {
		log.Fatal(err)
	}

	endTime, err := fromDBTime(endTimeStr)
	if err != nil {
		log.Fatal(err)
	}

	table, err := cmd.Flags().GetString("table")
	if err != nil {
		log.Fatal(err)
	}

	if len(table) < 3 {
		log.Fatal(fmt.Sprintf("Invalid table name %s", table))
	}

	tag, err := cmd.Flags().GetString("tag")
	if err != nil {
		log.Fatal(err)
	}

	skipBackup, err := cmd.Flags().GetBool("skip-backup")
	if err != nil {
		log.Fatal(err)
	}

	allowZip, err := cmd.Flags().GetBool("zip-output")
	if err != nil {
		log.Fatal(err)
	}

	truncateRecords, err := cmd.Flags().GetBool("delete-records")
	if err != nil {
		log.Fatal(err)
	}

	export_dir := os.Getenv("DB_EXPORT_DIR")
	formattedTime := strings.ReplaceAll(fmt.Sprintf("archive-%s-%s", startTimeStr, endTimeStr), " ", "-")
	folder_tag_name := formattedTime
	if tag != "" {
		folder_tag_name = strings.ToLower(strings.ReplaceAll(tag, " ", "-"))
	}

	folder_name := fmt.Sprintf("archive-backups-%s", folder_tag_name)
	folder_path := filepath.Join(export_dir, folder_name)

	manifestData := []internals.ManifestData{}

	if strings.Contains(table, ",") {
		tables := strings.Split(table, ",")
		for _, t := range tables {
			md := internals.DoTableArchive(internals.MakeArchvieRequest{
				Tag:             folder_tag_name,
				TruncateRecords: truncateRecords,
				FolderPath:      folder_path,
				SkipBackup:      skipBackup,
				StartDate:       startTime,
				EndDate:         endTime,
				Table:           t,
			})

			if md != (internals.ManifestData{}) {
				manifestData = append(manifestData, md)
			}
		}
	} else if table == "__all__" {
		for _, config := range internals.RegisteredTables {
			md := internals.DoTableArchive(internals.MakeArchvieRequest{
				Table:           config.TableName,
				TruncateRecords: truncateRecords,
				Tag:             folder_tag_name,
				FolderPath:      folder_path,
				SkipBackup:      skipBackup,
				StartDate:       startTime,
				EndDate:         endTime,
			})

			if md != (internals.ManifestData{}) {
				manifestData = append(manifestData, md)
			}
		}
	} else {
		md := internals.DoTableArchive(internals.MakeArchvieRequest{
			TruncateRecords: truncateRecords,
			Tag:             folder_tag_name,
			FolderPath:      folder_path,
			SkipBackup:      skipBackup,
			StartDate:       startTime,
			EndDate:         endTime,
			Table:           table,
		})

		if md != (internals.ManifestData{}) {
			manifestData = append(manifestData, md)
		}
	}

	internals.CreateManifest(internals.CreateManifestRequest{
		Tag:        folder_tag_name,
		Manifiests: manifestData,
		ExportDir:  folder_path,
		AllowZip:   allowZip,
	})
}
