package cmd

import (
	"db-incremental-backup/internals"
	"fmt"
	"log"
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

	archiveTableCMD.MarkFlagRequired("table")
	archiveTableCMD.MarkFlagsRequiredTogether("startTime", "endTime")
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

	if strings.Contains(table, ",") {
		tables := strings.Split(table, ",")
		for _, t := range tables {
			internals.DoTableArchive(startTime, endTime, t)
		}
	} else if table == "__all__" {
		for _, config := range internals.RegisteredTables {
			internals.DoTableArchive(startTime, endTime, config.TableName)
		}
	} else {
		internals.DoTableArchive(startTime, endTime, table)
	}
}
