package cmd

import (
	"db-incremental-backup/internals"
	"log"
	"strings"

	"github.com/spf13/cobra"
)

var customQueryCMD = &cobra.Command{
	Use:   "query",
	Short: "This program helps run pre-configured query",
	Long:  `This program helps run pre-configured query`,
	Run:   handleCustomerQuery,
}

func init() {
	rootCmd.AddCommand(customQueryCMD)

	customQueryCMD.Flags().StringP("endTime", "e", "", "Enter end time if required E.g 2023-01-01T00:00:00")
	customQueryCMD.Flags().StringP("startTime", "s", "", "Enter start time if required E.g 2021-01-01T00:00:00")
	customQueryCMD.Flags().StringP("query-name", "q", "", "Enter the name of the name of the query you want to run")
	customQueryCMD.Flags().StringSliceP("params", "p", []string{}, "Enter the params required E.G file_name=thrillee.csv")

	customQueryCMD.MarkFlagRequired("params")
	customQueryCMD.MarkFlagRequired("query-name")
	customQueryCMD.MarkFlagsRequiredTogether("startTime", "endTime")
}

func handleCustomerQuery(cmd *cobra.Command, args []string) {
	log.Println(">>>>>>>>>>>Starting Custom Query Engine<<<<<<<<<<<")
	log.Println("DEV: @_thrillee")
	defer log.Println(">>>>>>>>>>>Custom Query Completed<<<<<<<<<<<")

	queryName, err := cmd.Flags().GetString("query-name")
	if err != nil {
		log.Fatal(err)
	}

	params, err := cmd.Flags().GetStringSlice("params")
	if err != nil {
		log.Fatal(err)
	}

	startTimeStr, err := cmd.Flags().GetString("startTime")
	if err != nil {
		log.Fatal(err)
	}

	endTimeStr, err := cmd.Flags().GetString("endTime")
	if err != nil {
		log.Fatal(err)
	}

	params = append(params, strings.ReplaceAll(startTimeStr, "T", " "))
	params = append(params, strings.ReplaceAll(endTimeStr, "T", " "))

	internals.ProcessQuery(queryName, params)
}
