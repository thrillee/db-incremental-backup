/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"os"
	"time"

	"github.com/spf13/cobra"
)

func fromDBTime(dbTime string) (time.Time, error) {
	// Parse the database time string into a time.Time object
	layout := "2006-01-02T15:04:05"
	t, err := time.Parse(layout, dbTime)
	if err != nil {
		return time.Time{}, err
	}

	return t, nil
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   `DDMS - Database Data Management Service`,
	Short: "This service is created to help handle data work like, backups, restore, archiving",
	Long:  `This is a program that helps restore and backup database table. `,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	// Run: func(cmd *cobra.Command, args []string) { },
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	// rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.db-incremental-backup.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
}
