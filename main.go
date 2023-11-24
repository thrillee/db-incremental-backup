/*
Copyright © 2023 Oluwatobi Bello bellotobiloba01@gmail.com
*/
package main

import (
	"db-incremental-backup/cmd"
	"db-incremental-backup/internals"
	"log"
	"os"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load("db-incremental-backup.env")
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	tableConfigDir := os.Getenv("TABLE_CONFIG_DIR")
	internals.SetupTable(tableConfigDir)

	db := internals.StartDB()
	defer db.Close()

	cmd.Execute()
}

func errCheck(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
