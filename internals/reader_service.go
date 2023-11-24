package internals

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

func readManifest(manifest_file_dir string) []ManifestData {
	jsonData, err := os.ReadFile(manifest_file_dir)
	if err != nil {
		log.Fatal(err)
	}

	manifestData := []ManifestData{}
	err = json.Unmarshal(jsonData, &manifestData)
	errCheck(err)

	return manifestData
}

func processManifest(manifestData []ManifestData, backUpFileDir string) {
	log.Printf(">>>>>>>>>>>>>>>>>>>>>>>>Processing Manfinest<<<<<<<<<<<<<<<<<<<<<<<<")
	defer log.Printf(">>>>>>>>>>>>>>>>>>>>>>>>Processing Manfinest Completed<<<<<<<<<<<<<<<<<<<<<<<<\n")

	for _, md := range manifestData {
		loadCSV(md, backUpFileDir)
		insertRestoreEvent(md)
	}
}

func insertRestoreEvent(md ManifestData) {
	log.Printf(">>>>>>>>>>>>>>>>>>Inserting Restore Event<<<<<<<<<<<<<<<<<<\n")
	defer log.Printf(">>>>>>>>>>>>>>>>>>Inserting Restore Completed<<<<<<<<<<<<<<<<<<\n")

	log.Printf("Table: %s", md.Table)
	log.Printf("Duration: %v to %v", md.StartTime, md.EndTime)

	createBackEvent(CreateBackEventData{
		startTime: md.StartTime,
		endTime:   md.EndTime,
		table:     md.Table,
		dateField: md.DateField,
		state:     PROCESS_STATE_RESTORE,
		status:    BACKUP_STATUS_COMPLETED,
	})
}

func loadCSV(md ManifestData, backUpFileDir string) {
	defer log.Print("\n")
	backFilePath := filepath.Join(backUpFileDir, md.FileName)

	log.Printf("Table: %s", md.Table)
	log.Printf("Backup File: %s", backFilePath)

	loadQuery := fmt.Sprintf("LOAD DATA INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n'",
		backFilePath, md.Table)
	_, err := db.Exec(loadQuery)
	fmt.Println(err)

	log.Printf("Query: %s", loadQuery)
	errCheck(err)
}

func HandleBackUpRead(manifest_file_dir string, backUpFileDir string) {
	mds := readManifest(manifest_file_dir)
	processManifest(mds, backUpFileDir)
}
