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
	}
}

func insertRestoreEvent(md ManifestData) (string, error) {
	log.Printf(">>>>>>>>>>>>>>>>>>Inserting Restore Event<<<<<<<<<<<<<<<<<<\n")
	defer log.Printf(">>>>>>>>>>>>>>>>>>Inserting Restore Completed<<<<<<<<<<<<<<<<<<\n")

	log.Printf("Table: %s", md.Table)
	log.Printf("Duration: %v to %v", md.StartTime, md.EndTime)

	ref, err := createBackEvent(CreateBackEventData{
		startTime: md.StartTime,
		endTime:   md.EndTime,
		table:     md.Table,
		state:     PROCESS_STATE_RESTORE,
		status:    BACKUP_STATUS_COMPLETED,
	})

	return ref, err
}

func loadCSV(md ManifestData, backUpFileDir string) {
	defer log.Print("\n")
	backFilePath := filepath.Join(backUpFileDir, md.FileName)

	log.Printf("Table: %s", md.Table)
	log.Printf("Backup File: %s", backFilePath)

	loadQuery := fmt.Sprintf("LOAD DATA INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n'",
		backFilePath, md.Table)
	_, err := db.Exec(loadQuery)
	errCheck(err)
}

func HandleBackUpRead(manifest_file_dir string, backUpFileDir string) {
	mds := readManifest(manifest_file_dir)
	processManifest(mds, backUpFileDir)
}
