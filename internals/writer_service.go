package internals

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

type ManifestData struct {
	Table     string `json:"table"`
	FileName  string `json:"fileName"`
	Ref       string `json:"ref"`
	StartTime string `json:"startTime"`
	EndTime   string `json:"endTime"`
}

func makeExport(tableName, dateSuffix, db_export_dir string) (string, error) {
	/* /var/lib/mysql-files/ */
	fileName := fmt.Sprintf("%s-%s.csv", tableName, dateSuffix)
	export_dir := strings.ReplaceAll(fmt.Sprintf("%s/%s", db_export_dir, fileName), " ", "-")
	log.Printf("Export Dir: %s", export_dir)

	export_query_str := fmt.Sprintf(
		"SELECT * INTO OUTFILE '%s' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' FROM %s", export_dir, tableName)
	log.Printf("Query: %s", export_query_str)
	_, err := db.Exec(export_query_str)
	log.Println("Completed")
	// errCheck(err) return fileName, err
	return fileName, err
}

func loadDBTables() []string {
	rows, err := db.Query("SHOW TABLES;")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	tableNames := make([]string, 0)

	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			log.Fatal(err)
		}

		tableNames = append(tableNames, tableName)
	}

	return tableNames
}

func updateNextBackupSchedule(oldEndTime time.Time, backUpDuration int) (time.Time, time.Time) {
	startTime := oldEndTime
	endTime := startTime.Add(time.Duration(backUpDuration) * time.Hour)

	return startTime, endTime
}

func insertNextBackupSchedule(table string, oldEndTime time.Time, backUpDuration int) (string, error) {
	startTime, endTime := updateNextBackupSchedule(oldEndTime, backUpDuration)

	log.Printf(">>>>>>>>>>>>>>>>>>Inserting new schedule<<<<<<<<<<<<<<<<<<\n")
	defer log.Printf(">>>>>>>>>>>>>>>>>>Insertion Completed<<<<<<<<<<<<<<<<<<\n")

	log.Printf("Table: %s", table)
	log.Printf("Duration: %v to %v", startTime, endTime)

	ref, err := createBackEvent(CreateBackEventData{
		startTime: ToDBTime(startTime),
		endTime:   ToDBTime(endTime),
		table:     table,
		state:     PROCESS_STATE_BACKUP,
		status:    BACKUP_STATUS_PENDING,
	})

	return ref, err
}

func createManifest(export_dir string, manifest []ManifestData) {
	file_ref := uuid.New().String()
	fileName := fmt.Sprintf("%s-%s.json", "manifest", file_ref)

	manifest_dir := fmt.Sprintf("%s/%s", export_dir, fileName)

	b, err := json.MarshalIndent(manifest, "", "\t")
	if err != nil {
		log.Fatal(err)
	}

	b = append(b, byte('\n'))

	if err := os.WriteFile(manifest_dir, b, 0644); err != nil {
		log.Fatal(err)
	}

	log.Printf("Manifest data written to %s", manifest_dir)
}

func getExportableTables() []string {
	db_export_tables := os.Getenv("DB_EXPORT_TABLES")
	is_all_tables := db_export_tables == "__all__"

	var tables []string

	if is_all_tables {
		tables = loadDBTables()
	} else {
		tables = strings.Split(db_export_tables, ",")
	}

	// now := time.Now()
	// formattedDate := now.Format(time.RFC3339)

	return tables
}

func BackUpReceiver() {
	backUpDuration, err := strconv.Atoi(os.Getenv("DURATION_HR"))
	errCheck(err)

	export_dir := os.Getenv("DB_EXPORT_DIR")

	log.Printf(">>>>>>>>>>>>>>>>>>Writer Started<<<<<<<<<<<<<<<<<<\n")
	defer log.Printf(">>>>>>>>>>>>>>>>>>Writer Completed<<<<<<<<<<<<<<<<<<\n\n")

	selectQuery := fmt.Sprintf(
		"select * from backupevent where start_time <= now() and status=%d and state='%s'",
		BACKUP_STATUS_PENDING, PROCESS_STATE_BACKUP)

	events := loadBackEvent(selectQuery)
	totalEvents := len(events)

	log.Printf("Events: %d", len(events))

	if totalEvents == 0 {
		return
	}

	t := time.Now()
	formattedTime := t.Format("20060102150405")

	folder_name := fmt.Sprintf("incremental-backups-%s", formattedTime)
	folder_path := filepath.Join(export_dir, folder_name)

	err = os.MkdirAll(folder_path, 0755)
	errCheck(err)

	manifestData := []ManifestData{}

	for _, e := range events {
		updateEventStatus(e.ID, BACKUP_STATUS_PROCESSING)
		dateSuffix := fmt.Sprintf("%v-%v", e.StartTimeStr, e.EndTimeStr)

		fileName, err := makeExport(e.TableName, dateSuffix, folder_path)
		if err != nil {
			updateEventStatus(e.ID, BACKUP_STATUS_FAILED)
			// errCheck(err)
		} else {
			updateEventStatus(e.ID, BACKUP_STATUS_COMPLETED)
		}

		endTime, err := FromDBTime(e.EndTimeStr)
		errCheck(err)

		ref, err := insertNextBackupSchedule(e.TableName, endTime, backUpDuration)
		errCheck(err)

		manifestData = append(manifestData, ManifestData{
			Table:     e.TableName,
			FileName:  fileName,
			Ref:       ref,
			EndTime:   e.EndTimeStr,
			StartTime: e.StartTimeStr,
		})
	}

	createManifest(folder_path, manifestData)
}

func ManualBackup(startTime time.Time, backUpDuration int) {
	tables := getExportableTables()

	for _, table := range tables {
		insertNextBackupSchedule(table, startTime, backUpDuration)
	}
}
