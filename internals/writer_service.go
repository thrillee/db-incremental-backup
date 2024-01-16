package internals

import (
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
	DateField string `json:"dateField"`
	file_path string
}

type exportData struct {
	tableName        string
	dateField        string
	dateSuffix       string
	startTime        string
	endTime          string
	exportFolderPath string
}

type exportResult struct {
	fileName string
	filePath string
}

func makeExport(data exportData) (exportResult, error) {
	/* /var/lib/mysql-files/ */
	fileName := fmt.Sprintf("%s-%s.csv", data.tableName, data.dateSuffix)
	export_path := strings.ReplaceAll(fmt.Sprintf("%s/%s", data.exportFolderPath, fileName), " ", "-")
	log.Printf("Export -> %s", export_path)

	count_query_str := fmt.Sprintf("select count(*) from %s where %s between '%s' and '%s'",
		data.tableName, data.dateField, data.startTime, data.endTime)
	count_result := db.QueryRow(count_query_str)

	var rowCount int
	err := count_result.Scan(&rowCount)
	errCheck(err)

	log.Printf("Record count -> %d", rowCount)

	export_query_str := fmt.Sprintf(
		"SELECT * INTO OUTFILE '%s' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' FROM %s where %s between '%s' and '%s'",
		export_path, data.tableName, data.dateField, data.startTime, data.endTime)

	_, err = db.Exec(export_query_str)
	errCheck(err)

	return exportResult{
		fileName: fileName,
		filePath: export_path,
	}, err
}

func updateNextBackupSchedule(oldEndTime time.Time, backUpDuration int) (time.Time, time.Time) {
	startTime := oldEndTime
	endTime := startTime.Add(time.Duration(backUpDuration) * time.Hour)

	return startTime, endTime
}

func insertNextBackupSchedule(table, dateField string, oldEndTime time.Time, backUpDuration int) (string, error) {
	startTime, endTime := updateNextBackupSchedule(oldEndTime, backUpDuration)

	log.Printf(">>>>>>>>>>>>>>>>>>Inserting new schedule<<<<<<<<<<<<<<<<<<\n")
	defer log.Printf(">>>>>>>>>>>>>>>>>>Insertion Completed<<<<<<<<<<<<<<<<<<\n")

	log.Printf("Table: %s", table)
	log.Printf("Duration: %v to %v", startTime, endTime)

	ref, err := createBackEvent(CreateBackEventData{
		table:     table,
		dateField: dateField,
		endTime:   ToDBTime(endTime),
		startTime: ToDBTime(startTime),
		state:     PROCESS_STATE_BACKUP,
		status:    BACKUP_STATUS_PENDING,
	})

	return ref, err
}

func BackUpReceiver(zipOutput bool) {
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

		exportResult, err := makeExport(exportData{
			tableName:        e.TableName,
			dateField:        e.dateField,
			dateSuffix:       dateSuffix,
			endTime:          e.EndTimeStr,
			startTime:        e.StartTimeStr,
			exportFolderPath: folder_path,
		})
		if err != nil {
			updateEventStatus(e.ID, BACKUP_STATUS_FAILED)
			// errCheck(err)
		} else {
			updateEventStatus(e.ID, BACKUP_STATUS_COMPLETED)
		}

		endTime, err := FromDBTime(e.EndTimeStr)
		errCheck(err)

		ref, err := insertNextBackupSchedule(e.TableName, e.dateField, endTime, backUpDuration)
		errCheck(err)

		manifestData = append(manifestData, ManifestData{
			Ref:       ref,
			Table:     e.TableName,
			DateField: e.dateField,
			EndTime:   e.EndTimeStr,
			StartTime: e.StartTimeStr,
			FileName:  exportResult.fileName,
		})
	}

	CreateManifest(CreateManifestRequest{
		AllowZip:   zipOutput,
		ExportDir:  folder_path,
		Manifiests: manifestData,
		Tag:        uuid.New().String(),
	})
}

func ManualBackup(startTime time.Time, backUpDuration int) {
	for _, value := range RegisteredTables {
		insertNextBackupSchedule(value.TableName, value.DateField, startTime, backUpDuration)
	}
}
