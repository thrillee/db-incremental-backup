package internals

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

type MakeArchvieRequest struct {
	StartDate       time.Time
	EndDate         time.Time
	Table           string
	Tag             string
	FolderPath      string
	SkipBackup      bool
	TruncateRecords bool
}

func DoTableArchive(archiveRequest MakeArchvieRequest) ManifestData {
	table := archiveRequest.Table
	endDate := archiveRequest.EndDate
	startDate := archiveRequest.StartDate

	log.Printf("<<<<<<<<<<<<<<<<<<Starting Table Archive for %s>>>>>>>>>>>>>>>>>>\n", table)
	defer log.Printf("<<<<<<<<<<<<<<<<<<Completed Table Archive for %s>>>>>>>>>>>>>>>>>>\n\n", table)

	config, present := RegisteredTables[table]
	if !present {
		log.Println("Table not found in setup")
		return ManifestData{}
	}

	dbStartTime := ToDBTime(startDate)
	dbEndTime := ToDBTime(endDate)

	log.Printf("start %s", dbStartTime)
	log.Printf("end %s", dbEndTime)

	folder_path := archiveRequest.FolderPath

	dateSuffix := strings.ReplaceAll(fmt.Sprintf("archive-%s", archiveRequest.Tag), " ", "-")

	err := os.MkdirAll(folder_path, 0755)
	errCheck(err)

	fileName := fmt.Sprintf("archive-%s-%s", table, archiveRequest.Tag)
	exportResult := exportResult{
		fileName: fileName,
		filePath: "",
	}
	if !archiveRequest.SkipBackup {
		exportResult, err = makeExport(exportData{
			tableName:        config.TableName,
			dateField:        config.DateField,
			startTime:        dbStartTime,
			endTime:          dbEndTime,
			dateSuffix:       dateSuffix,
			exportFolderPath: folder_path,
		})
		if err != nil {
			log.Println(err)
			return ManifestData{}
		}
	}

	if archiveRequest.TruncateRecords {
		log.Printf("Deleting %s from %s to %s...\n", table, dbStartTime, dbEndTime)
		deleteQuery := fmt.Sprintf("delete from %s where %s between '%s' and '%s'",
			table, config.DateField, dbStartTime, dbEndTime)

		_, dbError := db.Exec(deleteQuery)
		if dbError != nil {
			log.Println(dbError)
			return ManifestData{}
		}
	}

	beResult, beError := createBackEvent(CreateBackEventData{
		status:    BACKUP_STATUS_COMPLETED,
		state:     PROCESS_STATE_ARCHIVE,
		dateField: config.DateField,
		table:     table,
		endTime:   dbEndTime,
		startTime: dbStartTime,
	})
	log.Printf("Event Result -> %s\n", beResult)
	if beError != nil {
		log.Println(beError)
		return ManifestData{}
	}

	return ManifestData{
		Table:     table,
		EndTime:   dbEndTime,
		StartTime: dbStartTime,
		DateField: config.DateField,
		Ref:       archiveRequest.Tag,
		FileName:  exportResult.fileName,
		file_path: exportResult.filePath,
	}
}
