package internals

import (
	"fmt"
	"log"
	"strings"
	"time"
)

func DoTableArchive(startDate, endDate time.Time, table string) {
	log.Printf("<<<<<<<<<<<<<<<<<<Starting Table Archive for %s>>>>>>>>>>>>>>>>>>\n", table)
	defer log.Printf("<<<<<<<<<<<<<<<<<<Completed Table Archive for %s>>>>>>>>>>>>>>>>>>\n\n", table)

	config, present := RegisteredTables[table]
	if !present {
		log.Println("Table not found in setup")
		return
	}

	dbStartTime := ToDBTime(startDate)
	dbEndTime := ToDBTime(endDate)

	log.Printf("start %s", dbStartTime)
	log.Printf("end %s", dbEndTime)

	dateSuffix := strings.ReplaceAll(fmt.Sprintf("archive-%s-%s", dbStartTime, dbEndTime), " ", "-")

	fileName, exportError := makeExport(exportData{
		tableName:  config.TableName,
		dateField:  config.DateField,
		startTime:  dbStartTime,
		endTime:    dbEndTime,
		dateSuffix: dateSuffix,
	})

	if exportError != nil {
		log.Println(exportError)
		return
	}

	log.Printf("Backup Dir -> %s\n", fileName)

	log.Printf("Deleting %s from %s to %s...\n", table, dbStartTime, dbEndTime)
	deleteQuery := fmt.Sprintf("delete from %s where %s between '%s' and '%s'",
		table, config.DateField, dbStartTime, dbEndTime)

	_, dbError := db.Exec(deleteQuery)
	if dbError != nil {
		log.Println(dbError)
		return
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
		return
	}
}
