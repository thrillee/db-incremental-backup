package internals

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
)

const (
	BACKUP_STATUS_PENDING    = 0
	BACKUP_STATUS_COMPLETED  = 2
	BACKUP_STATUS_PROCESSING = 1
	BACKUP_STATUS_FAILED     = -1

	PROCESS_STATE_BACKUP  = "back_up"
	PROCESS_STATE_RESTORE = "restore"
)

type CreateBackEventData struct {
	table     string
	startTime string
	endTime   string
	state     string
	status    int
}

type BackupEvent struct {
	ID           int            `db:"id"`
	TableName    string         `db:"table_name"`
	Status       string         `db:"status"`
	State        string         `db:"state"`
	StartTimeStr string         `db:"start_time"`
	EndTimeStr   string         `db:"end_time"`
	StatusMsg    sql.NullString `db:"status_msg"`
	FilePath     sql.NullString `db:"file_path"`
	Notify       bool           `db:"notify"`
	Ref          sql.NullString `db:"ref"`
	ModifiedStr  string         `db:"modified"`
	CreatedStr   string         `db:"created"`
}

func scanTime(value interface{}) (time.Time, error) {
	if str, ok := value.(string); ok {
		t, err := time.Parse(time.RFC3339, str)
		if err != nil {
			return time.Time{}, err
		}

		return t, nil
	}

	return time.Time{}, errors.New("unable to scan value to time.Time")
}

func loadBackEvent(sqlQuery string) []BackupEvent {
	backEvents := []BackupEvent{}

	rows, err := db.Query(sqlQuery)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		be := BackupEvent{}
		err := rows.Scan(&be.ID, &be.TableName, &be.Status, &be.State, &be.StartTimeStr, &be.EndTimeStr, &be.StatusMsg, &be.FilePath, &be.Notify, &be.Ref, &be.ModifiedStr, &be.CreatedStr)
		if err != nil {
			log.Fatal(err)
		}

		backEvents = append(backEvents, be)
	}

	return backEvents
}

func createBackEvent(be CreateBackEventData) (string, error) {
	ref := uuid.New().String()

	startTimeStr := be.startTime
	endTimeStr := be.endTime

	columns := "(table_name, status, state, start_time, end_time, status_msg, file_path, notify, modified, created, ref)"
	values := fmt.Sprintf("('%s', %d, '%s', '%s', '%s', null, null, 0, NOW(), NOW(), '%s')",
		be.table, be.status, be.state, startTimeStr, endTimeStr, ref)
	insertQuery := fmt.Sprintf("INSERT INTO backupevent %s VALUES %s", columns, values)

	_, err := db.Exec(insertQuery)

	return ref, err
}

func updateEventStatus(eventId int, status int) {
	updateQuery := fmt.Sprintf("update backupevent set status=%d where id=%d",
		status, eventId)

	_, err := db.Exec(updateQuery)
	errCheck(err)
}

func updateExportStatus(ref, msg, export_path string, status int) {
	updateQuery := fmt.Sprintf("update backupevent set file_path='%s', status=%d, status_msg='%s' where ref='%s'",
		export_path, status, msg, ref)

	_, err := db.Exec(updateQuery)
	errCheck(err)
}
