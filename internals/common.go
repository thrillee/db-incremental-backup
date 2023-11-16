package internals

import (
	"log"
	"time"
)

func FromDBTime(dbTime string) (time.Time, error) {
	// Custom parse format to handle MySQL timestamp format
	customParseFormat := "2006-01-02 15:04:05"

	// Parse the database time string into a time.Time object
	t, err := time.Parse(customParseFormat, dbTime)
	if err != nil {
		return time.Time{}, err
	}

	return t, nil
}

func ToDBTime(t time.Time) string {
	return t.Format("2006-01-02 15:04:05")
}

func errCheck(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
