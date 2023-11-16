package internals

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
)

type DBParams struct {
	username string
	password string
	name     string
	host     string
	port     string
}

var db *sql.DB

func getDBConn(dbParams DBParams) *sql.DB {
	db_url := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s", dbParams.username, dbParams.password, dbParams.host, dbParams.port, dbParams.name)

	db, err := sql.Open("mysql", db_url)
	errCheck(err)
	return db
}

func StartDB() *sql.DB {
	db_data := DBParams{
		username: os.Getenv("DB_USERNAME"),
		password: os.Getenv("DB_PASSWORD"),
		name:     os.Getenv("DB_NAME"),
		host:     os.Getenv("DB_HOST"),
		port:     os.Getenv("DB_PORT"),
	}

	db = getDBConn(db_data)
	return db
}
