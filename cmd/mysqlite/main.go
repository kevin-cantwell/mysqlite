package main

import (
	"os"
	"time"

	"github.com/kevin-cantwell/mysqlite/internal/sqlite"
	sqle "github.com/liquidata-inc/go-mysql-server"
	"github.com/liquidata-inc/go-mysql-server/auth"
	"github.com/liquidata-inc/go-mysql-server/memory"
	"github.com/liquidata-inc/go-mysql-server/server"
	"github.com/liquidata-inc/go-mysql-server/sql"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	driver := sqle.NewDefault()
	driver.AddDatabase(createSQLiteDatabase(os.Args[1]))

	config := server.Config{
		Protocol: "tcp",
		Address:  "localhost:3306",
		Auth:     auth.NewNativeSingle("user", "pass", auth.AllPermissions),
	}

	s, err := server.NewDefaultServer(config, driver)
	if err != nil {
		panic(err)
	}

	s.Start()
}

func createSQLiteDatabase(dsn string) *sqlite.Database {
	db, err := sqlite.NewDatabase("default", dsn)
	if err != nil {
		panic(err)
	}
	return db
}

func createInMemoryDatabase() *memory.Database {
	const (
		dbName    = "test"
		tableName = "mytable"
	)

	db := memory.NewDatabase(dbName)
	table := memory.NewTable(tableName, sql.Schema{
		{Name: "name", Type: sql.Text, Nullable: false, Source: tableName},
		{Name: "email", Type: sql.Text, Nullable: false, Source: tableName},
		{Name: "phone_numbers", Type: sql.JSON, Nullable: false, Source: tableName},
		{Name: "created_at", Type: sql.Timestamp, Nullable: false, Source: tableName},
	})

	db.AddTable(tableName, table)
	ctx := sql.NewEmptyContext()

	rows := []sql.Row{
		sql.NewRow("John Doe", "john@doe.com", []string{"555-555-555"}, time.Now()),
		sql.NewRow("John Doe", "johnalt@doe.com", []string{}, time.Now()),
		sql.NewRow("Jane Doe", "jane@doe.com", []string{}, time.Now()),
		sql.NewRow("Evil Bob", "evilbob@gmail.com", []string{"555-666-555", "666-666-666"}, time.Now()),
	}

	for _, row := range rows {
		table.Insert(ctx, row)
	}

	return db
}
