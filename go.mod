module github.com/kevin-cantwell/mysqlite

go 1.14

require (
	github.com/liquidata-inc/go-mysql-server v0.6.0
	github.com/liquidata-inc/vitess v0.0.0-20200807222445-2db8e9fb6365
	github.com/mattn/go-sqlite3 v1.14.4
	github.com/pkg/errors v0.8.1
)

replace vitess.io/vitess => github.com/liquidata-inc/vitess v0.0.0-20200430040751-192bb76ecd8b
