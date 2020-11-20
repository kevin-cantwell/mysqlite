package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/kevin-cantwell/mysqlite/internal/sqlite"
	sqle "github.com/liquidata-inc/go-mysql-server"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/vitess/go/vt/sqlparser"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	var (
		query   string
		dataDir string
	)
	{
		flag.StringVar(&query, "query", "", "SQL Query.")
		flag.StringVar(&query, "q", "", "SQL Query.")
		dataDirDefault, err := ioutil.TempDir("", "streamsql_*")
		if err != nil {
			panic(err)
		}
		flag.StringVar(&dataDir, "data-dir", dataDirDefault, "Data directory.")
		flag.StringVar(&dataDir, "d", dataDirDefault, "Data directory.")
		flag.Parse()
	}

	ctx := sql.NewContext(context.Background())

	engine := sqle.NewDefault()
	db := createStreamDatabase(dataDir)
	engine.AddDatabase(db)

	enc := json.NewEncoder(os.Stdout)

	stmt, err := sqlparser.Parse(query)
	if err != nil {
		panic(err)
	}

	doQuery := func(stmt sqlparser.Statement) int {
		fmt.Println("doQuery:", sqlparser.String(stmt))
		_, rows, err := engine.Query(ctx, sqlparser.String(stmt))
		if err != nil {
			panic(err)
		}
		var count int
		for {
			row, err := rows.Next()
			if err != nil {
				if err == io.EOF {
					return count
				}
				panic(err)
			}
			count++
			if err := enc.Encode(row); err != nil {
				panic(err)
			}
		}
	}

	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		timer := time.NewTimer(time.Second)

		origWhere := *stmt.Where

		tables := parseSelectTableNames(stmt)
		maxRowtimes := queryMaxRowTimes(ctx, engine, tables)
		stmt.Where = setRowtimesLessEqualThan(&origWhere, maxRowtimes)

		// First query everything up to and including max rowtimes
		if doQuery(stmt) == 0 {
			// if there's no results, wait a sec before trying again
			select {
			case <-ctx.Done():
				os.Exit(0)
			case <-timer.C:
				timer.Reset(time.Second)
			}
		}

		for {
			prevMaxRowtimes := maxRowtimes
			maxRowtimes = queryMaxRowTimes(ctx, engine, tables)
			if maxRowtimesEqual(prevMaxRowtimes, maxRowtimes) {
				// if there's no new rows, wait a sec before trying again
				select {
				case <-ctx.Done():
					os.Exit(0)
				case <-timer.C:
					timer.Reset(time.Second)
					continue
				}
			}

			startingWhere := setRowtimesLessEqualThan(&origWhere, maxRowtimes)

			// for each table in the query, fetch a new result set limited to new rows since the last iter
			for _, table := range tables {
				stmt.Where = setRowtimeGreaterThan(startingWhere, table, prevMaxRowtimes[table])
				// For each table, query up to and including max rowtimes, but for this table only include
				// new rows that arrivved since the last query.
				if doQuery(stmt) == 0 {
					// if there's no results, wait a sec before trying again
					select {
					case <-ctx.Done():
						os.Exit(0)
					case <-timer.C:
						timer.Reset(time.Second)
					}
				}

			}
		}
	default:
		doQuery(stmt)
	}
}

func createStreamDatabase(dataDir string) *sqlite.Database {
	dsn := filepath.Join(dataDir, "stream.db")
	db, err := sqlite.NewDatabase("", dsn)
	if err != nil {
		panic(err)
	}
	return db
	// return &sqlite.StreamDatabase{Database: db}
}

func maxRowtimesEqual(left map[string]int64, right map[string]int64) bool {
	if len(left) != len(right) {
		return false
	}
	for key := range left {
		if left[key] != right[key] {
			return false
		}
	}
	return true
}

func queryMaxRowTimes(ctx *sql.Context, engine *sqle.Engine, tables []string) map[string]int64 {
	maxRowtimes := map[string]int64{}
	for _, table := range tables {
		_, rows, err := engine.Query(ctx, fmt.Sprintf("SELECT COALESCE(max(rowtime), 0) FROM \"%s\"", table))
		if err != nil {
			panic(err)
		}
		row, err := rows.Next()
		if err != nil {
			if err == io.EOF {
				continue
			}
			panic(err)
		}
		maxRowtimes[table] = row[0].(int64)
	}
	return maxRowtimes
}

func setRowtimeGreaterThan(where *sqlparser.Where, table string, rowtime int64) *sqlparser.Where {
	expr := &sqlparser.ComparisonExpr{
		Operator: sqlparser.GreaterThanStr,
		Left: &sqlparser.ColName{
			Name: sqlparser.NewColIdent("rowtime"),
			Qualifier: sqlparser.TableName{
				Name: sqlparser.NewTableIdent(table),
				// Qualifier: TODO, // database name
			},
		},
		Right: sqlparser.NewIntVal([]byte(fmt.Sprintf("%d", rowtime))),
	}
	if where == nil {
		return sqlparser.NewWhere(sqlparser.WhereStr, expr)
	}
	return sqlparser.NewWhere(sqlparser.WhereStr, &sqlparser.AndExpr{
		Left:  expr,
		Right: where.Expr,
	})

}

// Modifies the WHERE clause of stmt to ensure that "table.rowtime <= $maxRowTime" for all tables
func setRowtimesLessEqualThan(where *sqlparser.Where, maxRowtimes map[string]int64) *sqlparser.Where {
	if len(maxRowtimes) == 0 {
		return where
	}

	var expr sqlparser.Expr

	var comps []*sqlparser.ComparisonExpr
	for table, rowtime := range maxRowtimes {
		// table.rowtime <= $rowtime
		comp := &sqlparser.ComparisonExpr{
			Operator: sqlparser.LessEqualStr,
			Left: &sqlparser.ColName{
				Name: sqlparser.NewColIdent("rowtime"),
				Qualifier: sqlparser.TableName{
					Name: sqlparser.NewTableIdent(table),
					// Qualifier: TODO, // database name
				},
			},
			Right: sqlparser.NewIntVal([]byte(fmt.Sprintf("%d", rowtime))),
		}
		comps = append(comps, comp)
	}

	// comp[0] AND comp[1] AND comp[2]...
	expr = comps[0]
	for _, comp := range comps[1:] {
		expr = &sqlparser.AndExpr{
			Left:  expr,
			Right: comp,
		}
	}

	if where == nil {
		return sqlparser.NewWhere(sqlparser.WhereStr, expr)
	}
	return sqlparser.NewWhere(sqlparser.WhereStr, &sqlparser.AndExpr{
		Left:  expr,
		Right: where.Expr,
	})
}

func parseSelectTableNames(stmt sqlparser.SelectStatement) []string {
	var tables []string
	switch stmt := stmt.(type) {
	case *sqlparser.Union:
		tables = append(tables, parseSelectTableNames(stmt.Left)...)
		tables = append(tables, parseSelectTableNames(stmt.Right)...)
	case *sqlparser.ParenSelect:
		tables = append(tables, parseSelectTableNames(stmt.Select)...)
	case *sqlparser.Select:
		tables = append(tables, parseTableExprsNames(stmt.From)...)
	default:
		panic(fmt.Sprintf("unexpected SELECT type: %T", stmt))
	}
	return dedupStrings(tables)
}

func parseTableExprsNames(tt sqlparser.TableExprs) []string {
	var tables []string
	for _, t := range tt {
		switch t := t.(type) {
		case *sqlparser.AliasedTableExpr:
			switch expr := t.Expr.(type) {
			case sqlparser.TableName:
				tables = append(tables, expr.Name.String())
			case *sqlparser.Subquery:
				tables = append(tables, parseSelectTableNames(expr.Select)...)
			}
		case *sqlparser.ParenTableExpr:
			tables = append(tables, parseTableExprsNames(t.Exprs)...)
		case *sqlparser.JoinTableExpr:
			tables = append(tables, parseTableExprsNames(sqlparser.TableExprs{t.LeftExpr})...)
			tables = append(tables, parseTableExprsNames(sqlparser.TableExprs{t.RightExpr})...)
		}
	}
	return tables
}

func dedupStrings(ss []string) []string {
	var deduped []string
	m := map[string]bool{}
	for _, s := range ss {
		m[s] = true
	}
	for s, _ := range m {
		deduped = append(deduped, s)
	}
	return deduped
}
