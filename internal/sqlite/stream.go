package sqlite

// import (
// 	stdsql "database/sql"
// 	"io"
// 	"strconv"
// 	"strings"
// 	"sync"
// 	"time"

// 	"github.com/liquidata-inc/go-mysql-server/sql"
// 	"github.com/pkg/errors"
// )

// // StreamDatabase is valid for a single, repeatable query. It keeps track of the results
// // from the previous invocation and picks up where it left off. StreamDatabase should only
// // be used with SELECT queries and should never be shared across client connections.
// type StreamDatabase struct {
// 	*Database
// 	mu     sync.RWMutex
// 	stream bool // true if the query specifies a table stream (only one allowed per query)
// 	tables map[string]sql.Table
// }

// func (db *StreamDatabase) GetTableInsensitive(ctx *sql.Context, name string) (sql.Table, bool, error) {
// 	db.mu.RLock()
// 	stream := db.stream
// 	table, ok := db.tables[name]
// 	db.mu.RUnlock()

// 	if ok {
// 		return table, true, nil
// 	}

// 	if name[0] == '*' {
// 		if stream {
// 			return nil, false, errors.Errorf("only one streaming table allowed")
// 		}
// 		name = name[1:]
// 		var err error
// 		table, ok, err = db.Database.GetTableInsensitive(ctx, name)
// 		if !ok || err != nil {
// 			return nil, ok, err
// 		}
// 		table = &StreamTable{Table: table}
// 		stream = true
// 	}

// 	db.mu.Lock()
// 	db.stream = stream
// 	db.tables[name] = table
// 	db.mu.Unlock()

// 	return table, true, nil
// }

// // StreamTable is a type of table that always has a "time" column and
// // which query results stream forever.
// type StreamTable struct {
// 	Table   sql.Table
// 	rowIter *streamRowIter
// 	db      *StreamDatabase
// }

// var (
// 	_ sql.Table           = (*StreamTable)(nil)
// 	_ sql.InsertableTable = (*StreamTable)(nil)
// 	// _ sql.UpdatableTable = (*StreamTable)(nil)
// 	// _ sql.DeletableTable = (*StreamTable)(nil)
// 	// _ sql.ReplaceableTable = (*StreamTable)(nil)
// 	// _ sql.FilteredTable = (*StreamTable)(nil)
// 	// _ sql.ProjectedTable = (*StreamTable)(nil)
// 	// _ sql.DriverIndexableTable = (*StreamTable)(nil)
// 	// _ sql.AlterableTable = (*StreamTable)(nil)
// 	// _ sql.IndexAlterableTable = (*StreamTable)(nil)
// 	// _ sql.IndexedTable = (*StreamTable)(nil)
// 	// _ sql.ForeignKeyAlterableTable = (*StreamTable)(nil)
// 	// _ sql.ForeignKeyTable = (*StreamTable)(nil)
// )

// func (t *StreamTable) Name() string {
// 	return t.Table.Name()
// }

// func (t *StreamTable) String() string {
// 	return t.Table.String()
// }

// func (t *StreamTable) Schema() sql.Schema {
// 	return t.Table.Schema()
// }

// func (t *StreamTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
// 	return t.Table.Partitions(ctx)
// }

// func (t *StreamTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
// 	if t.rowIter != nil {
// 		return t.rowIter, nil
// 	}

// 	queryFunc := func(lastTime int64) (*stdsql.Rows, error) {
// 		// fmt.Println("queryFunc:", t.Table.Name(), lastTime)
// 		return t.dbr.QueryContext(ctx, "SELECT * FROM \""+t.name+"\" WHERE _time > ? ORDER BY _time ASC", lastTime)
// 	}

// 	t.rowIter = &streamRowIter{
// 		ctx:       ctx,
// 		schema:    t.Table.schema,
// 		queryFunc: queryFunc,
// 		lastTime:  0,
// 	}

// 	return t.rowIter, nil
// }

// func (t *StreamTable) Inserter(ctx *sql.Context) sql.RowInserter {
// 	return t.Table.Inserter(ctx)
// }

// // Streams results continuously. Never closes, just keeps executing.
// type streamRowIter struct {
// 	ctx       *sql.Context
// 	schema    sql.Schema
// 	queryFunc func(lastTime int64) (*stdsql.Rows, error)
// 	lastTime  int64
// 	rows      *stdsql.Rows
// 	timer     *time.Timer
// }

// func (r *streamRowIter) Next() (sql.Row, error) {
// 	if r.rows == nil {
// 		r.timer = time.NewTimer(time.Second)
// 		rows, err := r.queryFunc(r.lastTime)
// 		if err != nil {
// 			return nil, err
// 		}
// 		r.rows = rows
// 	}
// 	for !r.rows.Next() {
// 		if err := r.rows.Err(); err != nil {
// 			return nil, err
// 		}
// 		// delay the next query to maximum once per second to keep things chill
// 		select {
// 		case <-r.ctx.Done():
// 			return nil, io.EOF
// 		case <-r.timer.C:
// 			r.timer.Reset(time.Second)
// 		}
// 		rows, err := r.queryFunc(r.lastTime)
// 		if err != nil {
// 			return nil, err
// 		}
// 		r.rows = rows
// 	}
// 	row := make([]interface{}, len(r.schema))
// 	for i := range row {
// 		row[i] = new(interface{})
// 	}
// 	if err := r.rows.Scan(row...); err != nil {
// 		return nil, err
// 	}
// 	for i, ptr := range row {
// 		row[i] = *(ptr.(*interface{}))
// 	}
// 	for i, col := range r.schema {
// 		// TODO: How do we guarantee this column exists?
// 		if strings.ToLower(col.Name) == "_time" {
// 			switch v := row[i].(type) {
// 			case int64:
// 				r.lastTime = v
// 			default:
// 				val, err := col.Type.SQL(row[i])
// 				if err != nil {
// 					return nil, err
// 				}
// 				lastTime, err := strconv.ParseInt(val.ToString(), 10, 64)
// 				if err != nil {
// 					return nil, err
// 				}
// 				r.lastTime = lastTime
// 			}
// 			break
// 		}
// 	}
// 	return row, nil
// }

// func (r *streamRowIter) Close() error {
// 	return r.rows.Close()
// }

// // Keeps track of all new rows since the last query was executed
// type PostQueryActivity struct {
// 	mu    sync.Mutex
// 	table string
// 	rows  []sql.Row
// }

// func (a *PostQueryActivity) Publish(table string, row sql.Row) {
// 	a.mu.Lock()
// 	defer a.mu.Unlock()
// 	if a.table == "" || a.table == table {
// 		a.table = table
// 		a.rows = append(a.rows, row)
// 	}
// }

// func (a *PostQueryActivity) Snap() (string, []sql.Row) {
// 	a.mu.Lock()
// 	defer func() {
// 		a.table = ""
// 		a.rows = nil
// 		a.mu.Unlock()
// 	}()

// 	return a.table, a.rows
// }
