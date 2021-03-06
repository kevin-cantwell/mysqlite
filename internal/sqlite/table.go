package sqlite

import (
	stdsql "database/sql"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/liquidata-inc/go-mysql-server/sql"
)

type Table struct {
	name   string
	schema sql.Schema
	dbr    *stdsql.DB
	dbw    *stdsql.DB
}

var (
	_ sql.Table           = (*Table)(nil)
	_ sql.InsertableTable = (*Table)(nil)
	// _ sql.UpdatableTable = (*Table)(nil)
	// _ sql.DeletableTable = (*Table)(nil)
	// _ sql.ReplaceableTable = (*Table)(nil)
	// _ sql.FilteredTable = (*Table)(nil)
	// _ sql.ProjectedTable = (*Table)(nil)
	// _ sql.DriverIndexableTable = (*Table)(nil)
	// _ sql.AlterableTable = (*Table)(nil)
	// _ sql.IndexAlterableTable = (*Table)(nil)
	// _ sql.IndexedTable = (*Table)(nil)
	// _ sql.ForeignKeyAlterableTable = (*Table)(nil)
	// _ sql.ForeignKeyTable = (*Table)(nil)
)

func (t *Table) Name() string {
	return t.name
}

func (t *Table) String() string {
	return fmt.Sprintf("Table(%s)", t.name)
}

func (t *Table) Schema() sql.Schema {
	return t.schema
}

func (t *Table) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return &partitionIter{
		keys: [][]byte{[]byte("0")},
	}, nil
}

func (t *Table) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	if string(partition.Key()) != "0" {
		return nil, fmt.Errorf("partition not found: %q", partition.Key())
	}

	rows, err := t.dbr.QueryContext(ctx, "SELECT * FROM \""+t.name+"\"")
	if err != nil {
		return nil, err
	}

	return &rowIter{
		schema: t.schema,
		rows:   rows,
	}, nil
}

type partition struct {
	key []byte
}

func (p *partition) Key() []byte { return p.key }

type partitionIter struct {
	keys [][]byte
	pos  int
}

func (p *partitionIter) Next() (sql.Partition, error) {
	if p.pos >= len(p.keys) {
		return nil, io.EOF
	}

	key := p.keys[p.pos]
	p.pos++
	return &partition{key}, nil
}

func (p *partitionIter) Close() error { return nil }

type rowIter struct {
	schema sql.Schema
	rows   *stdsql.Rows
}

func (r *rowIter) Next() (sql.Row, error) {
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return nil, err
		}
		if err := r.Close(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	row := make([]interface{}, len(r.schema))
	for i := range row {
		row[i] = new(interface{})
	}
	if err := r.rows.Scan(row...); err != nil {
		return nil, err
	}
	for i, ptr := range row {
		row[i] = *(ptr.(*interface{}))
	}
	return row, nil
}

func (r *rowIter) Close() error {
	return r.rows.Close()
}

func (t *Table) Inserter(ctx *sql.Context) sql.RowInserter {
	tx, err := t.dbw.BeginTx(ctx, nil)
	return &rowInserter{
		table: t,
		tx:    tx,
		err:   err,
	}
}

type rowInserter struct {
	table *Table
	tx    *stdsql.Tx
	err   error
}

func (i *rowInserter) Insert(ctx *sql.Context, row sql.Row) error {
	cols := make([]string, len(i.table.schema))
	phdr := make([]string, len(i.table.schema))
	for i, col := range i.table.schema {
		cols[i] = col.Name
		if col.Name == "rowtime" && row[i] == nil {
			phdr[i] = fmt.Sprintf("%d", time.Now().UnixNano())
			row = append(row[:i], row[i+1:]...)
			fmt.Println("ROW:", row)
		} else {
			phdr[i] = "?"
		}
	}
	statement := fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES (%s)`, i.table.name, strings.Join(cols, ","), strings.Join(phdr, ","))
	_, err := i.tx.ExecContext(ctx, statement, row...)
	return err
}

func (i *rowInserter) Close(ctx *sql.Context) error {
	if i.err != nil {
		_ = i.tx.Rollback()
		return i.err
	}
	return i.tx.Commit()
}
