package sqlite

import (
	stdsql "database/sql"
	"fmt"
	"io"

	"github.com/liquidata-inc/go-mysql-server/sql"
)

type Table struct {
	name   string
	schema sql.Schema
	dbr    *stdsql.DB
	dbw    *stdsql.DB
}

var _ sql.Table = (*Table)(nil)

// var _ sql.InsertableTable = (*Table)(nil)
// var _ sql.UpdatableTable = (*Table)(nil)
// var _ sql.DeletableTable = (*Table)(nil)
// var _ sql.ReplaceableTable = (*Table)(nil)
// var _ sql.FilteredTable = (*Table)(nil)
// var _ sql.ProjectedTable = (*Table)(nil)
// var _ sql.DriverIndexableTable = (*Table)(nil)
// var _ sql.AlterableTable = (*Table)(nil)
// var _ sql.IndexAlterableTable = (*Table)(nil)
// var _ sql.IndexedTable = (*Table)(nil)
// var _ sql.ForeignKeyAlterableTable = (*Table)(nil)
// var _ sql.ForeignKeyTable = (*Table)(nil)

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
		// TODO maybe use concrete types?
		row[i] = new(interface{})
	}
	if err := r.rows.Scan(row...); err != nil {
		return nil, err
	}
	return row, nil
}

func (r *rowIter) Close() error {
	return r.rows.Close()
}
