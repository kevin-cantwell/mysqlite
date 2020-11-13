package sqlite

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/vitess/go/vt/proto/query"
	"github.com/liquidata-inc/vitess/go/vt/sqlparser"
	_ "github.com/mattn/go-sqlite3"
)

type Database struct {
	name string
	w    *stdsql.DB
	r    *stdsql.DB
}

var (
	_ sql.Database     = (*Database)(nil)
	_ sql.TableCreator = (*Database)(nil)
	_ sql.TableDropper = (*Database)(nil)
	// _ sql.TableRenamer = (*Database)(nil)
)

func NewDatabase(name, dsn string) (*Database, error) {
	w, err := stdsql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	// sqlite3 does not allow concurrent write connections
	w.SetMaxOpenConns(1)
	w.SetMaxIdleConns(1)
	w.SetConnMaxLifetime(-1)

	if _, err := w.Exec(
		`CREATE TABLE IF NOT EXISTS mysqlite_table_schema (
			source TEXT, -- table name
			cid INTEGER NOT NULL,
			name TEXT NOT NULL,
			type TEXT NOT NULL,
			pk INTEGER NOT NULL DEFAULT false, -- boolean
			nullable INTEGER NOT NULL DEFAULT true, -- boolean
			dflt_value BLOB,
			comment TEXT,
			num_unsigned INTEGER,  -- boolean
			num_length INTEGER,
			num_scale INTEGER,
			txt_charset TEXT,
			txt_collate TEXT,
			enum_vals TEXT -- json array of strings
		)`,
	); err != nil {
		return nil, err
	}

	r, err := stdsql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	// sqlite3 allows concurrent readers
	r.SetMaxOpenConns(50)
	r.SetMaxIdleConns(10)
	r.SetConnMaxLifetime(-1)
	return &Database{
		name: name,
		w:    w,
		r:    r,
	}, nil
}

func (db *Database) Name() string {
	return db.name
}

func (db *Database) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	rows, err := db.r.QueryContext(ctx,
		`SELECT 
			name, type, pk, nullable, dflt_value, comment, num_unsigned, num_length, num_scale, txt_charset, txt_collate, enum_vals 
		FROM
			mysqlite_table_schema WHERE source = "`+tblName+`"
		ORDER BY
			cid`,
	)
	if err != nil {
		return nil, false, err
	}

	var schema sql.Schema
	for rows.Next() {
		var (
			name      string
			typ       string
			pk        bool
			nullable  bool
			dfltValue stdsql.NullString
			comment   stdsql.NullString
			unsigned  stdsql.NullBool
			length    stdsql.NullInt64
			scale     stdsql.NullInt64
			charset   stdsql.NullString
			collate   stdsql.NullString
			enum      string // json array
		)
		if err := rows.Scan(&name, &typ, &pk, &nullable, &dfltValue, &comment, &unsigned, &length, &scale, &charset, &collate, &enum); err != nil {
			return nil, false, err
		}

		ct := sqlparser.ColumnType{
			Type:     typ,
			Unsigned: sqlparser.BoolVal(unsigned.Bool),
			Charset:  charset.String,
			Collate:  collate.String,
		}
		if length.Valid {
			ct.Length = &sqlparser.SQLVal{
				Type: sqlparser.IntVal,
				Val:  []byte(fmt.Sprintf("%d", length.Int64)),
			}
		}
		if scale.Valid {
			ct.Scale = &sqlparser.SQLVal{
				Type: sqlparser.IntVal,
				Val:  []byte(fmt.Sprintf("%d", scale.Int64)),
			}
		}

		if len(enum) > 0 {
			var enumVals []string
			if err := json.Unmarshal([]byte(enum), &enumVals); err != nil {
				return nil, false, err
			}
			ct.EnumValues = enumVals
		}
		var dflt *string
		if dfltValue.Valid {
			dflt = &dfltValue.String
		}
		colType, err := ColumnTypeToType(&ct)
		if err != nil {
			return nil, false, err
		}
		col := sql.Column{
			Name:       name,
			Type:       colType,
			Default:    dflt,
			Nullable:   nullable,
			Source:     tblName,
			PrimaryKey: pk,
			Comment:    comment.String,
		}
		if dfltValue.Valid {
			d, err := colType.Convert(dfltValue.String)
			if err != nil {
				return nil, false, err
			}
			col.Default = d
		}
		schema = append(schema, &col)
	}

	return &Table{
		name:   tblName,
		schema: schema,
		dbw:    db.w,
		dbr:    db.r,
	}, true, nil
}

func (db *Database) GetTableNames(ctx *sql.Context) ([]string, error) {
	rows, err := db.r.QueryContext(ctx, "SELECT distinct(source) FROM mysqlite_table_schema")
	if err != nil {
		return nil, err
	}
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return tables, nil
}

func (db *Database) CreateTable(ctx *sql.Context, name string, schema sql.Schema) error {
	return inTx(ctx, db.w, func(tx *stdsql.Tx) error {
		var (
			defs []string
			pks  []string
		)

		type columnDefinition struct {
			Name         string
			Type         string
			Affinity     string
			PK           bool
			Nullable     bool
			Comment      string
			DefaultValue *string // formatted for CREATE TABLE syntax
			NumUnsigned  *bool
			NumLength    *int64
			NumScale     *int64
			TxtCharset   *string
			TxtCollate   *string
			EnumVals     string // json array of strings
		}

		for cid, col := range schema {

			def := columnDefinition{
				Name:     col.Name,
				Type:     col.Type.Type().String(),
				PK:       col.PrimaryKey,
				Nullable: col.Nullable,
				Comment:  col.Comment,
			}

			// TODO: These cases may overlap if the interface is not very specific.
			//       You must switch on the base types, unfortunately.
			switch t := col.Type.(type) {
			case sql.EnumType:
				charset := string(t.CharacterSet())
				if len(charset) > 0 {
					def.TxtCharset = &charset
				}
				collate := string(t.Collation())
				if len(collate) > 0 {
					def.TxtCollate = &collate
				}
				b, err := json.Marshal(t.Values())
				if err != nil {
					return err
				}
				def.EnumVals = string(b)

				if col.Default != nil {
					d, err := t.Convert(col.Default)
					if err != nil {
						return err
					}
					val := fmt.Sprintf("%s", d)
					def.DefaultValue = &val
				}
				def.Affinity = "TEXT"
			case sql.StringType:
				charset := string(t.CharacterSet())
				if len(charset) > 0 {
					def.TxtCharset = &charset
				}
				length := t.MaxCharacterLength()
				def.NumLength = &length
				collate := string(t.Collation())
				if len(collate) > 0 {
					def.TxtCollate = &collate
				}
				if col.Default != nil {
					d, err := t.Convert(col.Default)
					if err != nil {
						return err
					}
					val := fmt.Sprintf("%s", d)
					def.DefaultValue = &val
				}

				def.Affinity = "TEXT"
			case sql.NumberType:
				unsigned := !t.IsSigned()
				def.NumUnsigned = &unsigned
				if col.Default != nil {
					d, err := t.Convert(col.Default)
					if err != nil {
						return err
					}
					val := fmt.Sprintf("%v", d)
					def.DefaultValue = &val
				}
				switch t.Type() {
				case query.Type_INT8,
					query.Type_INT16,
					query.Type_INT24,
					query.Type_INT32,
					query.Type_INT64,
					query.Type_UINT8,
					query.Type_UINT16,
					query.Type_UINT32,
					query.Type_UINT64:
					def.Affinity = "INTEGER"
				case query.Type_FLOAT32,
					query.Type_FLOAT64:
					def.Affinity = "REAL"
				default:
					def.Affinity = "NUMERIC"
				}
			case sql.DecimalType:
				length := int64(t.Precision())
				scale := int64(t.Scale())
				def.NumLength = &length
				def.NumScale = &scale
				if col.Default != nil {
					d, err := t.Convert(col.Default)
					if err != nil {
						return err
					}
					val := fmt.Sprintf("%v", d)
					def.DefaultValue = &val
				}
				def.Affinity = "NUMERIC"
			case sql.DatetimeType:
				if col.Default != nil {
					d, err := t.ConvertWithoutRangeCheck(col.Default)
					if err != nil {
						return err
					}
					val := d.Format("2006-01-02 15:04:05.999")
					def.DefaultValue = &val
				}
				def.Affinity = "TEXT" // Best known way to allow for fractional seconds in SQLite
			case sql.TimeType:
				if col.Default != nil {
					d, err := t.Marshal(col.Default)
					if err != nil {
						return err
					}
					val := fmt.Sprintf("%d", d)
					def.DefaultValue = &val
				}
				def.Affinity = "INTEGER"
			case sql.SetType:
				charset := string(t.CharacterSet())
				if len(charset) > 0 {
					def.TxtCharset = &charset
				}
				collate := string(t.Collation())
				if len(collate) > 0 {
					def.TxtCollate = &collate
				}
				b, err := json.Marshal(t.Values())
				if err != nil {
					return err
				}
				def.EnumVals = string(b)

				if col.Default != nil {
					d, err := t.Convert(col.Default)
					if err != nil {
						return err
					}
					val := fmt.Sprintf("%s", d)
					def.DefaultValue = &val
				}
				def.Affinity = "TEXT"
			case sql.BitType:
				if col.Default != nil {
					d, err := t.Convert(col.Default)
					if err != nil {
						return err
					}
					val := fmt.Sprintf("%d", d)
					def.DefaultValue = &val
				}
				def.Affinity = "INTEGER"

			case sql.YearType:
				if col.Default != nil {
					d, err := t.Convert(col.Default)
					if err != nil {
						return err
					}
					val := fmt.Sprintf("%d", d)
					def.DefaultValue = &val
				}
				def.Affinity = "INTEGER"
			case sql.NullType:
				def.Affinity = "TEXT"
			case sql.JsonType:
				if col.Default != nil {
					d, err := t.Convert(col.Default)
					if err != nil {
						return err
					}
					val := fmt.Sprintf("%s", d)
					def.DefaultValue = &val
				}
				def.Affinity = "TEXT"

			default:
				panic("TODO")
			}

			// These strings are added to the CREATE TABLE statement
			colDefClause := fmt.Sprintf("%s %s", def.Name, def.Affinity)
			if def.DefaultValue != nil {
				colDefClause += fmt.Sprintf(" DEFAULT %q", *def.DefaultValue)
			}
			defs = append(defs, colDefClause)
			if def.PK {
				pks = append(pks, def.Name)
			}

			// Track mysql-specific metadata for each column definition
			tx.Exec(
				`INSERT INTO mysqlite_table_schema (
					source,
					cid,
					name,
					type,
					pk,
					nullable,
					dflt_value,
					comment,
					num_unsigned,
					num_length,
					num_scale,
					txt_charset,
					txt_collate,
					enum_vals
				) VALUES (
					?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
				)`,
				name,
				cid,
				def.Name,
				def.Type,
				def.PK,
				def.Nullable,
				def.DefaultValue,
				def.Comment,
				def.NumUnsigned,
				def.NumLength,
				def.NumScale,
				def.TxtCharset,
				def.TxtCollate,
				def.EnumVals,
			)
		}

		if len(pks) > 0 {
			defs = append(defs, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(pks, ", ")))
		}
		if _, err := tx.Exec(fmt.Sprintf(`CREATE TABLE "%s" (%s)`, name, strings.Join(defs, ", "))); err != nil {
			return err
		}
		return nil
	})
}

func (db *Database) DropTable(ctx *sql.Context, name string) error {
	return inTx(ctx, db.w, func(tx *stdsql.Tx) error {
		if _, err := tx.Exec(`DROP TABLE "` + name + `"`); err != nil {
			return err
		}
		if _, err := tx.Exec(`DELETE FROM mysqlite_table_schema WHERE source = "` + name + `"`); err != nil {
			return err
		}
		return nil
	})
}

func inTx(ctx context.Context, db *stdsql.DB, f func(tx *stdsql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := f(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
