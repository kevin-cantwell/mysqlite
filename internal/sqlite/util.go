// NOTE: Extended from https://godoc.org/github.com/liquidata-inc/go-mysql-server/sql#ColumnTypeToType
//       in order to handle more base types
package sqlite

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/vitess/go/sqltypes"
	"github.com/liquidata-inc/vitess/go/vt/sqlparser"
)

const (
	charBinaryMax       = 255
	varcharVarbinaryMax = 65535

	tinyTextBlobMax   = charBinaryMax
	textBlobMax       = varcharVarbinaryMax
	mediumTextBlobMax = 16777215
	longTextBlobMax   = int64(4294967295)
)

// ColumnTypeToType gets the column type using the column definition.
func ColumnTypeToType(ct *sqlparser.ColumnType) (sql.Type, error) {
	switch strings.ToLower(ct.Type) {
	case "int8", "boolean", "bool":
		return sql.Int8, nil
	case "uint8":
		return sql.Uint8, nil
	case "tinyint":
		if ct.Unsigned {
			return sql.Uint8, nil
		}
		return sql.Int8, nil
	case "int16":
		return sql.Int16, nil
	case "uint16":
		return sql.Uint16, nil
	case "smallint":
		if ct.Unsigned {
			return sql.Uint16, nil
		}
		return sql.Int16, nil
	case "int24":
		return sql.Int24, nil
	case "uint24":
		return sql.Uint24, nil
	case "mediumint":
		if ct.Unsigned {
			return sql.Uint24, nil
		}
		return sql.Int24, nil
	case "int32":
		return sql.Int32, nil
	case "uint32":
		return sql.Uint32, nil
	case "int", "integer":
		if ct.Unsigned {
			return sql.Uint32, nil
		}
		return sql.Int32, nil
	case "int64":
		return sql.Int64, nil
	case "uint64":
		return sql.Uint64, nil
	case "bigint":
		if ct.Unsigned {
			return sql.Uint64, nil
		}
		return sql.Int64, nil
	case "float", "float32":
		return sql.Float32, nil
	case "double", "real", "double precision", "float64":
		return sql.Float64, nil
	case "decimal", "fixed", "dec", "numeric":
		precision := int64(0)
		scale := int64(0)
		if ct.Length != nil {
			var err error
			precision, err = strconv.ParseInt(string(ct.Length.Val), 10, 8)
			if err != nil {
				return nil, err
			}
		}
		if ct.Scale != nil {
			var err error
			scale, err = strconv.ParseInt(string(ct.Scale.Val), 10, 8)
			if err != nil {
				return nil, err
			}
		}
		return sql.CreateDecimalType(uint8(precision), uint8(scale))
	case "bit":
		length := int64(1)
		if ct.Length != nil {
			var err error
			length, err = strconv.ParseInt(string(ct.Length.Val), 10, 8)
			if err != nil {
				return nil, err
			}
		}
		return sql.CreateBitType(uint8(length))
	case "tinyblob":
		return sql.TinyBlob, nil
	case "blob":
		if ct.Length == nil {
			return sql.Blob, nil
		}
		length, err := strconv.ParseInt(string(ct.Length.Val), 10, 64)
		if err != nil {
			return nil, err
		}
		return sql.CreateBinary(sqltypes.Blob, length)
	case "mediumblob":
		return sql.MediumBlob, nil
	case "longblob":
		return sql.LongBlob, nil
	case "tinytext":
		collation, err := sql.ParseCollation(&ct.Charset, &ct.Collate, false)
		if err != nil {
			return nil, err
		}
		return sql.CreateString(sqltypes.Text, tinyTextBlobMax/collation.CharacterSet().MaxLength(), collation)
	case "text":
		collation, err := sql.ParseCollation(&ct.Charset, &ct.Collate, false)
		if err != nil {
			return nil, err
		}
		if ct.Length == nil {
			return sql.CreateString(sqltypes.Text, textBlobMax/collation.CharacterSet().MaxLength(), collation)
		}
		length, err := strconv.ParseInt(string(ct.Length.Val), 10, 64)
		if err != nil {
			return nil, err
		}
		return sql.CreateString(sqltypes.Text, length, collation)
	case "mediumtext", "long", "long varchar":
		collation, err := sql.ParseCollation(&ct.Charset, &ct.Collate, false)
		if err != nil {
			return nil, err
		}
		return sql.CreateString(sqltypes.Text, mediumTextBlobMax/collation.CharacterSet().MaxLength(), collation)
	case "longtext":
		collation, err := sql.ParseCollation(&ct.Charset, &ct.Collate, false)
		if err != nil {
			return nil, err
		}
		return sql.CreateString(sqltypes.Text, longTextBlobMax/collation.CharacterSet().MaxLength(), collation)
	case "char", "character":
		collation, err := sql.ParseCollation(&ct.Charset, &ct.Collate, false)
		if err != nil {
			return nil, err
		}
		length := int64(1)
		if ct.Length != nil {
			var err error
			length, err = strconv.ParseInt(string(ct.Length.Val), 10, 64)
			if err != nil {
				return nil, err
			}
		}
		return sql.CreateString(sqltypes.Char, length, collation)
	case "nchar", "national char", "national character":
		length := int64(1)
		if ct.Length != nil {
			var err error
			length, err = strconv.ParseInt(string(ct.Length.Val), 10, 64)
			if err != nil {
				return nil, err
			}
		}
		return sql.CreateString(sqltypes.Char, length, sql.Collation_utf8mb3_general_ci)
	case "varchar", "character varying":
		collation, err := sql.ParseCollation(&ct.Charset, &ct.Collate, false)
		if err != nil {
			return nil, err
		}
		if ct.Length == nil {
			return nil, fmt.Errorf("VARCHAR requires a length")
		}
		length, err := strconv.ParseInt(string(ct.Length.Val), 10, 64)
		if err != nil {
			return nil, err
		}
		return sql.CreateString(sqltypes.VarChar, length, collation)
	case "nvarchar", "national varchar", "national character varying":
		if ct.Length == nil {
			return nil, fmt.Errorf("VARCHAR requires a length")
		}
		length, err := strconv.ParseInt(string(ct.Length.Val), 10, 64)
		if err != nil {
			return nil, err
		}
		return sql.CreateString(sqltypes.VarChar, length, sql.Collation_utf8mb3_general_ci)
	case "binary":
		length := int64(1)
		if ct.Length != nil {
			var err error
			length, err = strconv.ParseInt(string(ct.Length.Val), 10, 64)
			if err != nil {
				return nil, err
			}
		}
		return sql.CreateString(sqltypes.Binary, length, sql.Collation_binary)
	case "varbinary":
		if ct.Length == nil {
			return nil, fmt.Errorf("VARBINARY requires a length")
		}
		length, err := strconv.ParseInt(string(ct.Length.Val), 10, 64)
		if err != nil {
			return nil, err
		}
		return sql.CreateString(sqltypes.VarBinary, length, sql.Collation_binary)
	case "year":
		return sql.Year, nil
	case "date":
		return sql.Date, nil
	case "time":
		return sql.Time, nil
	case "timestamp":
		return sql.Timestamp, nil
	case "datetime":
		return sql.Datetime, nil
	case "enum":
		collation, err := sql.ParseCollation(&ct.Charset, &ct.Collate, false)
		if err != nil {
			return nil, err
		}
		return sql.CreateEnumType(ct.EnumValues, collation)
	case "set":
		collation, err := sql.ParseCollation(&ct.Charset, &ct.Collate, false)
		if err != nil {
			return nil, err
		}
		return sql.CreateSetType(ct.EnumValues, collation)
	case "json":
		return sql.JSON, nil
	case "geometry":
	case "geometrycollection":
	case "linestring":
	case "multilinestring":
	case "point":
	case "multipoint":
	case "polygon":
	case "multipolygon":
	default:
		return nil, fmt.Errorf("unknown type: %v", ct.Type)
	}
	return nil, fmt.Errorf("type not yet implemented: %v", ct.Type)
}
