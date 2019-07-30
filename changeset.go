// Copyright 2019 Adam S Levy <adam@aslevy.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to
// deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
// sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.

package sqlitechangeset

import (
	"fmt"
	"io"
	"strings"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
	"github.com/Factom-Asset-Tokens/fatd/factom"
)

// ToSQL converts changeset, which may also be a patchset, into the equivalent
// SQL statements. The column names are queried from the database connected to
// by sqliteConn.
func ToSQL(sqliteConn *sqlite.Conn, changeset io.Reader) (sql string, err error) {
	conn := _Conn{Conn: sqliteConn, ColumnNames: make(map[string][]string)}
	iter, err := sqlite.ChangesetIterStart(changeset)
	if err != nil {
		return
	}
	defer iter.Finalize()
	for {
		var hasRow bool
		hasRow, err = iter.Next()
		if err != nil {
			return
		}
		if !hasRow {
			break
		}
		var tbl string
		var op sqlite.OpType
		tbl, _, op, _, err = iter.Op()
		if err != nil {
			return
		}
		var sqlLine string
		sqlLine, err = conn.BuildSQL(iter, tbl, op)
		if err != nil {
			return
		}
		sql += sqlLine
	}
	return
}

const (
	_TABLE_INFOF = `PRAGMA TABLE_INFO('%s');`

	_INSERTF = "INSERT INTO '%s' (%s) VALUES (%s);\n"
	_UPDATEF = "UPDATE '%s' SET (%s) = (%s) WHERE (%s) = (%s);\n"
	_DELETEF = "DELETE FROM '%s' WHERE (%s) = (%s);\n"

	_COLUMNF = "'%s'"
	_COMMA   = ", "
)

var sprintf = fmt.Sprintf

type _Conn struct {
	*sqlite.Conn
	ColumnNames map[string][]string
}

func (conn _Conn) BuildSQL(iter sqlite.ChangesetIter,
	tbl string, op sqlite.OpType) (string, error) {
	names, err := conn.GetColNames(tbl)
	if err != nil {
		return "", err
	}
	switch op {
	case sqlite.SQLITE_INSERT:
		return buildInsert(iter, tbl, names)
	case sqlite.SQLITE_UPDATE:
		return buildUpdate(iter, tbl, names)
	case sqlite.SQLITE_DELETE:
		return buildDelete(iter, tbl, names)
	default:
		panic(sprintf("unsupported OpType: %v", op))
	}
	return "", nil
}

func buildInsert(iter sqlite.ChangesetIter,
	tbl string, names []string) (string, error) {
	var cols, vals string
	for i, name := range names {
		v, err := iter.New(i)
		if err != nil {
			return "", nil
		}
		if v.IsNil() {
			continue
		}
		cols += sprintf(_COLUMNF, name) + _COMMA
		vals += valueString(v) + _COMMA
	}
	cols = strings.TrimSuffix(cols, _COMMA)
	vals = strings.TrimSuffix(vals, _COMMA)
	return sprintf(_INSERTF, tbl, cols, vals), nil
}

func buildUpdate(iter sqlite.ChangesetIter,
	tbl string, names []string) (string, error) {
	pk, err := iter.PK()
	if err != nil {
		return "", err
	}
	var setCols, setVals, pkCols, pkVals string
	for i, name := range names {
		if pk[i] {
			v, err := iter.Old(i)
			if err != nil {
				return "", err
			}
			pkCols += sprintf(_COLUMNF, name) + _COMMA
			pkVals += valueString(v) + _COMMA
			continue
		}
		v, err := iter.New(i)
		if err != nil {
			return "", err
		}
		if v.IsNil() {
			continue
		}
		setCols += sprintf(_COLUMNF, name) + _COMMA
		setVals += valueString(v) + _COMMA
	}
	setCols = strings.TrimSuffix(setCols, _COMMA)
	setVals = strings.TrimSuffix(setVals, _COMMA)
	pkCols = strings.TrimSuffix(pkCols, _COMMA)
	pkVals = strings.TrimSuffix(pkVals, _COMMA)
	return sprintf(_UPDATEF, tbl, setCols, setVals, pkCols, pkVals), nil
}

func buildDelete(iter sqlite.ChangesetIter,
	tbl string, names []string) (string, error) {
	pk, err := iter.PK()
	if err != nil {
		return "", err
	}
	var pkCols, pkVals string
	for i, name := range names {
		if !pk[i] {
			continue
		}
		v, err := iter.Old(i)
		if err != nil {
			return "", err
		}
		pkCols += sprintf(_COLUMNF, name) + _COMMA
		pkVals += valueString(v) + _COMMA
	}
	pkCols = strings.TrimSuffix(pkCols, _COMMA)
	pkVals = strings.TrimSuffix(pkVals, _COMMA)
	return sprintf(_DELETEF, tbl, pkCols, pkVals), nil
}

const _HEXF = "x'%v'"
const _TEXTF = "%q"

func valueString(val sqlite.Value) string {
	valType := val.Type()
	switch valType {
	case sqlite.SQLITE_INTEGER:
		return sprintf("%v", val.Int64())
	case sqlite.SQLITE_FLOAT:
		return sprintf("%v", val.Float())
	case sqlite.SQLITE_TEXT:
		return sprintf(_TEXTF, val.Text())
	case sqlite.SQLITE_BLOB:
		return sprintf(_HEXF, factom.Bytes(val.Blob()))
	case sqlite.SQLITE_NULL:
		return "NULL"
	default:
		panic(sprintf("unsupported ColumnType: %v", valType))
	}
}

func (conn _Conn) GetColNames(tbl string) ([]string, error) {
	colNames, ok := conn.ColumnNames[tbl]
	if ok {
		return colNames, nil
	}
	err := sqlitex.Exec(conn.Conn, sprintf(_TABLE_INFOF, tbl),
		func(stmt *sqlite.Stmt) error {
			colNames = append(colNames, stmt.ColumnText(1))
			return nil
		})
	if err != nil {
		return nil, err
	}
	conn.ColumnNames[tbl] = colNames
	return colNames, nil
}
