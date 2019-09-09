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
	"bytes"
	"fmt"
	"io"
	"strings"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
)

var opIndex = map[sqlite.OpType]int{
	sqlite.SQLITE_INSERT: 0,
	sqlite.SQLITE_UPDATE: 1,
	sqlite.SQLITE_DELETE: 2,
}

func SessionToSQL(conn *sqlite.Conn, sess *sqlite.Session) (sql string, err error) {
	changeset := &bytes.Buffer{}
	if err = sess.Changeset(changeset); err != nil {
		return
	}
	return ToSQL(conn, changeset)
}

// ToSQL converts changeset, which may also be a patchset, into the equivalent
// SQL statements. The column names are queried from the database connected to
// by sqliteConn.
func ToSQL(conn *sqlite.Conn, changeset io.Reader) (sql string, err error) {
	Conn := _Conn{Conn: conn, ColumnNames: make(map[string][]string)}
	iter, err := sqlite.ChangesetIterStart(changeset)
	if err != nil {
		return
	}
	defer iter.Finalize()
	// We later group all statements by table and operation.
	tableIDs := map[string]int{}
	tableOps := [][][]string{}
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
		sqlLine, err = Conn.BuildSQL(iter, tbl, op)
		if err != nil {
			return
		}
		tblID, ok := tableIDs[tbl]
		if !ok {
			tblID = len(tableOps)
			tableIDs[tbl] = tblID
			tableOps = append(tableOps, make([][]string, 3))
		}
		opID := opIndex[op]
		tableOps[tblID][opID] = append(tableOps[tblID][opID], sqlLine)
	}

	// For each table...
	for _, ops := range tableOps {
		// For each op...
		for _, op := range ops {
			// Append each line.
			for _, line := range op {
				sql += line
			}
		}
		sql += "\n"
	}
	sql = strings.TrimSuffix(sql, "\n")
	return
}

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
		panic(fmt.Sprintf("unsupported OpType: %v", op))
	}
	return "", nil
}

const (
	_COLUMNF = `"%s"`
	_COMMA   = ", "
)

func buildInsert(iter sqlite.ChangesetIter,
	tbl string, names []string) (string, error) {
	const INSERTF = `INSERT INTO "%s" (%s) VALUES (%s);
`
	var cols, vals string
	for i, name := range names {
		v, err := iter.New(i)
		if err != nil {
			return "", nil
		}
		if v.IsNil() {
			continue
		}
		cols += fmt.Sprintf(_COLUMNF, name) + _COMMA
		vals += valueString(v) + _COMMA
	}
	cols = strings.TrimSuffix(cols, _COMMA)
	vals = strings.TrimSuffix(vals, _COMMA)
	return fmt.Sprintf(INSERTF, tbl, cols, vals), nil
}

func buildUpdate(iter sqlite.ChangesetIter,
	tbl string, names []string) (string, error) {
	const UPDATEF = `UPDATE "%s" SET (%s) = (%s) WHERE (%s) = (%s) /* (%v) */;
`
	pk, err := iter.PK()
	if err != nil {
		return "", err
	}
	var setCols, setVals, oldVals, pkCols, pkVals string
	for i, name := range names {
		vOld, err := iter.Old(i)
		if err != nil {
			return "", err
		}
		if pk[i] {
			pkCols += fmt.Sprintf(_COLUMNF, name) + _COMMA
			pkVals += valueString(vOld) + _COMMA
			continue
		}
		vNew, err := iter.New(i)
		if err != nil {
			return "", err
		}
		if vNew.IsNil() {
			continue
		}
		setCols += fmt.Sprintf(_COLUMNF, name) + _COMMA
		setVals += valueString(vNew) + _COMMA
		oldVals += valueString(vOld) + _COMMA
	}
	setCols = strings.TrimSuffix(setCols, _COMMA)
	setVals = strings.TrimSuffix(setVals, _COMMA)
	oldVals = strings.TrimSuffix(oldVals, _COMMA)
	pkCols = strings.TrimSuffix(pkCols, _COMMA)
	pkVals = strings.TrimSuffix(pkVals, _COMMA)
	return fmt.Sprintf(UPDATEF, tbl, setCols, setVals, pkCols, pkVals, oldVals), nil
}

func buildDelete(iter sqlite.ChangesetIter,
	tbl string, names []string) (string, error) {
	const DELETEF = `DELETE FROM "%s" WHERE (%s) = (%s) /* (%v) = (%v) */;
`
	pk, err := iter.PK()
	if err != nil {
		return "", err
	}
	var pkCols, pkVals string
	var oldCols, oldVals string
	for i, name := range names {
		v, err := iter.Old(i)
		if err != nil {
			return "", err
		}
		if pk[i] {
			pkCols += fmt.Sprintf(_COLUMNF, name) + _COMMA
			pkVals += valueString(v) + _COMMA
			continue
		}
		oldCols += fmt.Sprintf(_COLUMNF, name) + _COMMA
		oldVals += valueString(v) + _COMMA

	}
	pkCols = strings.TrimSuffix(pkCols, _COMMA)
	pkVals = strings.TrimSuffix(pkVals, _COMMA)
	oldCols = strings.TrimSuffix(oldCols, _COMMA)
	oldVals = strings.TrimSuffix(oldVals, _COMMA)
	return fmt.Sprintf(DELETEF, tbl, pkCols, pkVals, oldCols, oldVals), nil
}

func valueString(val sqlite.Value) string {
	valType := val.Type()
	switch valType {
	case sqlite.SQLITE_INTEGER:
		return fmt.Sprintf("%v", val.Int64())
	case sqlite.SQLITE_FLOAT:
		return fmt.Sprintf("%v", val.Float())
	case sqlite.SQLITE_BLOB:
		return fmt.Sprintf("X'%X'", val.Blob())
	case sqlite.SQLITE_TEXT:
		return fmt.Sprintf("'%v'", strings.ReplaceAll(val.Text(), "'", "''"))
	case sqlite.SQLITE_NULL:
		return "NULL"
	default:
		panic(fmt.Sprintf("unsupported ColumnType: %v", valType))
	}
}

func (conn _Conn) GetColNames(tbl string) ([]string, error) {
	const TABLE_INFOF = `PRAGMA TABLE_INFO("%s");`
	colNames, ok := conn.ColumnNames[tbl]
	if ok {
		return colNames, nil
	}
	err := sqlitex.Exec(conn.Conn, fmt.Sprintf(TABLE_INFOF, tbl),
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
