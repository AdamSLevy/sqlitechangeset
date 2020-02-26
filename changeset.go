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

// AlwaysUseBlob forces TEXT values to be encoded as hex, as a BLOB would be.
// This was added to address a potential bug in sqlite that causes BLOBs to be
// interpretted as TEXT.
var AlwaysUseBlob bool

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
	iter, err := sqlite.ChangesetIterStart(changeset)
	if err != nil {
		return
	}
	defer iter.Finalize()
	return ChangesetIterToSQL(conn, iter, false)
}

func ChangesetIterToSQL(conn *sqlite.Conn, iter sqlite.ChangesetIter,
	conflict bool) (sql string, err error) {
	Conn := _Conn{Conn: conn, ColumnNames: make(map[string][]string)}
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
		sqlLine, err = Conn.BuildSQL(iter, tbl, op, conflict)
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
	tbl string, op sqlite.OpType, conflict bool) (string, error) {
	names, err := conn.GetColNames(tbl)
	if err != nil {
		return "", err
	}
	switch op {
	case sqlite.SQLITE_INSERT:
		return buildInsert(iter, tbl, names, conflict)
	case sqlite.SQLITE_UPDATE:
		return buildUpdate(iter, tbl, names, conflict)
	case sqlite.SQLITE_DELETE:
		return buildDelete(iter, tbl, names, conflict)
	default:
		panic(fmt.Sprintf("unsupported OpType: %v", op))
	}
	return "", nil
}

const (
	_COLUMNF = `%q`
	_COMMA   = ", "
)

func buildInsert(iter sqlite.ChangesetIter,
	tbl string, names []string, conflict bool) (string, error) {
	const INSERTF = `INSERT INTO %q (%s) VALUES (%s)%s;
`
	var cols, vals, conf string
	for i, name := range names {
		v, err := iter.New(i)
		if err != nil {
			return "", err
		}
		if v.IsNil() {
			continue
		}
		cols += fmt.Sprintf(_COLUMNF+_COMMA, name)
		vals += valueString(v) + _COMMA
		if !conflict {
			continue
		}
		v, err = iter.Conflict(i)
		if err != nil {
			return "", err
		}
		conf += valueString(v) + _COMMA
	}
	cols = strings.TrimSuffix(cols, _COMMA)
	vals = strings.TrimSuffix(vals, _COMMA)
	if conflict {
		conf = strings.TrimSuffix(conf, _COMMA)
		conf = fmt.Sprintf(` /* conflict: (%s) */`, conf)
	}
	return fmt.Sprintf(INSERTF, tbl, cols, vals, conf), nil
}

func buildUpdate(iter sqlite.ChangesetIter,
	tbl string, names []string, conflict bool) (string, error) {
	const UPDATEF = `UPDATE %q SET (%s) = (%s) WHERE (%s) = (%s) /* old: (%s) %s*/;
`
	pk, err := iter.PK()
	if err != nil {
		return "", err
	}
	var setCols, setVals, oldVals, pkCols, pkVals, conf string
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
		if !conflict {
			continue
		}
		v, err := iter.Conflict(i)
		if err != nil {
			return "", err
		}
		conf += valueString(v) + _COMMA

	}
	setCols = strings.TrimSuffix(setCols, _COMMA)
	setVals = strings.TrimSuffix(setVals, _COMMA)
	oldVals = strings.TrimSuffix(oldVals, _COMMA)
	pkCols = strings.TrimSuffix(pkCols, _COMMA)
	pkVals = strings.TrimSuffix(pkVals, _COMMA)
	if conflict {
		conf = strings.TrimSuffix(conf, _COMMA)
		conf = fmt.Sprintf(`conflict: (%s) `, conf)
	}
	return fmt.Sprintf(UPDATEF, tbl, setCols, setVals, pkCols, pkVals, oldVals, conf), nil
}

func buildDelete(iter sqlite.ChangesetIter,
	tbl string, names []string, conflict bool) (string, error) {
	const DELETEF = `DELETE FROM %q WHERE (%s) = (%s) /* (%s) = (%s) %s*/;
`
	pk, err := iter.PK()
	if err != nil {
		return "", err
	}
	var pkCols, pkVals string
	var oldCols, oldVals string
	var conf string
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
		if !conflict {
			continue
		}
		v, err = iter.Conflict(i)
		if err != nil {
			return "", err
		}
		conf += valueString(v) + _COMMA

	}
	pkCols = strings.TrimSuffix(pkCols, _COMMA)
	pkVals = strings.TrimSuffix(pkVals, _COMMA)
	oldCols = strings.TrimSuffix(oldCols, _COMMA)
	oldVals = strings.TrimSuffix(oldVals, _COMMA)
	if conflict {
		conf = strings.TrimSuffix(conf, _COMMA)
		conf = fmt.Sprintf(`conflict: (%s) `, conf)
	}
	return fmt.Sprintf(DELETEF, tbl, pkCols, pkVals, oldCols, oldVals, conf), nil
}

func valueString(val sqlite.Value) string {
	valType := val.Type()
	switch valType {
	case sqlite.SQLITE_INTEGER:
		return fmt.Sprintf("%v", val.Int64())
	case sqlite.SQLITE_FLOAT:
		return fmt.Sprintf("%v", val.Float())
	case sqlite.SQLITE_TEXT:
		if !AlwaysUseBlob {
			return fmt.Sprintf("'%v'", strings.ReplaceAll(val.Text(), "'", "''"))
		}
		fallthrough
	case sqlite.SQLITE_BLOB:
		return fmt.Sprintf("X'%X'", val.Blob())
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
