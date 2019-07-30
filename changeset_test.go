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
	"testing"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToSQL(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	// Applying the returned changeset to conn should result in the session
	// having an empty changeset.
	conn, sess, changeset := createChangeset(t)
	defer conn.Close()
	defer sess.Delete()

	// Convert changeset to SQL.
	sql, err := ToSQL(conn, changeset)
	require.NoError(err, "ToSQL")
	require.NotEmpty(sql, "ToSQL")
	fmt.Println(sql)

	// Apply changeset via SQL.
	require.NoError(sqlitex.ExecScript(conn, sql))

	// Ensure that the sess now has no change.
	empty := &bytes.Buffer{}
	require.NoError(sess.Changeset(empty), "sqlite.Session.Changeset()")
	if assert.Empty(empty.Bytes()) {
		return
	}
	// Print the SQL of sess' changeset, which should have been empty.
	emptySQL, err := ToSQL(conn, empty)
	require.NoError(err, "ToSQL")
	assert.Empty(emptySQL)
}

func createChangeset(t *testing.T) (*sqlite.Conn, *sqlite.Session, io.Reader) {
	require := require.New(t)
	conn, err := sqlite.OpenConn(":memory:", 0)
	require.NoError(err, "sqlite.OpenConn()")

	require.NoError(sqlitex.ExecScript(conn, `
		CREATE TABLE t (
                        a INTEGER,
                        b INTEGER,
                        c TEXT,
                        d DOUBLE,
                        PRIMARY KEY (a, b)
                );
		CREATE TABLE t2 (
                        a INTEGER PRIMARY KEY,
                        b BLOB
                );
                INSERT INTO t (a, b, c, d) VALUES (1, 1, "hello", 1.5);
                INSERT INTO t (a, b, c, d) VALUES (2, 2, "world", 1.5);
                INSERT INTO t (a, b, c, d) VALUES (5, 5, "world", 1.5);
                INSERT INTO t2 (a, b) VALUES (1, x'01ff');
                INSERT INTO t2 (a, b) VALUES (2, x'02ff');`))

	sess, err := conn.CreateSession("")
	require.NoError(err, "sqlite.Conn.CreateSession()")
	defer sess.Delete()
	require.NoError(sess.Attach(""), "sqlite.Session.Attach()")

	require.NoError(sqlitex.ExecScript(conn, `
                UPDATE t SET c = "hello world" WHERE a = 1 AND b = 1;
                INSERT INTO t (a, b, c) VALUES (3, 3, "goodbye world");
                UPDATE t SET (c,d) = ("world hello", 5.25) WHERE a = 2 AND b = 2;
                DELETE FROM t WHERE (a, b) = (5, 5);
                INSERT INTO t (a, b, c) VALUES (4, 4, 'goodbye world''');
                UPDATE t2 SET (a, b) = (0, x'ffffff') WHERE rowid = 1;
                DELETE FROM t2 WHERE rowid = 2;
                `))

	// Save original changeset
	var changeset, changesetRet bytes.Buffer
	writer := io.MultiWriter(&changeset, &changesetRet)
	require.NoError(sess.Changeset(writer))
	require.NotEmpty(changeset.Bytes())

	// Start new session, so we can track if we return the database to this
	// state after applying the generated SQL.
	inverseSess, err := conn.CreateSession("")
	require.NoError(err, "sqlite.Conn.CreateSession()")
	require.NoError(inverseSess.Attach(""), "sqlite.Session.Attach()")

	// Manually rollback changeset, so we can apply it again with the
	// generated SQL.
	var inverseApply, inverse bytes.Buffer
	writer = io.MultiWriter(&inverseApply, &inverse)
	require.NoError(sqlite.ChangesetInvert(writer, &changeset))
	require.NotEmpty(inverse.Bytes())
	require.NotEmpty(inverseApply.Bytes())
	require.NoError(conn.ChangesetApply(&inverseApply, nil, nil))

	// The original session's changeset should be empty now.
	empty := &bytes.Buffer{}
	require.NoError(sess.Changeset(empty))
	require.Empty(empty.Bytes())

	// The new session's changeset should be equal to the inverse now.
	notEmpty := &bytes.Buffer{}
	require.NoError(inverseSess.Changeset(notEmpty))
	require.Equal(notEmpty.Bytes(), inverse.Bytes())

	return conn, inverseSess, &changesetRet
}
