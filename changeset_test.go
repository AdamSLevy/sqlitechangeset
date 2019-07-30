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
	"io"
	"testing"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
	"github.com/stretchr/testify/require"
)

const expectedSQL = `UPDATE 't' SET ('c') = ("hello world") WHERE ('a', 'b') = (1, 1);
INSERT INTO 't' ('a', 'b', 'c') VALUES (3, 3, "goodbye world");
UPDATE 't' SET ('c') = ("world hello") WHERE ('a', 'b') = (2, 2);
DELETE FROM 't' WHERE ('a', 'b') = (5, 5);
INSERT INTO 't' ('a', 'b', 'c') VALUES (4, 4, "goodbye world");
`

func TestToSQL(t *testing.T) {
	conn, changeset := createChangeset(t)
	defer conn.Close()
	sql, err := ToSQL(conn, changeset)
	require.NoError(t, err)
	require.Equal(t, expectedSQL, sql)
}

func createChangeset(t *testing.T) (*sqlite.Conn, io.Reader) {
	require := require.New(t)
	conn, err := sqlite.OpenConn(":memory:", 0)
	require.NoError(err, "sqlite.OpenConn()")

	require.NoError(sqlitex.ExecScript(conn, `
		CREATE TABLE t (
                        a INTEGER,
                        b INTEGER,
                        c TEXT,
                        PRIMARY KEY (a, b)
                );
                INSERT INTO t (a, b, c) VALUES (1, 1, "hello");
                INSERT INTO t (a, b, c) VALUES (2, 2, "world");
                INSERT INTO t (a, b, c) VALUES (5, 5, "world");`))

	sess, err := conn.CreateSession("")
	require.NoError(err, "sqlite.Conn.CreateSession()")
	defer sess.Delete()
	require.NoError(sess.Attach(""), "sqlite.Session.Attach()")

	require.NoError(sqlitex.ExecScript(conn, `
                UPDATE t SET c = "hello world" WHERE a = 1 AND b = 1;
                UPDATE t SET c = "world hello" WHERE a = 2 AND b = 2;
                INSERT INTO t (a, b, c) VALUES (3, 3, "goodbye world");
                INSERT INTO t (a, b, c) VALUES (4, 4, "goodbye world");
                DELETE FROM t WHERE (a, b) = (5, 5);`))

	buf := bytes.NewBuffer([]byte{})
	require.NoError(sess.Changeset(buf))
	return conn, buf
}
