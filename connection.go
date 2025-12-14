package duckdbcapi

/*
#include <duckdb.h>
*/
import "C"
import "unsafe"

type Connection struct {
	c C.duckdb_connection
}

func (c *Connection) Disconnect() {
	C.duckdb_disconnect(&c.c)
}

func (c *Connection) Query(query string, result *Result) error {
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))
	var pResult *C.duckdb_result = nil
	if result != nil {
		pResult = &result.c
	}
	if err := C.duckdb_query(c.c, cQuery, pResult); err == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (c *Connection) Prepare(query string, stmt *PreparedStatement) error {
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	if C.duckdb_prepare(c.c, cQuery, &stmt.c) == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (c *Connection) AppenderCreate(schema, table string) (*Appender, error) {
	cSchema := C.CString(schema)
	defer C.free(unsafe.Pointer(cSchema))
	cTable := C.CString(table)
	defer C.free(unsafe.Pointer(cTable))
	a := &Appender{}
	if C.duckdb_appender_create(c.c, cSchema, cTable, &a.c) == C.DuckDBError {
		return a, ErrDuckDBError
	}
	return a, nil
}

func (c *Connection) RegisterTableFunction(function *TableFunction) error {
	if C.duckdb_register_table_function(c.c, function.c) == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}
