package duckdbcapi

/*
#include <duckdb.h>
*/
import "C"
import (
	"unsafe"
)

type DataBase struct {
	c C.duckdb_database
}

func Open(path string) (*DataBase, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	var db C.duckdb_database
	if state := C.duckdb_open(cPath, &db); state == C.DuckDBError {
		return nil, ErrDuckDBError
	}
	return &DataBase{
		c: db,
	}, nil
}

func OpenExt(path string, config *Config) (*DataBase, string, error) {
	var db C.duckdb_database
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	var cError *C.char
	defer C.duckdb_free(unsafe.Pointer(cError))
	if state := C.duckdb_open_ext(cPath, &db, config.c, &cError); state == C.DuckDBError {
		return nil, C.GoString(cError), ErrDuckDBError
	}
	return &DataBase{
		c: db,
	}, "", nil
}

func (d *DataBase) Close() {
	C.duckdb_close(&d.c)
}

func (d *DataBase) Connection() (*Connection, error) {
	var c C.duckdb_connection
	if C.duckdb_connect(d.c, &c) == C.DuckDBError {
		return nil, ErrDuckDBError
	}
	return &Connection{c}, nil
}
