package duckdbcapi

/*
#include <duckdb.h>
*/
import "C"
import "unsafe"

type Value struct {
	c C.duckdb_value
}

func CreateVarchar(text string) *Value {
	cText := C.CString(text)
	defer C.free(unsafe.Pointer(cText))
	return &Value{C.duckdb_create_varchar(cText)}
}

func CreateVarCharLength(text string, length uint64) *Value {
	cText := C.CString(text)
	defer C.free(unsafe.Pointer(cText))
	return &Value{C.duckdb_create_varchar_length(cText, C.idx_t(length))}
}

func CreateInt64(val int64) *Value {
	return &Value{C.duckdb_create_int64(C.long(val))}
}

func (v *Value) GetVarChar() string {
	return C.GoString(C.duckdb_get_varchar(v.c))
}

func (v *Value) GetInt64() int64 {
	return int64(C.duckdb_get_int64(v.c))
}
func (v *Value) Destroy() {
	C.duckdb_destroy_value(&v.c)
}
