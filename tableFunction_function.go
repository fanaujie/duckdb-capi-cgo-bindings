package duckdbcapi

/*
#include <duckdb.h>
*/
import "C"
import (
	"runtime/cgo"
	"unsafe"
)

type FunctionInfo struct {
	c C.duckdb_function_info
}

func (f *FunctionInfo) GetExtraInfo() unsafe.Pointer {
	return unsafe.Pointer(C.duckdb_function_get_extra_info(f.c))
}

func (f *FunctionInfo) GetBindData() cgo.Handle {
	return *(*cgo.Handle)(C.duckdb_function_get_bind_data(f.c))
}
func (f *FunctionInfo) GetInitData() cgo.Handle {
	return *(*cgo.Handle)(C.duckdb_function_get_init_data(f.c))
}

func (f *FunctionInfo) SetError(errText string) {
	cErrText := C.CString(errText)
	defer C.free(unsafe.Pointer(cErrText))
	C.duckdb_function_set_error(f.c, cErrText)
}
