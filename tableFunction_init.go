package duckdbcapi

/*
#include <duckdb.h>
void callGoBridgeDeleteTableFunctionInitData(void *data);
*/
import "C"
import (
	"runtime/cgo"
	"unsafe"
)

type InitInfo struct {
	c C.duckdb_init_info
}

func (i *InitInfo) GetExtraInfo() unsafe.Pointer {
	return unsafe.Pointer(C.duckdb_init_get_extra_info(i.c))
}

func (i *InitInfo) GetBindData() cgo.Handle {
	return *(*cgo.Handle)(C.duckdb_init_get_bind_data(i.c))
}
func (i *InitInfo) SetInitData(initData cgo.Handle) {
	C.duckdb_init_set_init_data(i.c, unsafe.Pointer(&initData), C.duckdb_delete_callback_t(C.callGoBridgeDeleteTableFunctionInitData))
}

func (i *InitInfo) GetColumnCount() uint64 {
	return uint64(C.duckdb_init_get_column_count(i.c))
}

func (i *InitInfo) GetColumnIndex(columnIndex uint64) uint64 {
	return uint64(C.duckdb_init_get_column_index(i.c, C.idx_t(columnIndex)))
}

func (i *InitInfo) SetError(errText string) {
	cErrText := C.CString(errText)
	defer C.free(unsafe.Pointer(cErrText))
	C.duckdb_init_set_error(i.c, cErrText)
}
