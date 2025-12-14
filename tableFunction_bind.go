package duckdbcapi

/*
#include <duckdb.h>
void callGoBridgeDeleteTableFunctionBindData(void *data);
*/
import "C"
import (
	"runtime/cgo"
	"unsafe"
)

type BindInfo struct {
	c C.duckdb_bind_info
}

func (b *BindInfo) GetExtraInfo() unsafe.Pointer {
	return unsafe.Pointer(C.duckdb_bind_get_extra_info(b.c))
}

func (b *BindInfo) AddResultColumn(name string, logicalType *LogicalType) {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	C.duckdb_bind_add_result_column(b.c, cName, logicalType.c)
}

func (b *BindInfo) GetParameterCount() uint64 {
	return uint64(C.duckdb_bind_get_parameter_count(b.c))
}

func (b *BindInfo) GetParameter(index uint64) *Value {
	return &Value{C.duckdb_bind_get_parameter(b.c, C.idx_t(index))}
}

func (b *BindInfo) SetBindData(bindData cgo.Handle) {
	C.duckdb_bind_set_bind_data(b.c, unsafe.Pointer(&bindData), C.duckdb_delete_callback_t(C.callGoBridgeDeleteTableFunctionBindData))
}

func (b *BindInfo) SetError(errText string) {
	cErrText := C.CString(errText)
	defer C.free(unsafe.Pointer(cErrText))
	C.duckdb_bind_set_error(b.c, cErrText)
}
