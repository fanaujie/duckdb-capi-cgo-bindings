package duckdbcapi

/*
#include <duckdb.h>
void callGoBridgeSetBindCallback(duckdb_bind_info info);
void callGoBridgeSetInitCallback(duckdb_init_info info);
void callGoBridgeSetFunctionCallback(duckdb_function_info info,duckdb_data_chunk output);
void callGoBridgeDeleteSetExtraInfoCallback(void *data);

*/
import "C"
import (
	"runtime/cgo"
	"unsafe"
)

type TableFunctionCallback interface {
	Bind(*BindInfo)
	Init(*InitInfo)
	Function(*FunctionInfo, *DataChunk)
}

//export goBridgeSetBindCallback
func goBridgeSetBindCallback(info C.duckdb_bind_info) {
	h := *(*cgo.Handle)(C.duckdb_bind_get_extra_info(info))
	val := h.Value().(TableFunctionCallback)
	val.Bind(&BindInfo{info})
}

//export goBridgeSetInitCallback
func goBridgeSetInitCallback(info C.duckdb_init_info) {
	h := *(*cgo.Handle)(C.duckdb_init_get_extra_info(info))
	val := h.Value().(TableFunctionCallback)
	val.Init(&InitInfo{info})
}

//export goBridgeSetFunctionCallback
func goBridgeSetFunctionCallback(info C.duckdb_function_info, dataChunk C.duckdb_data_chunk) {
	h := *(*cgo.Handle)(C.duckdb_function_get_extra_info(info))
	val := h.Value().(TableFunctionCallback)
	val.Function(&FunctionInfo{info}, &DataChunk{dataChunk})
}

//export goBridgeDeleteSetExtraInfoCallback
func goBridgeDeleteSetExtraInfoCallback(data unsafe.Pointer) {
	h := *(*cgo.Handle)(data)
	h.Delete()
}

//export goBridgeDeleteTableFunctionBindData
func goBridgeDeleteTableFunctionBindData(data unsafe.Pointer) {
	h := *(*cgo.Handle)(data)
	h.Delete()
}

//export goBridgeDeleteTableFunctionInitData
func goBridgeDeleteTableFunctionInitData(data unsafe.Pointer) {
	h := *(*cgo.Handle)(data)
	h.Delete()
}

type TableFunction struct {
	c C.duckdb_table_function
}

func CreateTableFunction() *TableFunction {
	return &TableFunction{C.duckdb_create_table_function()}
}

func (t *TableFunction) Destroy() {
	C.duckdb_destroy_table_function(&t.c)
}

func (t *TableFunction) SetName(name string) {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	C.duckdb_table_function_set_name(t.c, cName)
}

func (t *TableFunction) AddParameter(logicalType *LogicalType) {
	C.duckdb_table_function_add_parameter(t.c, logicalType.c)
}

func (t *TableFunction) SetCallback(callback TableFunctionCallback) {
	h := cgo.NewHandle(callback)
	C.duckdb_table_function_set_extra_info(t.c, unsafe.Pointer(&h), C.duckdb_delete_callback_t(C.callGoBridgeDeleteSetExtraInfoCallback))
	C.duckdb_table_function_set_bind(t.c, C.duckdb_table_function_bind_t(C.callGoBridgeSetBindCallback))
	C.duckdb_table_function_set_init(t.c, C.duckdb_table_function_init_t(C.callGoBridgeSetInitCallback))
	C.duckdb_table_function_set_function(t.c, C.duckdb_table_function_t(C.callGoBridgeSetFunctionCallback))
}

func (t *TableFunction) SupportsProjectionPushDown(pushDown bool) {
	C.duckdb_table_function_supports_projection_pushdown(t.c, C.bool(pushDown))
}
