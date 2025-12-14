package duckdbcapi

/*
#include <duckdb.h>
#include <stdint.h>

void callGoBridgeReplacementCallback(duckdb_replacement_scan_info info, const char *table_name, void *data);
void callGoBridgeReplacementDeleteCallback(void *data);

*/
import "C"
import (
	"runtime/cgo"
	"unsafe"
)

type ReplacementScan interface {
	ReplacementScanCallback(info *ReplacementScanInfo, tableName string)
	DeleteCallback()
}

//export goBridgeReplacementCallback
func goBridgeReplacementCallback(info C.duckdb_replacement_scan_info, tableName *C.char, data unsafe.Pointer) {
	h := *(*cgo.Handle)(data)
	val := h.Value().(ReplacementScan)
	val.ReplacementScanCallback(&ReplacementScanInfo{info}, C.GoString(tableName))
}

//export goBridgeReplacementDeleteCallback
func goBridgeReplacementDeleteCallback(data unsafe.Pointer) {
	h := *(*cgo.Handle)(data)
	val := h.Value().(ReplacementScan)
	val.DeleteCallback()
	h.Delete()
}

func (d *DataBase) AddReplacementScan(replacementScan ReplacementScan) {
	h := cgo.NewHandle(replacementScan)
	C.duckdb_add_replacement_scan(d.c, C.duckdb_replacement_callback_t(C.callGoBridgeReplacementCallback),
		unsafe.Pointer(&h), C.duckdb_delete_callback_t(C.callGoBridgeReplacementDeleteCallback))
}

type ReplacementScanInfo struct {
	c C.duckdb_replacement_scan_info
}

func (r *ReplacementScanInfo) SetFunctionName(functionName string) {
	cFunctionName := C.CString(functionName)
	defer C.free(unsafe.Pointer(cFunctionName))
	C.duckdb_replacement_scan_set_function_name(r.c, cFunctionName)
}

func (r *ReplacementScanInfo) AddParameter(parameter *Value) {
	C.duckdb_replacement_scan_add_parameter(r.c, parameter.c)
}
