package duckdbcapi

/*
#include <duckdb.h>
*/
import "C"
import (
	"unsafe"
)

type SimpleDataType interface {
	bool | int8 | int16 | int32 | int64 | uint8 |
		uint16 | uint32 | uint64 | Float | Double |
		HugeInt | Date | Time | Timestamp | Blob |
		Interval
}

func UnsafeSimpleDataToSlice[T SimpleDataType](pSimpleData unsafe.Pointer, length uint64) []T {
	return unsafe.Slice((*T)(pSimpleData), length)
}
