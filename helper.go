package duckdbcapi

/*
#include <duckdb.h>
*/
import "C"
import (
	"unsafe"
)

func Malloc(size uint) unsafe.Pointer {
	return unsafe.Pointer(C.duckdb_malloc(C.size_t(size)))
}

func Free(p unsafe.Pointer) {
	C.duckdb_free(p)
}

func VectorSize() uint64 {
	return uint64(C.duckdb_vector_size())
}

func FromDate(date Date) DateStruct {
	return DateStruct{C.duckdb_from_date(date.c)}
}

func ToDate(dateStruct DateStruct) Date {
	return Date{C.duckdb_to_date(dateStruct.c)}
}

func FromTime(time Time) TimeStruct {
	return TimeStruct{C.duckdb_from_time(time.c)}
}

func ToTime(time TimeStruct) Time {
	return Time{C.duckdb_to_time(time.c)}
}

func FromTimestamp(time Timestamp) TimestampStruct {
	return TimestampStruct{C.duckdb_from_timestamp(time.c)}
}

func ToTimestamp(time TimestampStruct) Timestamp {
	return Timestamp{C.duckdb_to_timestamp(time.c)}
}

func HugeIntToDouble(hugeInt HugeInt) Double {
	return Double(C.duckdb_hugeint_to_double(hugeInt.c))
}

func DoubleToHugeInt(v Double) HugeInt {
	return HugeInt{C.duckdb_double_to_hugeint(C.double(v))}
}

func DecimalToDouble(decimal Decimal) Double {
	return Double(C.duckdb_decimal_to_double(decimal.c))
}
