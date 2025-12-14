package duckdbcapi

/*
#include <duckdb.h>
*/
import "C"
import (
	"unsafe"
)

type Result struct {
	c C.duckdb_result
}

func (r *Result) Destroy() {
	C.duckdb_destroy_result(&r.c)
}

func (r *Result) ColumnName(col uint64) (string, error) {
	name := C.duckdb_column_name(&r.c, C.idx_t(col))
	if name == nil {
		return "", ErrColumnNameNil
	}
	return C.GoString(name), nil
}

func (r *Result) ColumnType(col uint64) Type {
	return Type(C.duckdb_column_type(&r.c, C.idx_t(col)))
}

func (r *Result) ColumnData(col uint64) (unsafe.Pointer, error) {
	p := C.duckdb_column_data(&r.c, C.idx_t(col))
	if p == nil {
		return nil, ErrColumnDataNil
	}
	return p, nil
}

func (r *Result) ColumnCount() uint64 {
	return uint64(C.duckdb_column_count(&r.c))
}

func (r *Result) ColumnLogicalType(col uint64) *LogicalType {
	return &LogicalType{C.duckdb_column_logical_type(&r.c, C.idx_t(col))}
}

func (r *Result) ChunkCount() uint64 {
	return uint64(C.duckdb_result_chunk_count(r.c))
}

func (r *Result) Chunk(chunkId uint64) (*DataChunk, error) {
	chunk := C.duckdb_result_get_chunk(r.c, C.idx_t(chunkId))
	if chunk == nil {
		return nil, ErrDataChunkNil
	}
	return &DataChunk{c: chunk}, nil
}

func (r *Result) RowCount() uint64 {
	return uint64(C.duckdb_row_count(&r.c))
}

func (r *Result) RowsChanged() uint64 {
	return uint64(C.duckdb_rows_changed(&r.c))
}

func (r *Result) NullMaskData(col uint64) unsafe.Pointer {
	return unsafe.Pointer(C.duckdb_nullmask_data(&r.c, C.idx_t(col)))
}

func (r *Result) ValueIsNull(col, row uint64) bool {
	return bool(C.duckdb_value_is_null(&r.c, C.idx_t(col), C.idx_t(row)))
}

func (r *Result) ResultError() string {
	err := C.duckdb_result_error(&r.c)
	if err == nil {
		return ""
	}
	return C.GoString(err)
}

func (r *Result) ValueBoolean(col, row uint64) bool {
	return bool(C.duckdb_value_boolean(&r.c, C.idx_t(col), C.idx_t(row)))
}

func (r *Result) ValueInt8(col, row uint64) int8 {
	return int8(C.duckdb_value_int8(&r.c, C.idx_t(col), C.idx_t(row)))
}

func (r *Result) ValueInt16(col, row uint64) int16 {
	return int16(C.duckdb_value_int16(&r.c, C.idx_t(col), C.idx_t(row)))
}

func (r *Result) ValueInt32(col, row uint64) int32 {
	return int32(C.duckdb_value_int32(&r.c, C.idx_t(col), C.idx_t(row)))
}

func (r *Result) ValueInt64(col, row uint64) int64 {
	return int64(C.duckdb_value_int64(&r.c, C.idx_t(col), C.idx_t(row)))
}

func (r *Result) ValueUInt8(col, row uint64) uint8 {
	return uint8(C.duckdb_value_uint8(&r.c, C.idx_t(col), C.idx_t(row)))
}

func (r *Result) ValueUInt16(col, row uint64) uint16 {
	return uint16(C.duckdb_value_uint16(&r.c, C.idx_t(col), C.idx_t(row)))
}

func (r *Result) ValueUInt32(col, row uint64) uint32 {
	return uint32(C.duckdb_value_uint32(&r.c, C.idx_t(col), C.idx_t(row)))
}

func (r *Result) ValueUInt64(col, row uint64) uint64 {
	return uint64(C.duckdb_value_uint64(&r.c, C.idx_t(col), C.idx_t(row)))
}

func (r *Result) ValueFloat(col, row uint64) Float {
	return Float(C.duckdb_value_float(&r.c, C.idx_t(col), C.idx_t(row)))
}

func (r *Result) ValueHugeInt(col, row uint64) HugeInt {
	return HugeInt{C.duckdb_value_hugeint(&r.c, C.idx_t(col), C.idx_t(row))}
}

func (r *Result) ValueDouble(col, row uint64) Double {
	return Double(C.duckdb_value_double(&r.c, C.idx_t(col), C.idx_t(row)))
}

func (r *Result) ValueVarChar(col, row uint64) string {
	p := C.duckdb_value_varchar(&r.c, C.idx_t(col), C.idx_t(row))
	if p == nil {
		return ""
	}
	s := C.GoString(p)
	defer C.duckdb_free(unsafe.Pointer(p))
	return s
}

func (r *Result) ValueVarCharInternal(col, row uint64) string {
	p := C.duckdb_value_varchar_internal(&r.c, C.idx_t(col), C.idx_t(row))
	if p == nil {
		return ""
	}
	s := C.GoString(p)
	return s
}

func (r *Result) ValueDate(col, row uint64) Date {
	return Date{C.duckdb_value_date(&r.c, C.idx_t(col), C.idx_t(row))}
}

func (r *Result) ValueTime(col, row uint64) Time {
	return Time{C.duckdb_value_time(&r.c, C.idx_t(col), C.idx_t(row))}
}

func (r *Result) ValueTimestamp(col, row uint64) Timestamp {
	return Timestamp{C.duckdb_value_timestamp(&r.c, C.idx_t(col), C.idx_t(row))}
}

func (r *Result) ValueBlob(col, row uint64) Blob {
	return Blob{C.duckdb_value_blob(&r.c, C.idx_t(col), C.idx_t(row))}
}

func (r *Result) ValueDecimal(col, row uint64) Decimal {
	return Decimal{C.duckdb_value_decimal(&r.c, C.idx_t(col), C.idx_t(row))}
}

func (r *Result) ValueInterval(col, row uint64) Interval {
	return Interval{C.duckdb_value_interval(&r.c, C.idx_t(col), C.idx_t(row))}
}
