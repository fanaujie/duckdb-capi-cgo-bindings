package duckdbcapi

/*
#include <duckdb.h>
*/
import "C"
import "unsafe"

type Appender struct {
	c C.duckdb_appender
}

func (a *Appender) Destroy() error {
	if err := C.duckdb_appender_destroy(&a.c); err == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (a *Appender) Close() error {
	if err := C.duckdb_appender_close(a.c); err == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (a *Appender) Error() string {
	err := C.duckdb_appender_error(a.c)
	if err == nil {
		return ""
	}
	return C.GoString(err)
}

func (a *Appender) BeginRow() error {
	if err := C.duckdb_appender_begin_row(a.c); err == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}
func (a *Appender) EndRow() error {
	if err := C.duckdb_appender_end_row(a.c); err == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}
func (a *Appender) Flush() error {
	if err := C.duckdb_appender_flush(a.c); err == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (a *Appender) AppendBool(v bool) error {
	if err := C.duckdb_append_bool(a.c, C.bool(v)); err == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (a *Appender) AppendInt8(v int8) error {
	if err := C.duckdb_append_int8(a.c, C.schar(v)); err == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (a *Appender) AppendInt16(v int16) error {
	if err := C.duckdb_append_int16(a.c, C.short(v)); err == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (a *Appender) AppendInt32(v int32) error {
	if err := C.duckdb_append_int32(a.c, C.int(v)); err == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (a *Appender) AppendInt64(v int64) error {
	if err := C.duckdb_append_int64(a.c, C.long(v)); err == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (a *Appender) AppendUInt8(v uint8) error {
	if err := C.duckdb_append_uint8(a.c, C.uchar(v)); err == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (a *Appender) AppendUInt16(v uint16) error {
	if err := C.duckdb_append_uint16(a.c, C.ushort(v)); err == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (a *Appender) AppendUInt32(v uint32) error {
	if err := C.duckdb_append_uint32(a.c, C.uint(v)); err == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (a *Appender) AppendUInt64(v uint64) error {
	if err := C.duckdb_append_uint64(a.c, C.ulong(v)); err == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (a *Appender) AppendFloat(v Float) error {
	if err := C.duckdb_append_float(a.c, C.float(v)); err == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (a *Appender) AppendDouble(v Double) error {
	if err := C.duckdb_append_double(a.c, C.double(v)); err == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (a *Appender) AppendVarChar(v string) error {
	cV := C.CString(v)
	defer C.free(unsafe.Pointer(cV))
	if err := C.duckdb_append_varchar(a.c, cV); err == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (a *Appender) AppendVarCharLength(v string, length uint64) error {
	cV := C.CString(v)
	defer C.free(unsafe.Pointer(cV))
	if err := C.duckdb_append_varchar_length(a.c, cV, C.idx_t(length)); err == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (a *Appender) AppendBlob(data []byte) error {
	pData := C.CBytes(data)
	defer C.free(unsafe.Pointer(pData))
	if err := C.duckdb_append_blob(a.c, pData, C.idx_t(len(data))); err == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (a *Appender) AppendDate(date Date) error {
	if err := C.duckdb_append_date(a.c, date.c); err == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (a *Appender) AppendTime(time Time) error {
	if err := C.duckdb_append_time(a.c, time.c); err == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (a *Appender) AppendTimestamp(timestamp Timestamp) error {
	if err := C.duckdb_append_timestamp(a.c, timestamp.c); err == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (a *Appender) AppendInterval(interval Interval) error {
	if err := C.duckdb_append_interval(a.c, interval.c); err == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (a *Appender) AppendHugeInt(hugeInt HugeInt) error {
	if err := C.duckdb_append_hugeint(a.c, hugeInt.c); err == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (a *Appender) AppendNull() error {
	if err := C.duckdb_append_null(a.c); err == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (a *Appender) AppendDataChunk(chunk *DataChunk) error {
	if err := C.duckdb_append_data_chunk(a.c, chunk.c); err == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}
