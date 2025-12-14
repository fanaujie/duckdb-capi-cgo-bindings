package duckdbcapi

/*
#include <duckdb.h>
*/
import "C"
import (
	"errors"
	"unsafe"
)

type PreparedStatement struct {
	c C.duckdb_prepared_statement
}

func (p *PreparedStatement) PrepareError() error {
	err := C.duckdb_prepare_error(p.c)
	if err == nil {
		return nil
	}
	return errors.New(C.GoString(err))
}

func (p *PreparedStatement) Destroy() {
	C.duckdb_destroy_prepare(&p.c)
}

func (p *PreparedStatement) ExecutePrepared(result *Result) error {
	var pResult *C.duckdb_result
	if result != nil {
		pResult = &result.c
	}
	if C.duckdb_execute_prepared(p.c, pResult) == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (p *PreparedStatement) BindBoolean(paramIdx uint64, val bool) error {
	if C.duckdb_bind_boolean(p.c, C.idx_t(paramIdx), C.bool(val)) == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (p *PreparedStatement) BindInt8(paramIdx uint64, val int8) error {
	if C.duckdb_bind_int8(p.c, C.idx_t(paramIdx), C.schar(val)) == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (p *PreparedStatement) BindInt16(paramIdx uint64, val int16) error {
	if C.duckdb_bind_int16(p.c, C.idx_t(paramIdx), C.short(val)) == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (p *PreparedStatement) BindInt32(paramIdx uint64, val int32) error {
	if C.duckdb_bind_int32(p.c, C.idx_t(paramIdx), C.int(val)) == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (p *PreparedStatement) BindInt64(paramIdx uint64, val int64) error {
	if C.duckdb_bind_int64(p.c, C.idx_t(paramIdx), C.long(val)) == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (p *PreparedStatement) BindUInt8(paramIdx uint64, val uint8) error {
	if C.duckdb_bind_uint8(p.c, C.idx_t(paramIdx), C.uchar(val)) == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (p *PreparedStatement) BindUInt16(paramIdx uint64, val uint16) error {
	if C.duckdb_bind_uint16(p.c, C.idx_t(paramIdx), C.ushort(val)) == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (p *PreparedStatement) BindUInt32(paramIdx uint64, val uint32) error {
	if C.duckdb_bind_uint32(p.c, C.idx_t(paramIdx), C.uint(val)) == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (p *PreparedStatement) BindUInt64(paramIdx uint64, val uint64) error {
	if C.duckdb_bind_uint64(p.c, C.idx_t(paramIdx), C.ulong(val)) == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (p *PreparedStatement) BindVarChar(paramIdx uint64, val string) error {
	cVal := C.CString(val)
	defer C.free(unsafe.Pointer(cVal))
	if C.duckdb_bind_varchar(p.c, C.idx_t(paramIdx), cVal) == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}
func (p *PreparedStatement) BindVarCharLength(paramIdx uint64, val string, length uint64) error {
	cVal := C.CString(val)
	defer C.free(unsafe.Pointer(cVal))
	if C.duckdb_bind_varchar_length(p.c, C.idx_t(paramIdx), cVal, C.idx_t(length)) == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (p *PreparedStatement) BindBlob(paramIdx uint64, data []byte) error {
	pData := C.CBytes(data)
	defer C.free(unsafe.Pointer(pData))
	if C.duckdb_bind_blob(p.c, C.idx_t(paramIdx), pData, C.idx_t(len(data))) == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (p *PreparedStatement) BindNull(paramIdx uint64) error {
	if C.duckdb_bind_null(p.c, C.idx_t(paramIdx)) == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (p *PreparedStatement) BindHugeInt(paramIdx uint64, val HugeInt) error {
	if C.duckdb_bind_hugeint(p.c, C.idx_t(paramIdx), val.c) == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (p *PreparedStatement) BindFloat(paramIdx uint64, val Float) error {
	if C.duckdb_bind_float(p.c, C.idx_t(paramIdx), C.float(val)) == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (p *PreparedStatement) BindDouble(paramIdx uint64, val Double) error {
	if C.duckdb_bind_double(p.c, C.idx_t(paramIdx), C.double(val)) == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (p *PreparedStatement) BindDate(paramIdx uint64, val Date) error {
	if C.duckdb_bind_date(p.c, C.idx_t(paramIdx), val.c) == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (p *PreparedStatement) BindTime(paramIdx uint64, val Time) error {
	if C.duckdb_bind_time(p.c, C.idx_t(paramIdx), val.c) == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (p *PreparedStatement) BindTimestamp(paramIdx uint64, val Timestamp) error {
	if C.duckdb_bind_timestamp(p.c, C.idx_t(paramIdx), val.c) == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (p *PreparedStatement) BindInterval(paramIdx uint64, val Interval) error {
	if C.duckdb_bind_interval(p.c, C.idx_t(paramIdx), val.c) == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}

func (p *PreparedStatement) NParams() uint64 {
	return uint64(C.duckdb_nparams(p.c))
}
func (p *PreparedStatement) ParamType(paramIdx uint64) Type {
	return Type(C.duckdb_param_type(p.c, C.idx_t(paramIdx)))
}
