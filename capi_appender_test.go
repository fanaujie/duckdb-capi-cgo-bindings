package duckdbcapi

/*
reference from https://github.com/duckdb/duckdb/blob/master/test/api/capi/test_capi_appender.cpp
*/

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAppenderStatementsInCAPI(t *testing.T) {
	var tester CAPITester

	// open the database in in-memory mode
	assert.Equal(t, true, tester.OpenDatabase(""))
	defer tester.CleanUp()

	assert.Nil(t, tester.NoResultQuery("CREATE TABLE test (i INTEGER, d double, s string)"))

	appender, err := tester.conn.AppenderCreate("", "nonexistant-table")
	assert.Equal(t, ErrDuckDBError, err)

	assert.NotEqual(t, uintptr(0), uintptr(appender.c))
	assert.NotEqual(t, "", appender.Error())
	assert.Nil(t, appender.Destroy())

	appender, err = tester.conn.AppenderCreate("", "test")
	assert.Nil(t, err)
	assert.Equal(t, "", appender.Error())

	assert.Nil(t, appender.BeginRow())
	assert.Nil(t, appender.AppendInt32(42))
	assert.Nil(t, appender.AppendDouble(4.2))
	assert.Nil(t, appender.AppendVarChar("Hello, World"))

	// out of cols here
	assert.Equal(t, ErrDuckDBError, appender.AppendInt32(42))
	assert.Nil(t, appender.EndRow())
	assert.Nil(t, appender.Flush())

	assert.Nil(t, appender.BeginRow())
	assert.Nil(t, appender.AppendInt32(42))
	assert.Nil(t, appender.AppendDouble(4.2))
	// not enough cols here
	assert.Equal(t, ErrDuckDBError, appender.EndRow())
	assert.NotEqual(t, "", appender.Error())

	assert.Nil(t, appender.AppendVarChar("Hello, World"))
	// out of cols here
	assert.Equal(t, ErrDuckDBError, appender.AppendInt32(42))
	assert.NotEqual(t, "", appender.Error())
	assert.Nil(t, appender.EndRow())
	// we can flush again why not
	assert.Nil(t, appender.Flush())
	assert.Nil(t, appender.Close())
	var result1 CAPIResult
	assert.Nil(t, tester.Query("SELECT * FROM test", &result1))
	defer result1.Destroy()
	assert.Equal(t, int32(42), result1.FetchValueInt32(0, 0))
	assert.Equal(t, Double(4.2), result1.FetchValueDouble(1, 0))
	assert.Equal(t, "Hello, World", result1.FetchValueVarChar(2, 0))

	assert.Nil(t, appender.Destroy())
	// this has been destroyed
	assert.Equal(t, ErrDuckDBError, appender.Close())
	assert.Equal(t, "", appender.Error())
	assert.Equal(t, ErrDuckDBError, appender.Flush())
	assert.Equal(t, ErrDuckDBError, appender.EndRow())
	assert.Equal(t, ErrDuckDBError, appender.AppendInt32(42))
	assert.Equal(t, ErrDuckDBError, appender.Destroy())

	// many types
	qStr := `CREATE TABLE many_types(bool boolean, t TINYINT, s SMALLINT, b BIGINT, ut UTINYINT,
	                             us USMALLINT, ui UINTEGER, ub UBIGINT, uf REAL, ud DOUBLE, txt VARCHAR, blb BLOB, dt
	                             DATE, tm TIME, ts TIMESTAMP, ival INTERVAL, h HUGEINT)`
	assert.Nil(t, tester.NoResultQuery(qStr))

	appender, err = tester.conn.AppenderCreate("", "many_types")
	assert.Nil(t, err)
	assert.Nil(t, appender.BeginRow())
	assert.Nil(t, appender.AppendBool(true))
	assert.Nil(t, appender.AppendInt8(1))
	assert.Nil(t, appender.AppendInt16(1))
	assert.Nil(t, appender.AppendInt64(1))
	assert.Nil(t, appender.AppendUInt8(1))
	assert.Nil(t, appender.AppendUInt16(1))
	assert.Nil(t, appender.AppendUInt32(1))
	assert.Nil(t, appender.AppendUInt64(1))
	assert.Nil(t, appender.AppendFloat(0.5))
	assert.Nil(t, appender.AppendDouble(0.5))
	assert.Nil(t, appender.AppendVarCharLength("hello world", 5))

	ds := InitDateStruct(1992, 9, 3)
	blobData := []byte("hello world this is my long string")
	assert.Nil(t, appender.AppendBlob(blobData))
	assert.Nil(t, appender.AppendDate(ToDate(ds)))
	ts := InitTimeStruct(12, 22, 33, 1234)
	assert.Nil(t, appender.AppendTime(ToTime(ts)))
	tss := InitTimestampStruct(ds, ts)
	assert.Nil(t, appender.AppendTimestamp(ToTimestamp(tss)))
	interval := InitInterval(3, 0, 0)
	assert.Nil(t, appender.AppendInterval(interval))
	assert.Nil(t, appender.AppendHugeInt(DoubleToHugeInt(27)))
	assert.Nil(t, appender.EndRow())
	assert.Nil(t, appender.BeginRow())
	for i := 0; i < 17; i++ {
		assert.Nil(t, appender.AppendNull())
	}
	assert.Nil(t, appender.EndRow())
	assert.Nil(t, appender.Flush())
	assert.Nil(t, appender.Close())
	assert.Nil(t, appender.Destroy())
	var result2 CAPIResult
	assert.Nil(t, tester.Query("SELECT * FROM many_types", &result2))
	defer result2.Destroy()

	assert.Equal(t, true, result2.FetchValueBoolean(0, 0))
	assert.Equal(t, int8(1), result2.FetchValueInt8(1, 0))
	assert.Equal(t, int16(1), result2.FetchValueInt16(2, 0))
	assert.Equal(t, int64(1), result2.FetchValueInt64(3, 0))
	assert.Equal(t, uint8(1), result2.FetchValueUInt8(4, 0))
	assert.Equal(t, uint16(1), result2.FetchValueUInt16(5, 0))
	assert.Equal(t, uint32(1), result2.FetchValueUInt32(6, 0))
	assert.Equal(t, uint64(1), result2.FetchValueUInt64(7, 0))
	assert.Equal(t, Float(0.5), result2.FetchValueFloat(8, 0))
	assert.Equal(t, Double(0.5), result2.FetchValueDouble(9, 0))
	assert.Equal(t, "hello", result2.FetchValueVarChar(10, 0))

	blob := result2.FetchValueBlob(11, 0)
	defer blob.Free()
	assert.Equal(t, uint64(34), blob.Size())
	assert.Equal(t, 0, bytes.Compare(blob.UnsafeDataToSlice(), blobData))
	assert.Equal(t, uint32(0), result2.FetchValueUInt32(11, 0))

	_dateStruct := result2.FetchValueDateStruct(12, 0)
	assert.Equal(t, int32(1992), _dateStruct.Year())
	assert.Equal(t, int8(9), _dateStruct.Month())
	assert.Equal(t, int8(3), _dateStruct.Day())

	_timeStruct := result2.FetchValueTimeStruct(13, 0)
	assert.Equal(t, int8(12), _timeStruct.Hour())
	assert.Equal(t, int8(22), _timeStruct.Min())
	assert.Equal(t, int8(33), _timeStruct.Sec())
	assert.Equal(t, int32(1234), _timeStruct.Micros())

	_timestampSture := result2.FetchValueTimestampStruct(14, 0)
	_dateStruct = _timestampSture.Date()
	_timeStruct = _timestampSture.Time()
	assert.Equal(t, int32(1992), _dateStruct.Year())
	assert.Equal(t, int8(9), _dateStruct.Month())
	assert.Equal(t, int8(3), _dateStruct.Day())
	assert.Equal(t, int8(12), _timeStruct.Hour())
	assert.Equal(t, int8(22), _timeStruct.Min())
	assert.Equal(t, int8(33), _timeStruct.Sec())
	assert.Equal(t, int32(1234), _timeStruct.Micros())

	interval = result2.FetchValueInterval(15, 0)
	assert.Equal(t, int32(3), interval.Months())
	assert.Equal(t, int32(0), interval.Days())
	assert.Equal(t, int64(0), interval.Micros())

	assert.Equal(t, Double(27), HugeIntToDouble(result2.FetchValueHugeInt(16, 0)))
	for i := uint64(0); i < 17; i++ {
		isNull, err := result2.IsNull(i, 1)
		assert.Nil(t, err)
		assert.Equal(t, true, isNull)
	}

	assert.Equal(t, false, result2.FetchValueBoolean(0, 1))
	assert.Equal(t, int8(0), result2.FetchValueInt8(1, 1))
	assert.Equal(t, int16(0), result2.FetchValueInt16(2, 1))
	assert.Equal(t, int64(0), result2.FetchValueInt64(3, 1))
	assert.Equal(t, uint8(0), result2.FetchValueUInt8(4, 1))
	assert.Equal(t, uint16(0), result2.FetchValueUInt16(5, 1))
	assert.Equal(t, uint32(0), result2.FetchValueUInt32(6, 1))
	assert.Equal(t, uint64(0), result2.FetchValueUInt64(7, 1))
	assert.Equal(t, Float(0), result2.FetchValueFloat(8, 1))
	assert.Equal(t, Double(0), result2.FetchValueDouble(9, 1))
	assert.Equal(t, "", result2.FetchValueVarChar(10, 1))

	blob2 := result2.FetchValueBlob(11, 1)
	defer blob2.Free()
	assert.Equal(t, uint64(0), blob2.Size())

	_dateStruct = result2.FetchValueDateStruct(12, 1)
	assert.Equal(t, int32(1970), _dateStruct.Year())

	_timeStruct = result2.FetchValueTimeStruct(13, 1)
	assert.Equal(t, int8(0), _timeStruct.Hour())

	_timestampSture = result2.FetchValueTimestampStruct(14, 1)
	_dateStruct = _timestampSture.Date()
	_timeStruct = _timestampSture.Time()
	assert.Equal(t, int32(1970), _dateStruct.Year())
	assert.Equal(t, int8(0), _timeStruct.Hour())

	interval = result2.FetchValueInterval(15, 1)
	assert.Equal(t, int32(0), interval.Months())

	assert.Equal(t, Double(0), HugeIntToDouble(result2.FetchValueHugeInt(16, 1)))

	// double out of range for HugeInt
	hugeInt := DoubleToHugeInt(1e300)
	assert.Equal(t, uint64(0), hugeInt.Lower())
	assert.Equal(t, int64(0), hugeInt.Upper())

}
