package duckdbcapi

/*
reference from https://github.com/duckdb/duckdb/blob/master/test/api/capi/test_capi_prepared.cpp
*/

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPreparedStatementsInCAPI(t *testing.T) {
	var tester CAPITester
	// open the database in in-memory mode
	assert.Equal(t, true, tester.OpenDatabase(""))
	defer tester.CleanUp()
	var stmt PreparedStatement
	assert.Nil(t, tester.conn.Prepare("SELECT CAST($1 AS BIGINT)", &stmt))

	t.Run("BindBoolean", func(t *testing.T) {
		var result Result
		assert.Nil(t, stmt.BindBoolean(1, true))
		assert.Equal(t, ErrDuckDBError, stmt.BindBoolean(2, true))
		assert.Nil(t, stmt.ExecutePrepared(&result))
		defer result.Destroy()
		assert.Equal(t, int64(1), result.ValueInt64(0, 0))
	})
	t.Run("BindInt8", func(t *testing.T) {
		var result Result
		assert.Nil(t, stmt.BindInt8(1, 8))
		assert.Nil(t, stmt.ExecutePrepared(&result))
		defer result.Destroy()
		assert.Equal(t, int64(8), result.ValueInt64(0, 0))
	})
	t.Run("BindInt16", func(t *testing.T) {
		var result Result
		assert.Nil(t, stmt.BindInt16(1, 16))
		assert.Nil(t, stmt.ExecutePrepared(&result))
		defer result.Destroy()
		assert.Equal(t, int64(16), result.ValueInt64(0, 0))
	})
	t.Run("BindInt32", func(t *testing.T) {
		var result Result
		assert.Nil(t, stmt.BindInt32(1, 32))
		assert.Nil(t, stmt.ExecutePrepared(&result))
		defer result.Destroy()
		assert.Equal(t, int64(32), result.ValueInt64(0, 0))
	})
	t.Run("BindInt64", func(t *testing.T) {
		var result Result
		assert.Nil(t, stmt.BindInt64(1, 64))
		assert.Nil(t, stmt.ExecutePrepared(&result))
		defer result.Destroy()
		assert.Equal(t, int64(64), result.ValueInt64(0, 0))
	})

	t.Run("BindHugeInt", func(t *testing.T) {
		var result Result
		assert.Nil(t, stmt.BindHugeInt(1, DoubleToHugeInt(64)))
		assert.Nil(t, stmt.ExecutePrepared(&result))
		defer result.Destroy()
		assert.Equal(t, Double(64), HugeIntToDouble(result.ValueHugeInt(0, 0)))
	})

	t.Run("BindUInt8", func(t *testing.T) {
		var result Result
		assert.Nil(t, stmt.BindUInt8(1, 8))
		assert.Nil(t, stmt.ExecutePrepared(&result))
		defer result.Destroy()
		assert.Equal(t, uint8(8), result.ValueUInt8(0, 0))
	})
	t.Run("BindUInt16", func(t *testing.T) {
		var result Result
		assert.Nil(t, stmt.BindUInt16(1, 16))
		assert.Nil(t, stmt.ExecutePrepared(&result))
		defer result.Destroy()
		assert.Equal(t, uint16(16), result.ValueUInt16(0, 0))
	})
	t.Run("BindUInt32", func(t *testing.T) {
		var result Result
		assert.Nil(t, stmt.BindUInt32(1, 32))
		assert.Nil(t, stmt.ExecutePrepared(&result))
		defer result.Destroy()
		assert.Equal(t, uint32(32), result.ValueUInt32(0, 0))
	})
	t.Run("BindUInt64", func(t *testing.T) {
		var result Result
		assert.Nil(t, stmt.BindUInt64(1, 64))
		assert.Nil(t, stmt.ExecutePrepared(&result))
		defer result.Destroy()
		assert.Equal(t, uint64(64), result.ValueUInt64(0, 0))
	})
	t.Run("BindFloat", func(t *testing.T) {
		var result Result
		assert.Nil(t, stmt.BindFloat(1, 42.0))
		assert.Nil(t, stmt.ExecutePrepared(&result))
		defer result.Destroy()
		assert.Equal(t, Float(42.0), result.ValueFloat(0, 0))
	})
	t.Run("BindDouble", func(t *testing.T) {
		var result Result
		assert.Nil(t, stmt.BindDouble(1, 43.0))
		assert.Nil(t, stmt.ExecutePrepared(&result))
		defer result.Destroy()
		assert.Equal(t, Double(43.0), result.ValueDouble(0, 0))
	})
	t.Run("BindVarChar", func(t *testing.T) {
		var result Result
		assert.Equal(t, ErrDuckDBError, stmt.BindVarChar(1, "\x80\x40\x41"))
		assert.Nil(t, stmt.BindVarChar(1, "44"))
		assert.Nil(t, stmt.ExecutePrepared(&result))
		defer result.Destroy()
		assert.Equal(t, int64(44), result.ValueInt64(0, 0))
	})

	t.Run("BindNull", func(t *testing.T) {
		var result Result
		assert.Nil(t, stmt.BindNull(1))
		assert.Nil(t, stmt.ExecutePrepared(&result))
		defer result.Destroy()
		assert.Equal(t, true, UnsafeSimpleDataToSlice[bool](result.NullMaskData(0), 1)[0])
	})
	stmt.Destroy()
	stmt.Destroy()

	assert.Nil(t, tester.conn.Prepare("SELECT CAST($1 AS VARCHAR)", &stmt))
	t.Run("BindVarCharLength", func(t *testing.T) {
		var result Result
		assert.Equal(t, ErrDuckDBError, stmt.BindVarCharLength(1, "\x80\x40\x41", 3))
		assert.Nil(t, stmt.BindVarCharLength(1, "hello world", 5))
		assert.Nil(t, stmt.ExecutePrepared(&result))
		defer result.Destroy()
		assert.Equal(t, "hello", result.ValueVarChar(0, 0))
		assert.Equal(t, int8(0), result.ValueInt8(0, 0))
	})
	t.Run("BindBlob", func(t *testing.T) {
		var result Result
		d := []byte{'h', 'e', 'l', 'l', 'o', 0, 'w', 'o', 'r', 'l', 'd'}
		assert.Nil(t, stmt.BindBlob(1, d))
		assert.Nil(t, stmt.ExecutePrepared(&result))
		defer result.Destroy()
		assert.Equal(t, "hello\\x00world", result.ValueVarChar(0, 0))
		assert.Equal(t, int8(0), result.ValueInt8(0, 0))
	})
	t.Run("BindDate", func(t *testing.T) {
		var result Result
		dateStruct := InitDateStruct(1992, 9, 3)
		assert.Nil(t, stmt.BindDate(1, ToDate(dateStruct)))
		assert.Nil(t, stmt.ExecutePrepared(&result))
		defer result.Destroy()
		assert.Equal(t, "1992-09-03", result.ValueVarChar(0, 0))
	})
	t.Run("BindTime", func(t *testing.T) {
		var result Result
		timeStruct := InitTimeStruct(12, 22, 33, 123400)
		assert.Nil(t, stmt.BindTime(1, ToTime(timeStruct)))
		assert.Nil(t, stmt.ExecutePrepared(&result))
		defer result.Destroy()
		assert.Equal(t, "12:22:33.1234", result.ValueVarChar(0, 0))
	})

	t.Run("BindTimestamp", func(t *testing.T) {
		var result Result
		timestampStruct := InitTimestampStruct(InitDateStruct(1992, 9, 3), InitTimeStruct(12, 22, 33, 123400))
		assert.Nil(t, stmt.BindTimestamp(1, ToTimestamp(timestampStruct)))
		assert.Nil(t, stmt.ExecutePrepared(&result))
		defer result.Destroy()
		assert.Equal(t, "1992-09-03 12:22:33.1234", result.ValueVarChar(0, 0))
	})
	t.Run("BindInterval", func(t *testing.T) {
		var result Result
		interval := InitInterval(3, 0, 0)
		assert.Nil(t, stmt.BindInterval(1, interval))
		assert.Nil(t, stmt.ExecutePrepared(&result))
		defer result.Destroy()
		assert.Equal(t, "3 months", result.ValueVarChar(0, 0))
	})
	stmt.Destroy()

	t.Run("SUM(i)*$1-$2", func(t *testing.T) {
		assert.Nil(t, tester.conn.Query("CREATE TABLE a (i INTEGER)", nil))
		assert.Equal(t, uint64(0), stmt.NParams())
		assert.Equal(t, DuckDBTypeInvalid, stmt.ParamType(0))

		assert.Nil(t, tester.conn.Prepare("INSERT INTO a VALUES (?)", &stmt))
		assert.Equal(t, uint64(1), stmt.NParams())
		assert.Equal(t, DuckDBTypeInvalid, stmt.ParamType(0))
		assert.Equal(t, DuckDBTypeInteger, stmt.ParamType(1))
		assert.Equal(t, DuckDBTypeInvalid, stmt.ParamType(2))

		for i := int32(0); i <= 1000; i++ {
			assert.Nil(t, stmt.BindInt32(1, i))
			assert.Nil(t, stmt.ExecutePrepared(nil))
		}
		stmt.Destroy()

		var result Result
		assert.Nil(t, tester.conn.Prepare("SELECT SUM(i)*$1-$2 FROM a", &stmt))
		assert.Nil(t, stmt.BindInt32(1, 2))
		assert.Nil(t, stmt.BindInt32(2, 1000))
		assert.Nil(t, stmt.ExecutePrepared(&result))
		defer result.Destroy()
		assert.Equal(t, int32(1000000), result.ValueInt32(0, 0))
		stmt.Destroy()

	})
	t.Run("not-so-happy path", func(t *testing.T) {
		assert.Equal(t, ErrDuckDBError, tester.conn.Prepare("SELECT XXXXX", &stmt))
		stmt.Destroy()
		assert.Nil(t, tester.conn.Prepare("SELECT CAST($1 AS INTEGER)", &stmt))
		var result Result
		assert.Equal(t, ErrDuckDBError, stmt.ExecutePrepared(&result))
		result.Destroy()
		stmt.Destroy()
	})

}
