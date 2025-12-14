package duckdbcapi

/*
reference from https://github.com/duckdb/duckdb/blob/master/test/api/capi/test_capi_data_chunk.cpp
*/

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLogicalTypesCAPI(t *testing.T) {
	lt := CreateLogicalType(DuckDBTypeBigInt)
	assert.Equal(t, DuckDBTypeBigInt, lt.GetTypeId())
	lt.Destroy()
	lt.Destroy()
}

func TestDataChunkCAPI(t *testing.T) {
	var tester CAPITester

	assert.Equal(t, true, tester.OpenDatabase(""))
	defer tester.CleanUp()
	assert.Equal(t, uint64(1024), VectorSize())
	assert.Nil(t, tester.NoResultQuery("CREATE TABLE test(i BIGINT, j SMALLINT)"))

	types := []*LogicalType{CreateLogicalType(DuckDBTypeBigInt), CreateLogicalType(DuckDBTypeSmallInt)}

	dataChunk, err := CreateDataChunk(types, 2)
	assert.Nil(t, err)
	assert.Equal(t, uint64(2), dataChunk.GetColumnCount())

	vec, err := dataChunk.GetVector(0)
	assert.Nil(t, err)

	firstType := vec.GetColumnType()
	defer firstType.Destroy()
	assert.Equal(t, DuckDBTypeBigInt, firstType.GetTypeId())

	vec, err = dataChunk.GetVector(1)
	assert.Nil(t, err)
	secondType := vec.GetColumnType()
	defer secondType.Destroy()
	assert.Equal(t, DuckDBTypeSmallInt, secondType.GetTypeId())

	_, err = dataChunk.GetVector(999)
	assert.Equal(t, ErrVectorNil, err)
	{
		_dataChunk := DataChunk{}
		_, err = _dataChunk.GetVector(0)
		assert.Equal(t, ErrVectorNil, err)
		_vector := Vector{}
		assert.Equal(t, uintptr(0), uintptr(_vector.GetColumnType().c))
		assert.Equal(t, uint64(0), _dataChunk.GetSize())
	}
	assert.Equal(t, uint64(0), dataChunk.GetSize())

	// use the appender to insert a value using the data chunk API
	appender, err := tester.conn.AppenderCreate("", "test")
	assert.Nil(t, err)

	// append standard primitive values
	vec, err = dataChunk.GetVector(0)
	assert.Nil(t, err)
	d0, err := vec.GetData()
	assert.Nil(t, err)
	*(*uint64)(d0) = 42

	vec, err = dataChunk.GetVector(1)
	assert.Nil(t, err)
	d1, err := vec.GetData()
	assert.Nil(t, err)
	*(*uint64)(d1) = 84
	{
		var _v Vector
		_, err = _v.GetData()
		assert.Equal(t, ErrVectorGetDataNil, err)
	}
	dataChunk.SetSize(1)
	assert.Equal(t, uint64(1), dataChunk.GetSize())

	assert.Nil(t, appender.AppendDataChunk(dataChunk))
	assert.Equal(t, ErrDuckDBError, appender.AppendDataChunk(&DataChunk{}))
	{
		var _app Appender
		assert.Equal(t, ErrDuckDBError, _app.AppendDataChunk(dataChunk))
	}
	// append nulls
	dataChunk.Reset()
	assert.Equal(t, uint64(0), dataChunk.GetSize())

	vec, err = dataChunk.GetVector(0)
	assert.Nil(t, err)
	vec.EnsureValidityWritable()
	vec, err = dataChunk.GetVector(1)
	assert.Nil(t, err)
	vec.EnsureValidityWritable()

	vec, err = dataChunk.GetVector(0)
	assert.Nil(t, err)
	validity, err := vec.GetValidity()
	assert.Nil(t, err)
	assert.Equal(t, true, validity.RowIsValid(0))
	validity.SetRowValidity(0, false)
	assert.Equal(t, false, validity.RowIsValid(0))

	vec, err = dataChunk.GetVector(1)
	assert.Nil(t, err)
	validity, err = vec.GetValidity()
	assert.Nil(t, err)
	assert.Equal(t, true, validity.RowIsValid(0))
	validity.SetRowValidity(0, false)
	assert.Equal(t, false, validity.RowIsValid(0))

	dataChunk.SetSize(1)
	assert.Equal(t, uint64(1), dataChunk.GetSize())
	assert.Nil(t, appender.AppendDataChunk(dataChunk))

	{
		var _v = Vector{}
		_, err = _v.GetValidity()
		assert.Equal(t, ErrVectorGetValidityNil, err)
	}
	appender.Destroy()
	var result CAPIResult
	assert.Nil(t, tester.Query("SELECT * FROM test", &result))
	defer result.Destroy()

	assert.Equal(t, int64(42), result.FetchValueInt64(0, 0))
	assert.Equal(t, int64(84), result.FetchValueInt64(1, 0))
	isNull, err := result.IsNull(0, 1)
	assert.Nil(t, err)
	assert.Equal(t, true, isNull)
	isNull, err = result.IsNull(1, 1)
	assert.Nil(t, err)
	assert.Equal(t, true, isNull)

	dataChunk.Reset()
	{
		var _d DataChunk
		_d.Reset()
	}
	assert.Equal(t, uint64(0), dataChunk.GetSize())
	dataChunk.Destroy()
	dataChunk.Destroy()
	{
		var _d DataChunk
		_d.Destroy()
	}
	for _, lt := range types {
		lt.Destroy()
	}
}
func TestDataChunkResultFetchInCAPI(t *testing.T) {
	var tester CAPITester

	assert.Equal(t, true, VectorSize() > 64)

	assert.Equal(t, true, tester.OpenDatabase(""))
	defer tester.CleanUp()
	var result CAPIResult
	assert.Nil(t, tester.Query("SELECT CASE WHEN i=1 THEN NULL ELSE i::INTEGER END i FROM range(3) tbl(i)", &result))
	defer result.Destroy()
	assert.Equal(t, uint64(1), result.ColumnCount())
	assert.Equal(t, uint64(3), result.RowCount())
	assert.Equal(t, "", result.ErrorMessage())

	assert.Equal(t, uint64(1), result.ChunkCount())
	chunk, err := result.FetchChunk(0)
	assert.Nil(t, err)

	assert.Equal(t, uint64(1), chunk.ColumnCount())
	assert.Equal(t, uint64(3), chunk.Size())

	pData, err := chunk.GetData(0)
	assert.Nil(t, err)
	validity, err := chunk.GetValidity(0)
	assert.Nil(t, err)
	data := UnsafeSimpleDataToSlice[int32](pData, chunk.Size())
	assert.Equal(t, int32(0), data[0])
	assert.Equal(t, int32(2), data[2])

	assert.Equal(t, true, validity.RowIsValid(0))
	assert.Equal(t, false, validity.RowIsValid(1))
	assert.Equal(t, true, validity.RowIsValid(2))

	// after fetching a chunk, we cannot use the old API anymore
	_, err = result.ColumnData(0)
	assert.Equal(t, ErrColumnDataNil, err)

	_, err = result.ColumnData(0)
	assert.NotNil(t, err)
	assert.Equal(t, int32(0), result.FetchValueInt32(0, 1))

	// result set is exhausted!
	chunk, err = result.FetchChunk(1)
	assert.Equal(t, ErrDataChunkNil, err)
}
