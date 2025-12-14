package duckdbcapi

/*
reference from https://github.com/duckdb/duckdb/blob/master/test/include/capi_tester.hpp
*/

import (
	"errors"
	"unsafe"
)

type CAPITester struct {
	db   *DataBase
	conn *Connection
}

func (c *CAPITester) CleanUp() {
	if c.conn != nil {
		c.conn.Disconnect()
		c.conn = nil
	}
	if c.db != nil {
		c.db.Close()
		c.db = nil
	}
}

func (c *CAPITester) OpenDatabase(path string) bool {
	c.CleanUp()
	db, err := Open(path)
	if err != nil {
		return false
	}
	c.db = db
	c.conn, err = c.db.Connection()
	if err != nil {
		c.CleanUp()
		return false
	}
	return true
}

func (c *CAPITester) Query(query string, apiResult *CAPIResult) error {
	if err := c.conn.Query(query, &apiResult.result); err != nil {
		return err
	}
	return nil
}

func (c *CAPITester) NoResultQuery(query string) error {
	return c.conn.Query(query, nil)
}

type CAPIDataChunk struct {
	chunk *DataChunk
}

func (c *CAPIDataChunk) ColumnCount() uint64 {
	return c.chunk.GetColumnCount()
}

func (c *CAPIDataChunk) Size() uint64 {
	return c.chunk.GetSize()
}

func (c *CAPIDataChunk) GetData(col uint64) (unsafe.Pointer, error) {
	vec, err := c.chunk.GetVector(col)
	if err != nil {
		return nil, err
	}
	pData, err := vec.GetData()
	if err != nil {
		return nil, err
	}
	return pData, nil
}

func (c *CAPIDataChunk) GetValidity(col uint64) (*Validity, error) {
	vec, err := c.chunk.GetVector(col)
	if err != nil {
		return nil, err
	}
	return vec.GetValidity()
}

func (c *CAPIDataChunk) GetChunk() *DataChunk {
	return c.chunk
}

type CAPIResult struct {
	result Result
}

func (c *CAPIResult) Destroy() {
	c.result.Destroy()
	c.result = Result{}
}

func (c *CAPIResult) ColumnType(col uint64) Type {
	return c.result.ColumnType(col)
}
func (c *CAPIResult) ColumnName(col uint64) (string, error) {
	return c.result.ColumnName(col)
}

func (c *CAPIResult) ChunkCount() uint64 {
	return c.result.ChunkCount()
}

func (c *CAPIResult) FetchChunk(col uint64) (*CAPIDataChunk, error) {
	chunk, err := c.result.Chunk(col)
	if err != nil {
		return nil, err
	}
	return &CAPIDataChunk{chunk}, nil
}

func (c *CAPIResult) IsNull(col, row uint64) (bool, error) {
	pData := c.result.NullMaskData(col)
	if pData == nil {
		return false, ErrNullMaskDataNil
	}
	v := UnsafeSimpleDataToSlice[bool](pData, c.result.RowCount())[row]
	if v != c.result.ValueIsNull(col, row) {
		return false, errors.New("failed to check null mask")
	}
	return v, nil
}

func (c *CAPIResult) ErrorMessage() string {
	return c.result.ResultError()
}

func (c *CAPIResult) ColumnData(col uint64) (unsafe.Pointer, error) {
	return c.result.ColumnData(col)
}

func (c *CAPIResult) ColumnCount() uint64 {
	return c.result.ColumnCount()
}

func (c *CAPIResult) RowCount() uint64 {
	return c.result.RowCount()
}

func (c *CAPIResult) RowsChanged() uint64 {
	return c.result.RowsChanged()
}

func (c *CAPIResult) FetchValueBoolean(col, row uint64) bool {
	return c.result.ValueBoolean(col, row)
}

func (c *CAPIResult) FetchValueInt8(col, row uint64) int8 {
	return c.result.ValueInt8(col, row)
}

func (c *CAPIResult) FetchValueInt16(col, row uint64) int16 {
	return c.result.ValueInt16(col, row)
}

func (c *CAPIResult) FetchValueInt32(col, row uint64) int32 {
	return c.result.ValueInt32(col, row)
}

func (c *CAPIResult) FetchValueInt64(col, row uint64) int64 {
	return c.result.ValueInt64(col, row)
}

func (c *CAPIResult) FetchValueUInt8(col, row uint64) uint8 {
	return c.result.ValueUInt8(col, row)
}

func (c *CAPIResult) FetchValueUInt16(col, row uint64) uint16 {
	return c.result.ValueUInt16(col, row)
}

func (c *CAPIResult) FetchValueUInt32(col, row uint64) uint32 {
	return c.result.ValueUInt32(col, row)
}
func (c *CAPIResult) FetchValueUInt64(col, row uint64) uint64 {
	return c.result.ValueUInt64(col, row)
}

func (c *CAPIResult) FetchValueFloat(col, row uint64) Float {
	return c.result.ValueFloat(col, row)
}

func (c *CAPIResult) FetchValueDouble(col, row uint64) Double {
	return c.result.ValueDouble(col, row)
}

func (c *CAPIResult) FetchValueVarChar(col, row uint64) string {
	return c.result.ValueVarChar(col, row)
}

func (c *CAPIResult) FetchValueHugeInt(col, row uint64) HugeInt {
	return c.result.ValueHugeInt(col, row)
}
func (c *CAPIResult) FetchValueDate(col, row uint64) Date {
	return c.result.ValueDate(col, row)
}
func (c *CAPIResult) FetchValueTime(col, row uint64) Time {
	return c.result.ValueTime(col, row)
}

func (c *CAPIResult) FetchValueBlob(col, row uint64) Blob {
	return c.result.ValueBlob(col, row)
}

func (c *CAPIResult) FetchValueDecimal(col, row uint64) Decimal {
	return c.result.ValueDecimal(col, row)
}

func (c *CAPIResult) FetchValueDateStruct(col, row uint64) DateStruct {
	return FromDate(c.result.ValueDate(col, row))
}

func (c *CAPIResult) FetchValueTimeStruct(col, row uint64) TimeStruct {
	return FromTime(c.result.ValueTime(col, row))
}
func (c *CAPIResult) FetchValueTimestampStruct(col, row uint64) TimestampStruct {
	return FromTimestamp(c.result.ValueTimestamp(col, row))
}

func (c *CAPIResult) FetchValueInterval(col, row uint64) Interval {
	return c.result.ValueInterval(col, row)
}
