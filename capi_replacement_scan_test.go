package duckdbcapi

/*
reference from https://github.com/duckdb/duckdb/blob/master/test/api/capi/test_capi_replacement_scan.cpp
*/

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

type MyBaseNumber struct {
	number int
}

func (n *MyBaseNumber) ReplacementScanCallback(info *ReplacementScanInfo, tableName string) {
	info.SetFunctionName("range")
	number, err := strconv.Atoi(tableName)
	if err != nil {
		// not a number!
		return
	}
	val := CreateInt64(int64(number + n.number))
	info.AddParameter(val)
	val.Destroy()
}

func (n *MyBaseNumber) DeleteCallback() {

}

func TestReplacementScanInCAPI(t *testing.T) {
	var tester CAPITester

	// open the database in in-memory mode
	assert.Equal(t, true, tester.OpenDatabase(""))
	defer tester.CleanUp()
	num := &MyBaseNumber{
		number: 3,
	}
	tester.db.AddReplacementScan(num)
	var result CAPIResult
	assert.Nil(t, tester.Query("SELECT * FROM \"2\"", &result))
	tester.db.AddReplacementScan(num)
	assert.Equal(t, uint64(5), result.RowCount())
	assert.Equal(t, int64(0), result.FetchValueInt64(0, 0))
	assert.Equal(t, int64(1), result.FetchValueInt64(0, 1))
	assert.Equal(t, int64(2), result.FetchValueInt64(0, 2))
	assert.Equal(t, int64(3), result.FetchValueInt64(0, 3))
	assert.Equal(t, int64(4), result.FetchValueInt64(0, 4))
	result.Destroy()

	num.number = 1
	assert.Nil(t, tester.Query("SELECT * FROM \"2\"", &result))
	assert.Equal(t, uint64(3), result.RowCount())
	assert.Equal(t, int64(0), result.FetchValueInt64(0, 0))
	assert.Equal(t, int64(1), result.FetchValueInt64(0, 1))
	assert.Equal(t, int64(2), result.FetchValueInt64(0, 2))
	result.Destroy()

	assert.Equal(t, ErrDuckDBError, tester.Query("SELECT * FROM nonexistant", &result))
}
