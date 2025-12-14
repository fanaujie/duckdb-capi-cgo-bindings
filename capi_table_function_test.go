package duckdbcapi

import (
	"runtime/cgo"
	"testing"

	"github.com/stretchr/testify/assert"
)

/*
reference from https://github.com/duckdb/duckdb/blob/master/test/api/capi/capi_table_functions.cpp
*/

type myBindDataStruct struct {
	size int64
}

type myInitDataStruct struct {
	pos int64
}

type tableFunctionCallback struct {
	t *testing.T
}

func (c *tableFunctionCallback) Bind(info *BindInfo) {
	assert.Equal(c.t, uint64(1), info.GetParameterCount())
	lt := CreateLogicalType(DuckDBTypeBigInt)
	info.AddResultColumn("forty_two", lt)
	lt.Destroy()
	value := info.GetParameter(0)
	bindData := value.GetInt64()
	info.SetBindData(cgo.NewHandle(&myBindDataStruct{bindData}))
	value.Destroy()
}

func (c *tableFunctionCallback) Init(info *InitInfo) {
	info.SetInitData(cgo.NewHandle(&myInitDataStruct{0}))
}

func (c *tableFunctionCallback) Function(info *FunctionInfo, dataChunk *DataChunk) {
	myBind := info.GetBindData().Value().(*myBindDataStruct)
	myInit := info.GetInitData().Value().(*myInitDataStruct)
	vec, err := dataChunk.GetVector(0)
	assert.Nil(c.t, err)
	pData, err := vec.GetData()
	assert.Nil(c.t, err)
	dataSlice := UnsafeSimpleDataToSlice[int64](pData, uint64(myBind.size))
	i := uint64(0)
	for i = 0; i < VectorSize(); i++ {
		if myInit.pos == myBind.size {
			break
		}
		if myInit.pos%2 == 0 {
			dataSlice[i] = 42
		} else {
			dataSlice[i] = 84
		}
		myInit.pos++
	}
	dataChunk.SetSize(i)
}

func cAPIRegisterTableFunction(t *testing.T, conn *Connection, name string, callback TableFunctionCallback) {

	// create a table function
	function := CreateTableFunction()
	{
		_f := TableFunction{}
		_f.SetName(name)
	}
	function.SetName("")
	function.SetName(name)
	function.SetName(name)

	// add a string parameter
	lt := CreateLogicalType(DuckDBTypeBigInt)
	function.AddParameter(lt)
	lt.Destroy()
	// set up the function pointers
	function.SetCallback(callback)

	// register and cleanup
	assert.Nil(t, conn.RegisterTableFunction(function))
	function.Destroy()
	function.Destroy()
	{
		_t := TableFunction{}
		_t.Destroy()
	}
}

func TestTableFunctionInCAPI(t *testing.T) {
	var tester CAPITester

	// open the database in in-memory mode
	assert.Equal(t, true, tester.OpenDatabase(""))
	defer tester.CleanUp()
	cb := &tableFunctionCallback{
		t: t,
	}

	cAPIRegisterTableFunction(t, tester.conn, "my_function", cb)
	// now call it
	var result CAPIResult
	assert.Nil(t, tester.Query("SELECT * FROM my_function(1)", &result))
	assert.Equal(t, int64(42), result.FetchValueInt64(0, 0))
	result.Destroy()

	assert.Nil(t, tester.Query("SELECT * FROM my_function(3)", &result))
	assert.Equal(t, int64(42), result.FetchValueInt64(0, 0))
	assert.Equal(t, int64(84), result.FetchValueInt64(0, 1))
	assert.Equal(t, int64(42), result.FetchValueInt64(0, 2))
	result.Destroy()

	assert.Nil(t, tester.Query("SELECT forty_two, COUNT(*) FROM my_function(10000) GROUP BY 1 ORDER BY 1", &result))
	assert.Equal(t, int64(42), result.FetchValueInt64(0, 0))
	assert.Equal(t, int64(84), result.FetchValueInt64(0, 1))
	assert.Equal(t, int64(5000), result.FetchValueInt64(1, 0))
	assert.Equal(t, int64(5000), result.FetchValueInt64(1, 1))
	result.Destroy()

}

type myErrorBind struct {
	tf *tableFunctionCallback
}

func (c *myErrorBind) Bind(info *BindInfo) {
	{
		_i := BindInfo{}
		_i.SetError("")
	}
	info.SetError("My error message")
}

func (c *myErrorBind) Init(info *InitInfo) {
	c.tf.Init(info)
}

func (c *myErrorBind) Function(info *FunctionInfo, dataChunk *DataChunk) {
	c.tf.Function(info, dataChunk)
}

type myErrorInit struct {
	tf *tableFunctionCallback
}

func (c *myErrorInit) Bind(info *BindInfo) {
	c.tf.Bind(info)
}

func (c *myErrorInit) Init(info *InitInfo) {
	{
		_i := InitInfo{}
		_i.SetError("")
	}
	info.SetError("My error message")
}

func (c *myErrorInit) Function(info *FunctionInfo, dataChunk *DataChunk) {
	c.tf.Function(info, dataChunk)
}

type myErrorFunction struct {
	tf *tableFunctionCallback
}

func (c *myErrorFunction) Bind(info *BindInfo) {
	c.tf.Bind(info)
}

func (c *myErrorFunction) Init(info *InitInfo) {
	c.tf.Init(info)
}

func (c *myErrorFunction) Function(info *FunctionInfo, dataChunk *DataChunk) {
	{
		_i := FunctionInfo{}
		_i.SetError("")
	}
	info.SetError("My error message")
}

func TestTableFunctionErrorsInCAPI(t *testing.T) {
	var tester CAPITester

	// open the database in in-memory mode
	assert.Equal(t, true, tester.OpenDatabase(""))
	defer tester.CleanUp()

	cAPIRegisterTableFunction(t, tester.conn, "my_error_bind", &myErrorBind{&tableFunctionCallback{t}})
	cAPIRegisterTableFunction(t, tester.conn, "my_error_init", &myErrorInit{&tableFunctionCallback{t}})
	cAPIRegisterTableFunction(t, tester.conn, "my_error_function", &myErrorFunction{&tableFunctionCallback{t}})
	var result CAPIResult
	assert.Equal(t, ErrDuckDBError, tester.Query("SELECT * FROM my_error_bind(1)", &result))
	assert.Equal(t, ErrDuckDBError, tester.Query("SELECT * FROM my_error_init(1)", &result))
	assert.Equal(t, ErrDuckDBError, tester.Query("SELECT * FROM my_error_function(1)", &result))
}
