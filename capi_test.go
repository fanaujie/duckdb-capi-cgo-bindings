package duckdbcapi

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

/*
reference from https://github.com/duckdb/duckdb/blob/master/test/api/capi/test_capi.cpp
*/

func TestBasicOfCAPI(t *testing.T) {
	var tester CAPITester

	// open the database in in-memory mode
	assert.Equal(t, true, tester.OpenDatabase(":memory:"))
	defer tester.CleanUp()

	// select scalar value
	t.Run("query 1", func(t *testing.T) {
		var result CAPIResult
		assert.Nil(t, tester.Query("SELECT CAST(42 AS BIGINT)", &result))
		defer result.Destroy()
		assert.Equal(t, DuckDBTypeBigInt, result.ColumnType(0))
		pData, err := result.ColumnData(0)
		assert.Nil(t, err)
		data := UnsafeSimpleDataToSlice[int64](pData, 1)
		assert.Equal(t, int64(42), data[0])
		assert.Equal(t, uint64(1), result.ColumnCount())
		assert.Equal(t, uint64(1), result.RowCount())
		assert.Equal(t, int64(42), result.FetchValueInt64(0, 0))
		isNil, err := result.IsNull(0, 0)
		assert.Nil(t, err)
		assert.Equal(t, false, isNil)
		// out of range fetch
		assert.Equal(t, int64(0), result.FetchValueInt64(1, 0))
		assert.Equal(t, int64(0), result.FetchValueInt64(0, 1))
		// cannot fetch data chunk after using the value API
		_, err = result.FetchChunk(0)
		assert.Error(t, err)
	})
	// select scalar NULL
	t.Run("query 2", func(t *testing.T) {
		var result CAPIResult
		assert.Nil(t, tester.Query("SELECT NULL", &result))
		defer result.Destroy()
		assert.Equal(t, uint64(1), result.ColumnCount())
		assert.Equal(t, uint64(1), result.RowCount())
		assert.Equal(t, int64(0), result.FetchValueInt64(0, 0))
		isNil, err := result.IsNull(0, 0)
		assert.Nil(t, err)
		assert.Equal(t, true, isNil)
	})
	// select scalar string
	t.Run("query 3", func(t *testing.T) {
		var result CAPIResult
		assert.Nil(t, tester.Query("SELECT 'hello'", &result))
		defer result.Destroy()
		assert.Equal(t, uint64(1), result.ColumnCount())
		assert.Equal(t, uint64(1), result.RowCount())
		assert.Equal(t, "hello", result.FetchValueVarChar(0, 0))
		isNil, err := result.IsNull(0, 0)
		assert.Nil(t, err)
		assert.Equal(t, false, isNil)
	})

	t.Run("query 4", func(t *testing.T) {
		var result CAPIResult
		assert.Nil(t, tester.Query("SELECT 1=1", &result))
		defer result.Destroy()
		assert.Equal(t, uint64(1), result.ColumnCount())
		assert.Equal(t, uint64(1), result.RowCount())
		assert.Equal(t, true, result.FetchValueBoolean(0, 0))
		isNil, err := result.IsNull(0, 0)
		assert.Nil(t, err)
		assert.Equal(t, false, isNil)
	})

	t.Run("query 5", func(t *testing.T) {
		var result CAPIResult
		assert.Nil(t, tester.Query("SELECT 1=0", &result))
		defer result.Destroy()
		assert.Equal(t, uint64(1), result.ColumnCount())
		assert.Equal(t, uint64(1), result.RowCount())
		assert.Equal(t, false, result.FetchValueBoolean(0, 0))
		isNil, err := result.IsNull(0, 0)
		assert.Nil(t, err)
		assert.Equal(t, false, isNil)
	})

	t.Run("query 6", func(t *testing.T) {
		var result CAPIResult
		assert.Nil(t, tester.Query("SELECT i FROM (values (true), (false)) tbl(i) group by i order by i", &result))
		defer result.Destroy()
		assert.Equal(t, uint64(1), result.ColumnCount())
		assert.Equal(t, uint64(2), result.RowCount())
		assert.Equal(t, false, result.FetchValueBoolean(0, 0))
		assert.Equal(t, true, result.FetchValueBoolean(0, 1))
		isNil, err := result.IsNull(0, 1)
		assert.Nil(t, err)
		assert.Equal(t, false, isNil)
	})

	t.Run("multiple insertions", func(t *testing.T) {
		assert.Nil(t, tester.NoResultQuery("CREATE TABLE test (a INTEGER, b INTEGER);"))
		assert.Nil(t, tester.NoResultQuery("INSERT INTO test VALUES (11, 22)"))
		assert.Nil(t, tester.NoResultQuery("INSERT INTO test VALUES (NULL, 21)"))
		var r1 CAPIResult
		assert.Nil(t, tester.Query("INSERT INTO test VALUES (13, 22)", &r1))
		defer r1.Destroy()
		assert.Equal(t, uint64(1), r1.RowsChanged())
		var r2 CAPIResult
		assert.Nil(t, tester.Query("SELECT a, b FROM test ORDER BY a", &r2))
		defer r2.Destroy()
		isNil, err := r2.IsNull(0, 0)
		assert.Nil(t, err)
		assert.Equal(t, true, isNil)
		assert.Equal(t, int32(11), r2.FetchValueInt32(0, 1))
		assert.Equal(t, int32(13), r2.FetchValueInt32(0, 2))
		assert.Equal(t, int32(21), r2.FetchValueInt32(1, 0))
		assert.Equal(t, int32(22), r2.FetchValueInt32(1, 1))
		assert.Equal(t, int32(22), r2.FetchValueInt32(1, 2))
		n, err := r2.ColumnName(0)
		assert.Nil(t, err)
		assert.Equal(t, "a", n)
		n, err = r2.ColumnName(1)
		assert.Nil(t, err)
		assert.Equal(t, "b", n)
		_, err = r2.ColumnName(2)
		assert.Error(t, err)
	})

}

func TestDifferentTypeOfCAPI(t *testing.T) {
	var tester CAPITester

	// open the database in in-memory mode
	assert.Equal(t, true, tester.OpenDatabase(":memory:"))
	defer tester.CleanUp()

	t.Run("integer columns", func(t *testing.T) {
		types := []string{"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
			"UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT"}
		for _, _type := range types {
			func() {
				assert.Nil(t, tester.NoResultQuery("BEGIN TRANSACTION"))
				assert.Nil(t, tester.NoResultQuery(fmt.Sprintf("CREATE TABLE integers(i %s)", _type)))
				assert.Nil(t, tester.NoResultQuery("INSERT INTO integers VALUES (1), (NULL)"))
				var result CAPIResult
				assert.Nil(t, tester.Query("SELECT * FROM integers ORDER BY i", &result))
				defer result.Destroy()
				null, err := result.IsNull(0, 0)
				assert.Nil(t, err)
				assert.Equal(t, true, null)
				assert.Equal(t, int8(0), result.FetchValueInt8(0, 0))
				assert.Equal(t, int16(0), result.FetchValueInt16(0, 0))
				assert.Equal(t, int32(0), result.FetchValueInt32(0, 0))
				assert.Equal(t, int64(0), result.FetchValueInt64(0, 0))
				assert.Equal(t, uint8(0), result.FetchValueUInt8(0, 0))
				assert.Equal(t, uint16(0), result.FetchValueUInt16(0, 0))
				assert.Equal(t, uint32(0), result.FetchValueUInt32(0, 0))
				assert.Equal(t, uint64(0), result.FetchValueUInt64(0, 0))
				assert.Equal(t, Double(0), HugeIntToDouble(result.FetchValueHugeInt(0, 0)))
				assert.Equal(t, "", result.FetchValueVarChar(0, 0))
				assert.Equal(t, Float(0), result.FetchValueFloat(0, 0))
				assert.Equal(t, Double(0), result.FetchValueDouble(0, 0))
				null, err = result.IsNull(0, 1)
				assert.Nil(t, err)
				assert.Equal(t, false, null)
				assert.Equal(t, int8(1), result.FetchValueInt8(0, 1))
				assert.Equal(t, int16(1), result.FetchValueInt16(0, 1))
				assert.Equal(t, int32(1), result.FetchValueInt32(0, 1))
				assert.Equal(t, int64(1), result.FetchValueInt64(0, 1))
				assert.Equal(t, uint8(1), result.FetchValueUInt8(0, 1))
				assert.Equal(t, uint16(1), result.FetchValueUInt16(0, 1))
				assert.Equal(t, uint32(1), result.FetchValueUInt32(0, 1))
				assert.Equal(t, uint64(1), result.FetchValueUInt64(0, 1))
				assert.Equal(t, Double(1), HugeIntToDouble(result.FetchValueHugeInt(0, 1)))
				assert.Equal(t, "1", result.FetchValueVarChar(0, 1))
				assert.Equal(t, Float(1), result.FetchValueFloat(0, 1))
				assert.Equal(t, Double(1), result.FetchValueDouble(0, 1))
				assert.Nil(t, tester.NoResultQuery("ROLLBACK"))
			}()
		}
	})
	t.Run("real/double columns", func(t *testing.T) {
		types := []string{"REAL", "DOUBLE"}
		for _, _type := range types {
			func() {
				assert.Nil(t, tester.NoResultQuery("BEGIN TRANSACTION"))
				assert.Nil(t, tester.NoResultQuery(fmt.Sprintf("CREATE TABLE doubles(i %s)", _type)))
				assert.Nil(t, tester.NoResultQuery("INSERT INTO doubles VALUES (1), (NULL)"))
				var result CAPIResult
				assert.Nil(t, tester.Query("SELECT * FROM doubles ORDER BY i", &result))
				defer result.Destroy()
				null, err := result.IsNull(0, 0)
				assert.Nil(t, err)
				assert.Equal(t, true, null)
				assert.Equal(t, int8(0), result.FetchValueInt8(0, 0))
				assert.Equal(t, int16(0), result.FetchValueInt16(0, 0))
				assert.Equal(t, int32(0), result.FetchValueInt32(0, 0))
				assert.Equal(t, int64(0), result.FetchValueInt64(0, 0))
				assert.Equal(t, "", result.FetchValueVarChar(0, 0))
				assert.Equal(t, Float(0), result.FetchValueFloat(0, 0))
				assert.Equal(t, Double(0), result.FetchValueDouble(0, 0))
				null, err = result.IsNull(0, 1)
				assert.Nil(t, err)
				assert.Equal(t, false, null)
				assert.Equal(t, int8(1), result.FetchValueInt8(0, 1))
				assert.Equal(t, int16(1), result.FetchValueInt16(0, 1))
				assert.Equal(t, int32(1), result.FetchValueInt32(0, 1))
				assert.Equal(t, int64(1), result.FetchValueInt64(0, 1))
				assert.Equal(t, Float(1), result.FetchValueFloat(0, 1))
				assert.Equal(t, Double(1), result.FetchValueDouble(0, 1))
				assert.Nil(t, tester.NoResultQuery("ROLLBACK"))
			}()
		}
	})
	t.Run("date columns", func(t *testing.T) {
		assert.Nil(t, tester.NoResultQuery("CREATE TABLE dates(d DATE)"))
		assert.Nil(t, tester.NoResultQuery("INSERT INTO dates VALUES ('1992-09-20'), (NULL), ('30000-09-20')"))
		var result CAPIResult
		assert.Nil(t, tester.Query("SELECT * FROM dates ORDER BY d", &result))
		defer result.Destroy()
		null, err := result.IsNull(0, 0)
		assert.Nil(t, err)
		assert.Equal(t, true, null)
		dateStruct := FromDate(result.FetchValueDate(0, 1))
		assert.Equal(t, int32(1992), dateStruct.Year())
		assert.Equal(t, int8(9), dateStruct.Month())
		assert.Equal(t, int8(20), dateStruct.Day())
		dateStruct = FromDate(result.FetchValueDate(0, 2))
		assert.Equal(t, int32(30000), dateStruct.Year())
		assert.Equal(t, int8(9), dateStruct.Month())
		assert.Equal(t, int8(20), dateStruct.Day())
	})
	t.Run("time columns", func(t *testing.T) {
		assert.Nil(t, tester.NoResultQuery("CREATE TABLE times(d TIME)"))
		assert.Nil(t, tester.NoResultQuery("INSERT INTO times VALUES ('12:00:30.1234'), (NULL), ('02:30:01')"))
		var result CAPIResult
		assert.Nil(t, tester.Query("SELECT * FROM times ORDER BY d", &result))
		defer result.Destroy()
		null, err := result.IsNull(0, 0)
		assert.Nil(t, err)
		assert.Equal(t, true, null)
		timeStruct := FromTime(result.FetchValueTime(0, 1))
		assert.Equal(t, int8(2), timeStruct.Hour())
		assert.Equal(t, int8(30), timeStruct.Min())
		assert.Equal(t, int8(1), timeStruct.Sec())
		assert.Equal(t, int32(0), timeStruct.Micros())
		timeStruct = FromTime(result.FetchValueTime(0, 2))
		assert.Equal(t, int8(12), timeStruct.Hour())
		assert.Equal(t, int8(0), timeStruct.Min())
		assert.Equal(t, int8(30), timeStruct.Sec())
		assert.Equal(t, int32(123400), timeStruct.Micros())
	})
	t.Run("blob columns", func(t *testing.T) {
		assert.Nil(t, tester.NoResultQuery("CREATE TABLE blobs(b BLOB)"))
		assert.Nil(t, tester.NoResultQuery("INSERT INTO blobs VALUES ('hello\\x12world'), ('\\x00'), (NULL)"))
		var result CAPIResult
		assert.Nil(t, tester.Query("SELECT * FROM blobs", &result))
		defer result.Destroy()
		null, err := result.IsNull(0, 0)
		assert.Nil(t, err)
		assert.Equal(t, false, null)

		blob1 := result.FetchValueBlob(0, 0)
		defer blob1.Free()
		assert.Equal(t, uint64(11), blob1.Size())
		cmpData := []byte("hello")
		cmpData = append(cmpData, 0x12)
		cmpData = append(cmpData, []byte("world")...)
		assert.Equal(t, true, bytes.Equal(cmpData, blob1.UnsafeDataToSlice()))
		assert.Equal(t, "\\x00", result.FetchValueVarChar(0, 1))
		null, err = result.IsNull(0, 2)
		assert.Nil(t, err)
		assert.Equal(t, true, null)
		blob2 := result.FetchValueBlob(0, 2)
		defer blob2.Free()
		assert.Equal(t, uint64(0), blob2.Size())
		assert.Nil(t, blob2.UnsafeDataToSlice())
	})
	t.Run("boolean columns", func(t *testing.T) {
		assert.Nil(t, tester.NoResultQuery("CREATE TABLE booleans(b BOOLEAN)"))
		assert.Nil(t, tester.NoResultQuery("INSERT INTO booleans VALUES (42 > 60), (42 > 20), (42 > NULL)"))
		var result CAPIResult
		assert.Nil(t, tester.Query("SELECT * FROM booleans ORDER BY b", &result))
		defer result.Destroy()
		null, err := result.IsNull(0, 0)
		assert.Nil(t, err)
		assert.Equal(t, true, null)
		assert.Equal(t, false, result.FetchValueBoolean(0, 0))
		assert.Equal(t, false, result.FetchValueBoolean(0, 1))
		assert.Equal(t, true, result.FetchValueBoolean(0, 2))
		assert.Equal(t, "true", result.FetchValueVarChar(0, 2))
	})
	t.Run("decimal columns", func(t *testing.T) {
		assert.Nil(t, tester.NoResultQuery("CREATE TABLE decimals(dec DECIMAL(18, 4) NULL)"))
		assert.Nil(t, tester.NoResultQuery("INSERT INTO decimals VALUES (NULL), (12.3)"))
		var result CAPIResult
		assert.Nil(t, tester.Query("SELECT * FROM decimals ORDER BY dec", &result))
		defer result.Destroy()
		null, err := result.IsNull(0, 0)
		assert.Nil(t, err)
		assert.Equal(t, true, null)
		assert.Equal(t, Double(12.3), DecimalToDouble(result.FetchValueDecimal(0, 1)))
		var result2 CAPIResult
		assert.Nil(t, tester.Query("SELECT 1.2::DECIMAL(4,1), 100.3::DECIMAL(9,1), 320938.4298::DECIMAL(18,4), 49082094824.904820482094::DECIMAL(30,12), NULL::DECIMAL", &result2))
		defer result2.Destroy()

		assert.Equal(t, Double(1.2), DecimalToDouble(result2.FetchValueDecimal(0, 0)))
		assert.Equal(t, Double(100.3), DecimalToDouble(result2.FetchValueDecimal(1, 0)))
		assert.Equal(t, Double(320938.4298), DecimalToDouble(result2.FetchValueDecimal(2, 0)))
		assert.Equal(t, Double(49082094824.904820482094), DecimalToDouble(result2.FetchValueDecimal(3, 0)))
		null, err = result2.IsNull(4, 0)
		assert.Nil(t, err)
		assert.Equal(t, true, null)
	})
}

func TestErrosInCAPI(t *testing.T) {
	var tester CAPITester

	//cannot open database in random directory
	assert.Equal(t, false, tester.OpenDatabase("/bla/this/directory/should/not/exist/hopefully/awerar333"))
	assert.Equal(t, true, tester.OpenDatabase(""))
	defer tester.CleanUp()

	// syntax error in query
	var result CAPIResult
	assert.Error(t, tester.Query("SELEC * FROM TABLE", &result))

	// bind error
	assert.Error(t, tester.Query("SELECT * FROM TABLE", &result))

	// fail prepare API calls
	conn := Connection{c: nil}
	stmt := PreparedStatement{}
	assert.Equal(t, ErrDuckDBError, conn.Prepare("SELECT 42", &stmt))
	assert.Equal(t, ErrDuckDBError, conn.Prepare("", &stmt))
	assert.Equal(t, ErrDuckDBError, tester.conn.Prepare("SELECT * from INVALID_TABLE", &stmt))
	defer stmt.Destroy()
	assert.NotNil(t, stmt.PrepareError())
	assert.NotNil(t, stmt.c)

	stmt2 := PreparedStatement{}
	defer stmt2.Destroy()
	assert.Error(t, stmt2.BindBoolean(0, true))
	res := Result{}
	assert.Error(t, stmt2.ExecutePrepared(&res))

	// default duckdb_value_date on invalid date
	assert.Nil(t, tester.Query("SELECT 1, true, 'a'", &result))
	ds := result.FetchValueDateStruct(0, 0)
	assert.Equal(t, int32(1970), ds.Year())
	assert.Equal(t, int8(1), ds.Month())
	assert.Equal(t, int8(1), ds.Day())

	ds = result.FetchValueDateStruct(1, 0)
	assert.Equal(t, int32(1970), ds.Year())
	assert.Equal(t, int8(1), ds.Month())
	assert.Equal(t, int8(1), ds.Day())

	ds = result.FetchValueDateStruct(2, 0)
	assert.Equal(t, int32(1970), ds.Year())
	assert.Equal(t, int8(1), ds.Month())
	assert.Equal(t, int8(1), ds.Day())

}

func TestCAPIConfig(t *testing.T) {

	// enumerate config options
	for i := uint64(0); i < ConfigCount(); i++ {
		name, description, err := GetConfigFlag(i)
		assert.Nil(t, err)
		assert.Equal(t, true, len(name) > 0)
		assert.Equal(t, true, len(description) > 0)
	}

	// test config creation
	cfg, err := CreateConfig()
	assert.Nil(t, err)
	defer cfg.Destroy()
	assert.Error(t, cfg.SetConfig("access_mode", "invalid_access_mode"))
	assert.Nil(t, cfg.SetConfig("access_mode", "read_only"))
	assert.Error(t, cfg.SetConfig("aaaa_invalidoption", "read_only"))

	dbDir, err := ioutil.TempDir("", "duckdb")
	assert.Nil(t, err)
	defer os.RemoveAll(dbDir)
	dbPath := path.Join(dbDir, "a")
	// open the database & connection
	// cannot open an in-memory database in read-only mode
	_, errStr, err := OpenExt(":memory:", cfg)
	assert.Equal(t, ErrDuckDBError, err)
	assert.Equal(t, true, len(errStr) > 0)

	// cannot open a database that does not exist
	_, errStr, err = OpenExt(dbPath, cfg)
	assert.Equal(t, ErrDuckDBError, err)
	assert.Equal(t, true, len(errStr) > 0)

	// we can create the database and add some tables
	func() {
		db, err := Open(dbPath)
		assert.Nil(t, err)
		defer db.Close()
		conn, err := db.Connection()
		assert.Nil(t, err)
		defer conn.Disconnect()

		assert.Nil(t, conn.Query("CREATE TABLE integers(i INTEGER)", nil))
		assert.Nil(t, conn.Query("INSERT INTO integers VALUES (42)", nil))
	}()
	// now we can connect
	db, errStr, err := OpenExt(dbPath, cfg)
	assert.Nil(t, err)
	assert.Equal(t, "", errStr)
	// we can destroy the config right after duckdb_open
	cfg.Destroy()
	// we can spam this
	cfg.Destroy()
	cfg.Destroy()

	conn, err := db.Connection()
	assert.Nil(t, err)
	defer conn.Disconnect()
	// we can query
	r1 := &Result{}
	defer r1.Destroy()
	assert.Nil(t, conn.Query("SELECT 42::INT", r1))
	assert.Equal(t, int32(42), r1.ValueInt32(0, 0))
	r2 := &Result{}
	defer r2.Destroy()
	assert.Nil(t, conn.Query("SELECT i::INT FROM integers", r2))
	assert.Equal(t, int32(42), r1.ValueInt32(0, 0))

	// api abuse
	_, _, err = GetConfigFlag(9999999)
	assert.Equal(t, ErrDuckDBError, err)

}
func TestIssue2058(t *testing.T) {
	/*
		Cleanup after execution of invalid SQL statement causes segmentation fault
	*/

	db, err := Open("")
	assert.Nil(t, err)
	conn, err := db.Connection()
	assert.Nil(t, err)
	defer db.Close()
	defer conn.Disconnect()

	resultCount := &Result{}
	assert.Nil(t, conn.Query("CREATE TABLE integers(i INTEGER, j INTEGER)", nil))
	assert.Nil(t, conn.Query("SELECT count(*) FROM integers;", resultCount))
	resultCount.Destroy()
	result := &Result{}
	assert.Equal(t, ErrDuckDBError, conn.Query("non valid SQL", result))
	result.Destroy() // segmentation failure happens here
}
