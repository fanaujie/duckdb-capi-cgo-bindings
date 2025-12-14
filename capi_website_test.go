package duckdbcapi

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

/*
reference from https://github.com/duckdb/duckdb/blob/master/test/api/capi/test_capi_website.cpp
*/

func TestCAPIExampleFromWebSite(t *testing.T) {
	t.Run("connect", func(t *testing.T) {

		db, err := Open("")
		assert.Nil(t, err)
		conn, err := db.Connection()
		assert.Nil(t, err)

		conn.Disconnect()
		db.Close()
	})

	t.Run("config", func(t *testing.T) {

		config, err := CreateConfig()
		assert.Nil(t, err)
		assert.Nil(t, config.SetConfig("access_mode", "READ_WRITE"))
		assert.Nil(t, config.SetConfig("threads", "8"))
		assert.Nil(t, config.SetConfig("max_memory", "8GB"))
		assert.Nil(t, config.SetConfig("default_order", "DESC"))

		db, _, err := OpenExt("", config)
		assert.Nil(t, err)
		config.Destroy()

		db.Close()
	})

	t.Run("query", func(t *testing.T) {
		db, err := Open("")
		assert.Nil(t, err)
		defer db.Close()
		conn, err := db.Connection()
		assert.Nil(t, err)
		defer conn.Disconnect()
		assert.Nil(t, conn.Query("CREATE TABLE integers(i INTEGER, j INTEGER);", nil))
		assert.Nil(t, conn.Query("INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);", nil))

		result := Result{}
		assert.Nil(t, conn.Query("SELECT * FROM integers", &result))
		defer result.Destroy()
		rowCount := result.RowCount()
		columnCount := result.ColumnCount()
		for row := uint64(0); row < rowCount; row++ {
			for column := uint64(0); column < columnCount; column++ {
				result.ValueVarChar(column, row)
			}
		}
		pCol0Data, err := result.ColumnData(0)
		assert.Nil(t, err)
		pCol1Data, err := result.ColumnData(1)
		assert.Nil(t, err)

		pCol0Mask := result.NullMaskData(0)
		pCol1Mask := result.NullMaskData(1)
		col0MaskSlice := UnsafeSimpleDataToSlice[bool](pCol0Mask, rowCount)
		pCol0DataSlice := UnsafeSimpleDataToSlice[int32](pCol0Data, rowCount)
		col1MaskSlice := UnsafeSimpleDataToSlice[bool](pCol1Mask, rowCount)
		pCol1DataSlice := UnsafeSimpleDataToSlice[int32](pCol1Data, rowCount)
		for row := uint64(0); row < rowCount; row++ {
			if col0MaskSlice[row] == false {
				assert.Equal(t, true, pCol0DataSlice[row] > 0)
			}
			if col1MaskSlice[row] == false {
				assert.Equal(t, true, pCol1DataSlice[row] > 0)
			}
		}

	})
	t.Run("prepared", func(t *testing.T) {
		db, err := Open("")
		assert.Nil(t, err)
		defer db.Close()
		conn, err := db.Connection()
		assert.Nil(t, err)
		defer conn.Disconnect()

		assert.Nil(t, conn.Query("CREATE TABLE integers(i INTEGER, j INTEGER);", nil))
		stmt := new(PreparedStatement)
		assert.Nil(t, conn.Prepare("INSERT INTO integers VALUES ($1, $2)", stmt))

		assert.Nil(t, stmt.BindInt32(1, 42))
		assert.Nil(t, stmt.BindInt32(2, 43))
		assert.Nil(t, stmt.ExecutePrepared(nil))
		stmt.Destroy()

		// we can also query result sets using prepared statements
		assert.Nil(t, conn.Prepare("SELECT * FROM integers WHERE i = ?", stmt))
		assert.Nil(t, stmt.BindInt32(1, 42))
		result := new(Result)
		assert.Nil(t, stmt.ExecutePrepared(result))
		// do something with result

		// clean up
		result.Destroy()
		stmt.Destroy()
	})

	t.Run("appender", func(t *testing.T) {
		db, err := Open("")
		assert.Nil(t, err)
		defer db.Close()
		conn, err := db.Connection()
		assert.Nil(t, err)
		defer conn.Disconnect()

		assert.Nil(t, conn.Query("CREATE TABLE people(id INTEGER, name VARCHAR)", nil))
		appender, err := conn.AppenderCreate("", "people")
		assert.Nil(t, err)

		assert.Nil(t, appender.AppendInt32(1))
		assert.Nil(t, appender.AppendVarChar("Mark"))
		assert.Nil(t, appender.EndRow())

		assert.Nil(t, appender.AppendInt32(2))
		assert.Nil(t, appender.AppendVarChar("Hannes"))
		assert.Nil(t, appender.EndRow())
		appender.Destroy()

		result := new(Result)
		assert.Nil(t, conn.Query("SELECT * FROM people", result))
		assert.Equal(t, int32(1), result.ValueInt32(0, 0))
		assert.Equal(t, int32(2), result.ValueInt32(0, 1))
		assert.Equal(t, "Mark", result.ValueVarCharInternal(1, 0))
		assert.Equal(t, "Hannes", result.ValueVarCharInternal(1, 1))
		assert.Equal(t, "", result.ValueVarCharInternal(0, 0))
		result.Destroy()
	})
}
