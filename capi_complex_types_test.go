package duckdbcapi

/*
reference from https://github.com/duckdb/duckdb/blob/master/test/api/capi/test_capi_complex_types.cpp
*/

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecimalTypesInCAPI(t *testing.T) {
	var tester CAPITester

	// open the database in in-memory mode
	assert.Equal(t, true, tester.OpenDatabase(""))
	defer tester.CleanUp()
	var result CAPIResult
	assert.Nil(t, tester.Query("SELECT 1.0::DECIMAL(4,1), 2.0::DECIMAL(9,2), 3.0::DECIMAL(18,3), 4.0::DECIMAL(38,4), 5::INTEGER", &result))
	defer result.Destroy()
	assert.Equal(t, uint64(5), result.ColumnCount())
	assert.Equal(t, "", result.ErrorMessage())
	assert.Equal(t, false, VectorSize() < 64)

	// fetch the first chunk
	assert.Equal(t, uint64(1), result.ChunkCount())
	chunk, err := result.FetchChunk(0)
	assert.Nil(t, err)

	widths := []uint8{4, 9, 18, 38, 0}
	scales := []uint8{1, 2, 3, 4, 0}
	types := []Type{DuckDBTypeDecimal, DuckDBTypeDecimal, DuckDBTypeDecimal, DuckDBTypeDecimal, DuckDBTypeInteger}
	internalTypes := []Type{DuckDBTypeSmallInt, DuckDBTypeInteger, DuckDBTypeBigInt, DuckDBTypeHugeInt, DuckDBTypeInvalid}
	for i := uint64(0); i < result.ColumnCount(); i++ {
		vec, err := chunk.GetChunk().GetVector(i)
		assert.Nil(t, err)
		logicalType := vec.GetColumnType()
		assert.Equal(t, types[i], logicalType.GetTypeId())
		assert.Equal(t, widths[i], logicalType.DecimalWidth())
		assert.Equal(t, scales[i], logicalType.DecimalScale())
		assert.Equal(t, internalTypes[i], logicalType.DecimalInternalType())
		logicalType.Destroy()
	}
	var lt LogicalType
	assert.Equal(t, uint8(0), lt.DecimalWidth())
	assert.Equal(t, uint8(0), lt.DecimalScale())
	assert.Equal(t, DuckDBTypeInvalid, lt.DecimalInternalType())

}

func TestEnumTypesInCAPI(t *testing.T) {
	var tester CAPITester
	assert.Equal(t, false, VectorSize() < 64)
	// open the database in in-memory mode
	assert.Equal(t, true, tester.OpenDatabase(""))
	defer tester.CleanUp()
	var result CAPIResult
	assert.Nil(t, tester.Query("select small_enum, medium_enum, large_enum, int from test_all_types();", &result))
	defer result.Destroy()
	assert.Equal(t, uint64(4), result.ColumnCount())
	assert.Equal(t, "", result.ErrorMessage())

	// fetch the first chunk
	assert.Equal(t, uint64(1), result.ChunkCount())
	chunk, err := result.FetchChunk(0)
	assert.Nil(t, err)

	types := []Type{DuckDBTypeEnum, DuckDBTypeEnum, DuckDBTypeEnum, DuckDBTypeInteger}
	internalTypes := []Type{DuckDBTypeUTinyInt, DuckDBTypeUSmallInt, DuckDBTypeUInteger, DuckDBTypeInvalid}
	dictionarySizes := []uint32{2, 300, 70000, 0}
	dictionaryStrings := []string{"DUCK_DUCK_ENUM", "enum_0", "enum_0", ""}
	for i := uint64(0); i < result.ColumnCount(); i++ {
		vec, err := chunk.GetChunk().GetVector(i)
		assert.Nil(t, err)
		logicalType := vec.GetColumnType()
		assert.Equal(t, types[i], logicalType.GetTypeId())
		assert.Equal(t, internalTypes[i], logicalType.EnumInternalType())
		assert.Equal(t, dictionarySizes[i], logicalType.EnumDictionarySize())
		assert.Equal(t, dictionaryStrings[i], logicalType.EnumDictionaryValue(0))
		logicalType.Destroy()
	}
	var lt LogicalType
	assert.Equal(t, DuckDBTypeInvalid, lt.EnumInternalType())
	assert.Equal(t, uint32(0), lt.EnumDictionarySize())
	assert.Equal(t, "", lt.EnumDictionaryValue(0))
}

func TestListTypesInCAPI(t *testing.T) {
	var tester CAPITester
	// open the database in in-memory mode
	assert.Equal(t, true, tester.OpenDatabase(""))
	defer tester.CleanUp()
	var result CAPIResult
	assert.Nil(t, tester.Query("select [1, 2, 3] l, ['hello', 'world'] s, [[1, 2, 3], [4, 5]] nested, 3::int", &result))
	defer result.Destroy()
	assert.Equal(t, uint64(4), result.ColumnCount())
	assert.Equal(t, "", result.ErrorMessage())

	// fetch the first chunk
	assert.Equal(t, uint64(1), result.ChunkCount())
	chunk, err := result.FetchChunk(0)
	assert.Nil(t, err)

	types := []Type{DuckDBTypeList, DuckDBTypeList, DuckDBTypeList, DuckDBTypeInteger}
	childTypes1 := []Type{DuckDBTypeInteger, DuckDBTypeVarChar, DuckDBTypeList, DuckDBTypeInvalid}
	childTypes2 := []Type{DuckDBTypeInvalid, DuckDBTypeInvalid, DuckDBTypeInteger, DuckDBTypeInvalid}
	for i := uint64(0); i < result.ColumnCount(); i++ {
		vec, err := chunk.GetChunk().GetVector(i)
		assert.Nil(t, err)
		logicalType := vec.GetColumnType()
		assert.Equal(t, types[i], logicalType.GetTypeId())
		ct1 := logicalType.ListTypeChildType()
		ct2 := ct1.ListTypeChildType()
		assert.Equal(t, childTypes1[i], ct1.GetTypeId())
		assert.Equal(t, childTypes2[i], ct2.GetTypeId())
		ct1.Destroy()
		ct2.Destroy()
		logicalType.Destroy()
	}
	var lt LogicalType
	assert.Equal(t, uintptr(0), uintptr(lt.ListTypeChildType().c))
}

func TestStructTypesInCAPI(t *testing.T) {
	var tester CAPITester
	// open the database in in-memory mode
	assert.Equal(t, true, tester.OpenDatabase(""))
	defer tester.CleanUp()
	var result CAPIResult
	assert.Nil(t, tester.Query("select {'a': 42::int}, {'b': 'hello', 'c': [1, 2, 3]}, {'d': {'e': 42}}, 3::int", &result))
	defer result.Destroy()
	assert.Equal(t, uint64(4), result.ColumnCount())
	assert.Equal(t, "", result.ErrorMessage())

	// fetch the first chunk
	assert.Equal(t, uint64(1), result.ChunkCount())
	chunk, err := result.FetchChunk(0)
	assert.Nil(t, err)

	types := []Type{DuckDBTypeStruct, DuckDBTypeStruct, DuckDBTypeStruct, DuckDBTypeInteger}
	childCount := []uint64{1, 2, 1, 0}
	childName := [][]string{{"a"}, {"b", "c"}, {"d"}, {}}
	chileType := [][]Type{{DuckDBTypeInteger}, {DuckDBTypeVarChar, DuckDBTypeList}, {DuckDBTypeStruct}, {}}
	for i := uint64(0); i < result.ColumnCount(); i++ {
		vec, err := chunk.GetChunk().GetVector(i)
		assert.Nil(t, err)
		logicalType := vec.GetColumnType()
		assert.Equal(t, types[i], logicalType.GetTypeId())
		assert.Equal(t, childCount[i], logicalType.StructTypeChildCount())
		for cIdx := uint64(0); cIdx < childCount[i]; cIdx++ {
			assert.Equal(t, childName[i][cIdx], logicalType.StructTypeChildName(cIdx))
			ct := logicalType.StructTypeChildType(cIdx)
			assert.Equal(t, chileType[i][cIdx], ct.GetTypeId())
			ct.Destroy()
		}
		logicalType.Destroy()
	}
	var lt LogicalType
	assert.Equal(t, uint64(0), lt.StructTypeChildCount())
	assert.Equal(t, "", lt.StructTypeChildName(0))
	assert.Equal(t, uintptr(0), uintptr(lt.StructTypeChildType(0).c))

}
