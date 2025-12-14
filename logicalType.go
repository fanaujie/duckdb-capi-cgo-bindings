package duckdbcapi

/*
#include <duckdb.h>
*/
import "C"

type LogicalType struct {
	c C.duckdb_logical_type
}

func CreateLogicalType(dbType Type) *LogicalType {
	return &LogicalType{C.duckdb_create_logical_type(C.duckdb_type(dbType))}
}

func CreateDecimalType(width, scale uint8) *LogicalType {
	return &LogicalType{C.duckdb_create_decimal_type(C.uchar(width), C.uchar(scale))}
}

func (l *LogicalType) Destroy() {
	C.duckdb_destroy_logical_type(&l.c)
}

func (l *LogicalType) GetTypeId() Type {
	return Type(C.duckdb_get_type_id(l.c))
}
func (l *LogicalType) DecimalWidth() uint8 {
	return uint8(C.duckdb_decimal_width(l.c))
}

func (l *LogicalType) DecimalScale() uint8 {
	return uint8(C.duckdb_decimal_scale(l.c))
}

func (l *LogicalType) DecimalInternalType() Type {
	return Type(C.duckdb_decimal_internal_type(l.c))
}

func (l *LogicalType) EnumInternalType() Type {
	return Type(C.duckdb_enum_internal_type(l.c))
}

func (l *LogicalType) EnumDictionarySize() uint32 {
	return uint32(C.duckdb_enum_dictionary_size(l.c))
}

func (l *LogicalType) EnumDictionaryValue(index uint64) string {
	return C.GoString(C.duckdb_enum_dictionary_value(l.c, C.idx_t(index)))
}

func (l *LogicalType) ListTypeChildType() *LogicalType {
	return &LogicalType{C.duckdb_list_type_child_type(l.c)}
}

func (l *LogicalType) StructTypeChildCount() uint64 {
	return uint64(C.duckdb_struct_type_child_count(l.c))
}

func (l *LogicalType) StructTypeChildName(index uint64) string {
	return C.GoString(C.duckdb_struct_type_child_name(l.c, C.idx_t(index)))
}

func (l *LogicalType) StructTypeChildType(index uint64) *LogicalType {
	return &LogicalType{C.duckdb_struct_type_child_type(l.c, C.idx_t(index))}
}
