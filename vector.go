package duckdbcapi

/*
#include <duckdb.h>
*/
import "C"
import "unsafe"

type Vector struct {
	c C.duckdb_vector
}

func (v *Vector) GetColumnType() *LogicalType {
	return &LogicalType{C.duckdb_vector_get_column_type(v.c)}
}

func (v *Vector) GetData() (unsafe.Pointer, error) {
	pData := C.duckdb_vector_get_data(v.c)
	if pData == nil {
		return nil, ErrVectorGetDataNil
	}
	return pData, nil
}

func (v *Vector) GetValidity() (*Validity, error) {
	pData := C.duckdb_vector_get_validity(v.c)
	if pData == nil {
		return nil, ErrVectorGetValidityNil
	}
	return &Validity{pData}, nil
}

func (v *Vector) EnsureValidityWritable() {
	C.duckdb_vector_ensure_validity_writable(v.c)
}

func (v *Vector) AssignStringElement(index uint64, str string) {
	cStr := C.CString(str)
	defer C.free(unsafe.Pointer(cStr))
	C.duckdb_vector_assign_string_element(v.c, C.idx_t(index), cStr)
}

func (v *Vector) AssignStringElementLen(index uint64, str string, strLen uint64) {
	cStr := C.CString(str)
	defer C.free(unsafe.Pointer(cStr))
	C.duckdb_vector_assign_string_element_len(v.c, C.idx_t(index), cStr, C.idx_t(strLen))
}

func (v *Vector) ListGetChild() (*Vector, error) {
	pData := C.duckdb_list_vector_get_child(v.c)
	if pData == nil {
		return nil, ErrVectorGetListChildNil
	}
	return &Vector{pData}, nil
}

func (v *Vector) ListGetSize() uint64 {
	return uint64(C.duckdb_list_vector_get_size(v.c))
}

func (v *Vector) StructGetChild(index uint64) (*Vector, error) {
	pData := C.duckdb_struct_vector_get_child(v.c, C.idx_t(index))
	if pData == nil {
		return nil, ErrVectorGetStructChildNil
	}
	return &Vector{pData}, nil
}
