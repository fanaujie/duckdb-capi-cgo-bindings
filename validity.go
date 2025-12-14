package duckdbcapi

/*
#include <duckdb.h>
*/
import "C"

type Validity struct {
	c *C.ulong
}

func (v *Validity) RowIsValid(row uint64) bool {
	return bool(C.duckdb_validity_row_is_valid(v.c, C.idx_t(row)))
}
func (v *Validity) SetRowValidity(row uint64, valid bool) {
	C.duckdb_validity_set_row_validity(v.c, C.idx_t(row), C.bool(valid))
}
func (v *Validity) SetRowInvalid(row uint64) {
	C.duckdb_validity_set_row_invalid(v.c, C.idx_t(row))
}
func (v *Validity) SetRowValid(row uint64) {
	C.duckdb_validity_set_row_valid(v.c, C.idx_t(row))
}
