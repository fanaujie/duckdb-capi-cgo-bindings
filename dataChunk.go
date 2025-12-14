package duckdbcapi

/*
#include <duckdb.h>
*/
import "C"

type DataChunk struct {
	c C.duckdb_data_chunk
}

func CreateDataChunk(logicalType []*LogicalType, columnCount uint64) (*DataChunk, error) {
	var dc []C.duckdb_logical_type
	for _, lt := range logicalType {
		dc = append(dc, lt.c)
	}
	dataChunk := C.duckdb_create_data_chunk((*C.duckdb_logical_type)(&dc[0]), C.idx_t(columnCount))
	if dataChunk == nil {
		return nil, ErrDataChunkNil
	}
	return &DataChunk{dataChunk}, nil
}

func (c *DataChunk) Destroy() {
	C.duckdb_destroy_data_chunk(&c.c)
}

func (c *DataChunk) Reset() {
	C.duckdb_data_chunk_reset(c.c)
}

func (c *DataChunk) GetColumnCount() uint64 {
	return uint64(C.duckdb_data_chunk_get_column_count(c.c))
}

func (c *DataChunk) GetSize() uint64 {
	return uint64(C.duckdb_data_chunk_get_size(c.c))
}

func (c *DataChunk) SetSize(size uint64) {
	C.duckdb_data_chunk_set_size(c.c, C.idx_t(size))
}

func (c *DataChunk) GetVector(col uint64) (*Vector, error) {
	v := C.duckdb_data_chunk_get_vector(c.c, C.idx_t(col))
	if v == nil {
		return nil, ErrVectorNil
	}
	return &Vector{c: v}, nil
}
