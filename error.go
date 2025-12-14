package duckdbcapi

import "errors"

var (
	ErrDuckDBError             = errors.New("ErrDuckDBError")
	ErrColumnNameNil           = errors.New("ErrColumnNameNil")
	ErrColumnDataNil           = errors.New("ErrColumnDataNil")
	ErrDataChunkNil            = errors.New("ErrDataChunkNil")
	ErrNullMaskDataNil         = errors.New("ErrNullMaskDataNil")
	ErrVectorNil               = errors.New("ErrVectorNil")
	ErrVectorGetDataNil        = errors.New("ErrVectorGetDataNil")
	ErrVectorGetValidityNil    = errors.New("ErrVectorGetValidityNil")
	ErrVectorGetListChildNil   = errors.New("ErrVectorGetListChildNil")
	ErrVectorGetStructChildNil = errors.New("ErrVectorGetStructChildNil")
)
