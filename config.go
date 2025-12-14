package duckdbcapi

/*
#include <duckdb.h>
*/
import "C"
import "unsafe"

func ConfigCount() uint64 {
	return uint64(C.duckdb_config_count())
}

func GetConfigFlag(index uint64) (string, string, error) {
	var pName *C.char
	var pDescription *C.char

	if C.duckdb_get_config_flag(C.ulong(index), &pName, &pDescription) == C.DuckDBError {
		return "", "", ErrDuckDBError
	}

	return C.GoString(pName), C.GoString(pDescription), nil
}

type Config struct {
	c C.duckdb_config
}

func CreateConfig() (*Config, error) {
	var cfg Config
	if C.duckdb_create_config(&cfg.c) == C.DuckDBError {
		return nil, ErrDuckDBError
	}
	return &cfg, nil
}

func (c *Config) Destroy() {
	C.duckdb_destroy_config(&c.c)
}

func (c *Config) SetConfig(name, option string) error {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	cOption := C.CString(option)
	defer C.free(unsafe.Pointer(cOption))
	if C.duckdb_set_config(c.c, cName, cOption) == C.DuckDBError {
		return ErrDuckDBError
	}
	return nil
}
