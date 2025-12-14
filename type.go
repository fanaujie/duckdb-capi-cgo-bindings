package duckdbcapi

/*
#include <duckdb.h>
*/
import "C"
import "unsafe"

type Type C.duckdb_type
type Double float64
type Float float32

const (
	DuckDBTypeInvalid Type = C.DUCKDB_TYPE_INVALID
	// bool
	DuckDBTypeBoolean Type = C.DUCKDB_TYPE_BOOLEAN
	// int8_t
	DuckDBTypeTinyInt Type = C.DUCKDB_TYPE_TINYINT
	// int16_t
	DuckDBTypeSmallInt Type = C.DUCKDB_TYPE_SMALLINT
	// int32_t
	DuckDBTypeInteger Type = C.DUCKDB_TYPE_INTEGER
	// int64_t
	DuckDBTypeBigInt Type = C.DUCKDB_TYPE_BIGINT
	// uint8_t
	DuckDBTypeUTinyInt Type = C.DUCKDB_TYPE_UTINYINT
	// uint16_t
	DuckDBTypeUSmallInt Type = C.DUCKDB_TYPE_USMALLINT
	// uint32_t
	DuckDBTypeUInteger Type = C.DUCKDB_TYPE_UINTEGER
	// uint64_t
	DuckDBTypeUBigInt Type = C.DUCKDB_TYPE_UBIGINT
	// float
	DuckDBTypeFloat Type = C.DUCKDB_TYPE_FLOAT
	// double
	DuckDBTypeDouble Type = C.DUCKDB_TYPE_DOUBLE
	// duckdb_timestamp, in microseconds
	DuckDBTypeTimestamp Type = C.DUCKDB_TYPE_TIMESTAMP
	// duckdb_date
	DuckDBTypeDate Type = C.DUCKDB_TYPE_DATE
	// duckdb_time
	DuckDBTypeTime Type = C.DUCKDB_TYPE_TIME
	// duckdb_interval
	DuckDBTypeInterval Type = C.DUCKDB_TYPE_INTERVAL
	// duckdb_hugeint
	DuckDBTypeHugeInt Type = C.DUCKDB_TYPE_HUGEINT
	// const char*
	DuckDBTypeVarChar Type = C.DUCKDB_TYPE_VARCHAR
	// duckdb_blob
	DuckDBTypeBlob Type = C.DUCKDB_TYPE_BLOB
	// decimal
	DuckDBTypeDecimal Type = C.DUCKDB_TYPE_DECIMAL
	// duckdb_timestamp, in seconds
	DuckDBTypeTimestamp_S Type = C.DUCKDB_TYPE_TIMESTAMP_S
	// duckdb_timestamp, in milliseconds
	DuckDBTypeTimestamp_MS Type = C.DUCKDB_TYPE_TIMESTAMP_MS
	// duckdb_timestamp, in nanoseconds
	DuckDBTypeTimestamp_NS Type = C.DUCKDB_TYPE_TIMESTAMP_NS
	// enum type, only useful as logical type
	DuckDBTypeEnum Type = C.DUCKDB_TYPE_ENUM
	// list type, only useful as logical type
	DuckDBTypeList Type = C.DUCKDB_TYPE_LIST
	// struct type, only useful as logical type
	DuckDBTypeStruct Type = C.DUCKDB_TYPE_STRUCT
	// map type, only useful as logical type
	DuckDBTypeMap Type = C.DUCKDB_TYPE_MAP
	// duckdb_hugeint
	DuckDBTypeUUID Type = C.DUCKDB_TYPE_UUID
	// const char*
	DuckDBTypeJson Type = C.DUCKDB_TYPE_JSON
)

type HugeInt struct {
	c C.duckdb_hugeint
}

func InitHugInt(lower uint64, upper int64) HugeInt {
	h := HugeInt{}
	h.c.lower = C.ulong(lower)
	h.c.upper = C.long(upper)
	return h
}

func (h *HugeInt) Lower() uint64 {
	return uint64(h.c.lower)
}

func (h *HugeInt) Upper() int64 {
	return int64(h.c.upper)
}

type Date struct {
	c C.duckdb_date
}

func InitDate(days int32) Date {
	d := Date{}
	d.c.days = C.int(days)
	return d
}

func (d *Date) Days() int32 {
	return int32(d.c.days)
}

type DateStruct struct {
	c C.duckdb_date_struct
}

func InitDateStruct(year int32, month int8, day int8) DateStruct {
	d := DateStruct{}
	d.c.year = C.int(year)
	d.c.month = C.schar(month)
	d.c.day = C.schar(day)
	return d
}

func (d *DateStruct) Year() int32 {
	return int32(d.c.year)
}
func (d *DateStruct) Month() int8 {
	return int8(d.c.month)
}
func (d *DateStruct) Day() int8 {
	return int8(d.c.day)
}

type Time struct {
	c C.duckdb_time
}

func InitTime(micros int64) Time {
	t := Time{}
	t.c.micros = C.long(micros)
	return t
}

func (t *Time) Micros() int64 {
	return int64(t.c.micros)
}

type TimeStruct struct {
	c C.duckdb_time_struct
}

func InitTimeStruct(hour, min, sec int8, micros int32) TimeStruct {
	t := TimeStruct{}
	t.c.hour = C.schar(hour)
	t.c.min = C.schar(min)
	t.c.sec = C.schar(sec)
	t.c.micros = C.int(micros)
	return t
}

func (s *TimeStruct) Hour() int8 {
	return int8(s.c.hour)
}
func (s *TimeStruct) Min() int8 {
	return int8(s.c.min)
}
func (s *TimeStruct) Sec() int8 {
	return int8(s.c.sec)
}
func (s *TimeStruct) Micros() int32 {
	return int32(s.c.micros)
}

type Timestamp struct {
	c C.duckdb_timestamp
}

func InitTimestamp(micros int64) Timestamp {
	t := Timestamp{}
	t.c.micros = C.long(micros)
	return t
}
func (t *Timestamp) Micros() int64 {
	return int64(t.c.micros)
}

type TimestampStruct struct {
	c C.duckdb_timestamp_struct
}

func InitTimestampStruct(date DateStruct, time TimeStruct) TimestampStruct {
	t := TimestampStruct{}
	t.c.date = date.c
	t.c.time = time.c
	return t
}

func (t *TimestampStruct) Date() DateStruct {
	return DateStruct{t.c.date}
}

func (t *TimestampStruct) Time() TimeStruct {
	return TimeStruct{t.c.time}
}

type Blob struct {
	c C.duckdb_blob
}

func (b *Blob) UnsafeDataToSlice() []byte {
	if b.c.data == nil || b.c.size == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(b.c.data)), uint64(b.c.size))
}

func (b *Blob) Size() uint64 {
	return uint64(b.c.size)
}

func (b *Blob) Free() {
	C.duckdb_free(b.c.data)
	b.c.data = nil
}

type Decimal struct {
	c C.duckdb_decimal
}

func InitDecimal(width, scale uint8, value HugeInt) Decimal {
	d := Decimal{}
	d.c.width = C.uchar(width)
	d.c.scale = C.uchar(scale)
	d.c.value = value.c
	return d
}

func (d *Decimal) Width() uint8 {
	return uint8(d.c.width)
}
func (d *Decimal) Scale() uint8 {
	return uint8(d.c.scale)
}
func (d *Decimal) Value() HugeInt {
	return HugeInt{d.c.value}
}

type Interval struct {
	c C.duckdb_interval
}

func InitInterval(months, days int32, micros int64) Interval {
	i := Interval{}
	i.c.months = C.int(months)
	i.c.days = C.int(days)
	i.c.micros = C.long(micros)
	return i
}

func (i *Interval) Months() int32 {
	return int32(i.c.months)
}
func (i *Interval) Days() int32 {
	return int32(i.c.days)
}
func (i *Interval) Micros() int64 {
	return int64(i.c.micros)
}
