#include <duckdb.h>

extern void goBridgeReplacementCallback(duckdb_replacement_scan_info info, const char *table_name, void *data);
extern void goBridgeReplacementDeleteCallback(void *data);
extern void goBridgeSetBindCallback(duckdb_bind_info info);
extern void goBridgeSetInitCallback(duckdb_init_info info);
extern void goBridgeSetFunctionCallback(duckdb_function_info info,duckdb_data_chunk output);
extern void goBridgeDeleteSetExtraInfoCallback(void *data);
extern void goBridgeDeleteTableFunctionBindData(void *data);
extern void goBridgeDeleteTableFunctionInitData(void *data);


void callGoBridgeReplacementCallback(duckdb_replacement_scan_info info, const char *table_name, void *data) {
	goBridgeReplacementCallback(info,table_name,data);
}

void callGoBridgeReplacementDeleteCallback(void *data) {
	goBridgeReplacementDeleteCallback(data);
}

void callGoBridgeSetBindCallback(duckdb_bind_info info) {
	goBridgeSetBindCallback(info);
}

void callGoBridgeSetInitCallback(duckdb_init_info info) {
	goBridgeSetInitCallback(info);
}

void callGoBridgeSetFunctionCallback(duckdb_function_info info,duckdb_data_chunk output) {
	goBridgeSetFunctionCallback(info,output);
}

void callGoBridgeDeleteSetExtraInfoCallback(void *data) {
	goBridgeDeleteSetExtraInfoCallback(data);
}

void callGoBridgeDeleteTableFunctionBindData(void *data) {
	goBridgeDeleteTableFunctionBindData(data);
}

void callGoBridgeDeleteTableFunctionInitData(void *data) {
	goBridgeDeleteTableFunctionInitData(data);
}
