#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void *SeekdbHandle;        // a seekdb instance
typedef void *SeekdbConnection;
typedef void *SeekdbResult;
typedef void *SeekdbStmt;
typedef void *SeekdbType;
typedef void *SeekdbValue;
// typedef void *SeekdbConfig;

typedef enum {
    SEEKDB_SUCCESS = 0,
    SEEKDB_INTERNAL_ERROR = -1,
    SEEKDB_INVALID_ARGUMENT = -2,
    // ...
} SeekdbReturnCode;

typedef enum {
    SEEKDB_TYPE_NULL,
    SEEKDB_TYPE_INT64,
    SEEKDB_TYPE_UINT64,
    SEEKDB_TYPE_FLOAT,
    SEEKDB_TYPE_DECIMAL,
    SEEKDB_TYPE_DATE,
    SEEKDB_TYPE_DATETIME,
    SEEKDB_TYPE_TIMESTAMP,
    SEEKDB_TYPE_VARCHAR,
    // ...
} SeekdbTypeId;

// bin_path: full path to the seekdb binary
// db_dir:   data directory (also used as base-dir / cwd for the server)
// port:     TCP listen port (port <= 0: do not listen on the network address)
int seekdb_open(const char *bin_path, const char *db_dir, int port,
                SeekdbHandle *out_handle);
int seekdb_close(SeekdbHandle handle);

int seekdb_connect(SeekdbHandle handle, const char *database, bool autocommit,
                   SeekdbConnection *out_connection);
int seekdb_disconnect(SeekdbConnection connection);

int seekdb_stmt_prepare(SeekdbConnection connection, const char *sql,
                        SeekdbStmt *out_stmt);
int seekdb_stmt_free(SeekdbStmt stmt);
int seekdb_stmt_param_count(SeekdbStmt stmt);
int seekdb_stmt_bind(SeekdbStmt stmt, int64_t index, SeekdbValue value);
int seekdb_stmt_bind_int64(SeekdbStmt stmt, int64_t index, int64_t value);
int seekdb_stmt_clear_bindings(SeekdbStmt stmt);
int seekdb_stmt_execute(SeekdbStmt stmt, SeekdbResult *out_result);

int seekdb_query(SeekdbConnection connection, const char *sql, int64_t sql_len,
                 SeekdbResult *out_result);

int seekdb_result_free(SeekdbResult result);
int seekdb_result_column_count(SeekdbResult result, int64_t *out_ncolumn);
int seekdb_result_column_name(SeekdbResult result, int64_t index,
                              const char **out_name);
int seekdb_result_column_type_id(SeekdbResult result, int64_t index,
                                 SeekdbTypeId *out_typeid);
int seekdb_result_row_count(SeekdbResult result, int64_t *out_nrows);
int seekdb_result_next(SeekdbResult result);
int seekdb_result_get_value(SeekdbResult result, int64_t index,
                            SeekdbValue *out_value);
int seekdb_result_get_int64(SeekdbResult result, int64_t index,
                            int64_t *out_value);
int seekdb_result_get_float(SeekdbResult result, int64_t index,
                            double *out_value);

int seekdb_trx_begin(SeekdbConnection connection);
int seekdb_trx_commit(SeekdbConnection connection);
int seekdb_trx_rollback(SeekdbConnection connection);

int seekdb_value_free(SeekdbValue value);
int seekdb_value_create_int64(int64_t int_value, SeekdbValue *out_value);
int seekdb_value_get_int64(SeekdbValue value, int64_t *out_value);

// utils
void *seekdb_malloc(size_t size);
void seekdb_free(void *ptr);

#ifdef __cplusplus
}
#endif
