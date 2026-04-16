#pragma once

#include "seekdb.h"

#include <mysql.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

typedef struct {
    char   *db_dir;
    char    sock_path[256];
    char    lock_path[256];
    char    pid_path[256];
    int     lock_fd;
} SeekdbHandleImpl;

typedef struct {
    MYSQL *mysql;
} SeekdbConnectionImpl;

typedef struct {
    MYSQL_STMT  *stmt;
    int          param_count;
    MYSQL_BIND  *param_binds;
    int64_t     *param_int64_buf;
    int          bound;
} SeekdbStmtImpl;

typedef struct {
    SeekdbTypeId type;
    union {
        int64_t  i64;
        uint64_t u64;
        double   f64;
        struct { char *data; size_t len; } str;
    } v;
} SeekdbValueImpl;

typedef enum {
    SEEKDB_RESULT_KIND_QUERY = 1,
    SEEKDB_RESULT_KIND_STMT  = 2,
} SeekdbResultKind;

#define BUFSZ_DEFAULT 1024

typedef struct {
    SeekdbResultKind kind;
    int              column_count;

    /* QUERY-kind state */
    MYSQL_RES       *mysql_res;
    MYSQL_ROW        current_row;
    unsigned long   *current_lengths;

    /* STMT-kind state */
    MYSQL_STMT      *stmt_ref;
    MYSQL_BIND      *result_binds;
    char           **result_str_buffers;
    unsigned long   *result_str_lens;
    char            *result_is_null;
    char            *result_error;
    enum enum_field_types *result_field_types;
} SeekdbResultImpl;
