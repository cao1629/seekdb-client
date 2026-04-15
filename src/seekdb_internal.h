#pragma once

#include "seekdb.h"

#include <mysql.h>
#include <stddef.h>
#include <stdint.h>

typedef struct {
    char *db_dir;
    int   port;
    char *host;
    char *default_user;
    char *default_password;
} SeekdbHandleImpl;

typedef struct {
    MYSQL *mysql;
} SeekdbConnectionImpl;

typedef struct {
    MYSQL_STMT  *stmt;
    int          param_count;
    MYSQL_BIND  *param_binds;       // calloc'd to param_count entries
    int64_t     *param_int64_buf;   // backing storage for bound int64s
    int          bound;             // 1 = mysql_stmt_bind_param has been called
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
    SEEKDB_RESULT_KIND_QUERY = 1,   // backed by MYSQL_RES (text protocol)
    SEEKDB_RESULT_KIND_STMT  = 2,   // backed by MYSQL_STMT (binary protocol)
} SeekdbResultKind;

typedef struct {
    SeekdbResultKind kind;
    int              column_count;

    /* QUERY-kind state */
    MYSQL_RES       *mysql_res;
    MYSQL_ROW        current_row;
    unsigned long   *current_lengths;

    /* STMT-kind state */
    MYSQL_STMT      *stmt_ref;          // not owned; owned by SeekdbStmtImpl
    MYSQL_BIND      *result_binds;
    char           **result_str_buffers;
    unsigned long   *result_str_lens;
    char            *result_is_null;    // 'my_bool' is just char in libmariadb
    char            *result_error;
    enum enum_field_types *result_field_types;
} SeekdbResultImpl;
