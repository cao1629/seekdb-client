#include "seekdb_internal.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <mysql.h>

#define BUFSZ_DEFAULT 1024

static char *xstrdup(const char *s) { return s ? strdup(s) : NULL; }
static void  xfree(void *p) { if (p) free(p); }

/* ============================================================ utils ====== */

void *seekdb_malloc(size_t size) { return malloc(size); }
void  seekdb_free(void *ptr)     { xfree(ptr); }

/* ============================================================ handle ===== */

int seekdb_open(const char *db_dir, int port, SeekdbHandle *out_handle)
{
    if (!out_handle) return SEEKDB_INVALID_ARGUMENT;
    *out_handle = NULL;

    SeekdbHandleImpl *h = calloc(1, sizeof(*h));
    if (!h) return SEEKDB_INTERNAL_ERROR;

    h->db_dir           = xstrdup(db_dir);
    h->port             = port;
    h->host             = xstrdup("127.0.0.1");
    h->default_user     = xstrdup("root");
    h->default_password = xstrdup("");

    *out_handle = (SeekdbHandle)h;
    return SEEKDB_SUCCESS;
}

int seekdb_close(SeekdbHandle handle)
{
    if (!handle) return SEEKDB_INVALID_ARGUMENT;
    SeekdbHandleImpl *h = handle;
    xfree(h->db_dir);
    xfree(h->host);
    xfree(h->default_user);
    xfree(h->default_password);
    free(h);
    /* mysql_library_end() is process-wide; leave it for the application. */
    return SEEKDB_SUCCESS;
}

/* ======================================================= connection ===== */

int seekdb_connect(SeekdbHandle handle, const char *database, bool autocommit,
                   SeekdbConnection *out_connection)
{
    if (!handle || !out_connection) return SEEKDB_INVALID_ARGUMENT;
    *out_connection = NULL;

    SeekdbHandleImpl *h = handle;

    SeekdbConnectionImpl *c = calloc(1, sizeof(*c));
    if (!c) return SEEKDB_INTERNAL_ERROR;

    c->mysql = mysql_init(NULL);
    if (!c->mysql) { free(c); return SEEKDB_INTERNAL_ERROR; }

    unsigned int proto = MYSQL_PROTOCOL_TCP;
    mysql_options(c->mysql, MYSQL_OPT_PROTOCOL, &proto);

    if (!mysql_real_connect(c->mysql,
                            h->host,
                            h->default_user,
                            h->default_password,
                            database,
                            (unsigned int)h->port,
                            NULL /* unix_socket */,
                            CLIENT_MULTI_STATEMENTS))
    {
        fprintf(stderr,
                "seekdb: mysql_real_connect(host=%s port=%d user=%s db=%s) failed: "
                "%s (errno=%u sqlstate=%s)\n",
                h->host ? h->host : "(null)",
                h->port,
                h->default_user ? h->default_user : "(null)",
                database ? database : "(null)",
                mysql_error(c->mysql),
                mysql_errno(c->mysql),
                mysql_sqlstate(c->mysql));
        mysql_close(c->mysql);
        free(c);
        return SEEKDB_INTERNAL_ERROR;
    }

    if (!autocommit) {
        if (mysql_real_query(c->mysql, "SET autocommit=0", 16)) {
            mysql_close(c->mysql);
            free(c);
            return SEEKDB_INTERNAL_ERROR;
        }
    }

    *out_connection = (SeekdbConnection)c;
    return SEEKDB_SUCCESS;
}

int seekdb_disconnect(SeekdbConnection connection)
{
    if (!connection) return SEEKDB_INVALID_ARGUMENT;
    SeekdbConnectionImpl *c = connection;
    if (c->mysql) mysql_close(c->mysql);
    free(c);
    return SEEKDB_SUCCESS;
}

/* ======================================================= transactions == */

static int run_simple(SeekdbConnectionImpl *c, const char *sql, size_t len)
{
    if (mysql_real_query(c->mysql, sql, (unsigned long)len)) {
        return SEEKDB_INTERNAL_ERROR;
    }
    return SEEKDB_SUCCESS;
}

int seekdb_trx_begin(SeekdbConnection connection)
{
    if (!connection) return SEEKDB_INVALID_ARGUMENT;
    return run_simple(connection, "START TRANSACTION", 17);
}

int seekdb_trx_commit(SeekdbConnection connection)
{
    if (!connection) return SEEKDB_INVALID_ARGUMENT;
    return run_simple(connection, "COMMIT", 6);
}

int seekdb_trx_rollback(SeekdbConnection connection)
{
    if (!connection) return SEEKDB_INVALID_ARGUMENT;
    return run_simple(connection, "ROLLBACK", 8);
}

/* ============================================================ query ===== */

static SeekdbTypeId map_field_type(enum enum_field_types t)
{
    switch (t) {
        case MYSQL_TYPE_TINY:
        case MYSQL_TYPE_SHORT:
        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_LONGLONG:
        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_YEAR:        return SEEKDB_TYPE_INT64;
        case MYSQL_TYPE_FLOAT:
        case MYSQL_TYPE_DOUBLE:      return SEEKDB_TYPE_FLOAT;
        case MYSQL_TYPE_DECIMAL:
        case MYSQL_TYPE_NEWDECIMAL:  return SEEKDB_TYPE_DECIMAL;
        case MYSQL_TYPE_DATE:        return SEEKDB_TYPE_DATE;
        case MYSQL_TYPE_DATETIME:    return SEEKDB_TYPE_DATETIME;
        case MYSQL_TYPE_TIMESTAMP:   return SEEKDB_TYPE_TIMESTAMP;
        case MYSQL_TYPE_NULL:        return SEEKDB_TYPE_NULL;
        case MYSQL_TYPE_VARCHAR:
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_STRING:      return SEEKDB_TYPE_VARCHAR;
        default:                     return SEEKDB_TYPE_VARCHAR;
    }
}

int seekdb_query(SeekdbConnection connection, const char *sql, int64_t sql_len,
                 SeekdbResult *out_result)
{
    if (!connection || !sql || !out_result) return SEEKDB_INVALID_ARGUMENT;
    *out_result = NULL;

    SeekdbConnectionImpl *c = connection;
    if (mysql_real_query(c->mysql, sql, (unsigned long)sql_len))
        return SEEKDB_INTERNAL_ERROR;

    MYSQL_RES *res = mysql_store_result(c->mysql);
    if (!res) {
        /* Either an error, or a no-result statement (INSERT/UPDATE/DDL).
         * Distinguish by checking field count. */
        if (mysql_field_count(c->mysql) == 0) {
            /* OK with no result set — return an empty result. */
        } else {
            return SEEKDB_INTERNAL_ERROR;
        }
    }

    SeekdbResultImpl *r = calloc(1, sizeof(*r));
    if (!r) { if (res) mysql_free_result(res); return SEEKDB_INTERNAL_ERROR; }

    r->kind         = SEEKDB_RESULT_KIND_QUERY;
    r->mysql_res    = res;
    r->column_count = res ? (int)mysql_num_fields(res) : 0;

    *out_result = (SeekdbResult)r;
    return SEEKDB_SUCCESS;
}

/* ====================================================== prepared stmt == */

int seekdb_stmt_prepare(SeekdbConnection connection, const char *sql,
                        SeekdbStmt *out_stmt)
{
    if (!connection || !sql || !out_stmt) return SEEKDB_INVALID_ARGUMENT;
    *out_stmt = NULL;

    SeekdbConnectionImpl *c = connection;

    MYSQL_STMT *m = mysql_stmt_init(c->mysql);
    if (!m) return SEEKDB_INTERNAL_ERROR;

    if (mysql_stmt_prepare(m, sql, (unsigned long)strlen(sql))) {
        mysql_stmt_close(m);
        return SEEKDB_INTERNAL_ERROR;
    }

    SeekdbStmtImpl *s = calloc(1, sizeof(*s));
    if (!s) { mysql_stmt_close(m); return SEEKDB_INTERNAL_ERROR; }

    s->stmt        = m;
    s->param_count = (int)mysql_stmt_param_count(m);
    if (s->param_count > 0) {
        s->param_binds     = calloc((size_t)s->param_count, sizeof(MYSQL_BIND));
        s->param_int64_buf = calloc((size_t)s->param_count, sizeof(int64_t));
        if (!s->param_binds || !s->param_int64_buf) {
            free(s->param_binds);
            free(s->param_int64_buf);
            free(s);
            mysql_stmt_close(m);
            return SEEKDB_INTERNAL_ERROR;
        }
    }

    *out_stmt = (SeekdbStmt)s;
    return SEEKDB_SUCCESS;
}

int seekdb_stmt_free(SeekdbStmt stmt)
{
    if (!stmt) return SEEKDB_INVALID_ARGUMENT;
    SeekdbStmtImpl *s = stmt;
    if (s->stmt) mysql_stmt_close(s->stmt);
    free(s->param_binds);
    free(s->param_int64_buf);
    free(s);
    return SEEKDB_SUCCESS;
}

int seekdb_stmt_param_count(SeekdbStmt stmt)
{
    if (!stmt) return -1;
    return ((SeekdbStmtImpl *)stmt)->param_count;
}

int seekdb_stmt_clear_bindings(SeekdbStmt stmt)
{
    if (!stmt) return SEEKDB_INVALID_ARGUMENT;
    SeekdbStmtImpl *s = stmt;
    if (s->param_binds && s->param_count > 0)
        memset(s->param_binds, 0, (size_t)s->param_count * sizeof(MYSQL_BIND));
    s->bound = 0;
    return SEEKDB_SUCCESS;
}

int seekdb_stmt_bind_int64(SeekdbStmt stmt, int64_t index, int64_t value)
{
    if (!stmt) return SEEKDB_INVALID_ARGUMENT;
    SeekdbStmtImpl *s = stmt;
    if (index < 0 || index >= s->param_count) return SEEKDB_INVALID_ARGUMENT;

    s->param_int64_buf[index] = value;

    MYSQL_BIND *b = &s->param_binds[index];
    memset(b, 0, sizeof(*b));
    b->buffer_type = MYSQL_TYPE_LONGLONG;
    b->buffer      = &s->param_int64_buf[index];
    b->is_unsigned = 0;

    s->bound = 0;  /* re-bind on next execute */
    return SEEKDB_SUCCESS;
}

int seekdb_stmt_bind(SeekdbStmt stmt, int64_t index, SeekdbValue value)
{
    if (!stmt || !value) return SEEKDB_INVALID_ARGUMENT;
    SeekdbValueImpl *v = value;
    /* Phase 1 only handles INT64 via the value path. */
    if (v->type != SEEKDB_TYPE_INT64) return SEEKDB_INVALID_ARGUMENT;
    return seekdb_stmt_bind_int64(stmt, index, v->v.i64);
}

static int stmt_setup_result(SeekdbStmtImpl *s, SeekdbResultImpl *r)
{
    MYSQL_RES *meta = mysql_stmt_result_metadata(s->stmt);
    if (!meta) {
        /* No result set (INSERT/UPDATE/DDL). */
        r->column_count = 0;
        return SEEKDB_SUCCESS;
    }
    int n = (int)mysql_num_fields(meta);
    r->column_count       = n;
    r->result_binds       = calloc((size_t)n, sizeof(MYSQL_BIND));
    r->result_str_buffers = calloc((size_t)n, sizeof(char *));
    r->result_str_lens    = calloc((size_t)n, sizeof(unsigned long));
    r->result_is_null     = calloc((size_t)n, sizeof(char));
    r->result_error       = calloc((size_t)n, sizeof(char));
    r->result_field_types = calloc((size_t)n, sizeof(enum enum_field_types));
    if (!r->result_binds || !r->result_str_buffers || !r->result_str_lens ||
        !r->result_is_null || !r->result_error || !r->result_field_types) {
        mysql_free_result(meta);
        return SEEKDB_INTERNAL_ERROR;
    }

    MYSQL_FIELD *fields = mysql_fetch_fields(meta);
    for (int i = 0; i < n; ++i) {
        r->result_field_types[i] = fields[i].type;
        r->result_str_buffers[i] = malloc(BUFSZ_DEFAULT);
        if (!r->result_str_buffers[i]) {
            mysql_free_result(meta);
            return SEEKDB_INTERNAL_ERROR;
        }
        MYSQL_BIND *b = &r->result_binds[i];
        b->buffer_type   = MYSQL_TYPE_STRING;
        b->buffer        = r->result_str_buffers[i];
        b->buffer_length = BUFSZ_DEFAULT;
        b->length        = &r->result_str_lens[i];
        b->is_null       = (void *)&r->result_is_null[i];
        b->error         = (void *)&r->result_error[i];
    }

    mysql_free_result(meta);

    if (mysql_stmt_bind_result(s->stmt, r->result_binds))
        return SEEKDB_INTERNAL_ERROR;
    if (mysql_stmt_store_result(s->stmt))
        return SEEKDB_INTERNAL_ERROR;

    return SEEKDB_SUCCESS;
}

int seekdb_stmt_execute(SeekdbStmt stmt, SeekdbResult *out_result)
{
    if (!stmt || !out_result) return SEEKDB_INVALID_ARGUMENT;
    *out_result = NULL;

    SeekdbStmtImpl *s = stmt;

    if (s->param_count > 0 && !s->bound) {
        if (mysql_stmt_bind_param(s->stmt, s->param_binds))
            return SEEKDB_INTERNAL_ERROR;
        s->bound = 1;
    }

    if (mysql_stmt_execute(s->stmt))
        return SEEKDB_INTERNAL_ERROR;

    SeekdbResultImpl *r = calloc(1, sizeof(*r));
    if (!r) return SEEKDB_INTERNAL_ERROR;
    r->kind     = SEEKDB_RESULT_KIND_STMT;
    r->stmt_ref = s->stmt;

    int rc = stmt_setup_result(s, r);
    if (rc != SEEKDB_SUCCESS) {
        seekdb_result_free((SeekdbResult)r);
        return rc;
    }

    *out_result = (SeekdbResult)r;
    return SEEKDB_SUCCESS;
}

/* =========================================================== result ==== */

int seekdb_result_free(SeekdbResult result)
{
    if (!result) return SEEKDB_INVALID_ARGUMENT;
    SeekdbResultImpl *r = result;

    if (r->kind == SEEKDB_RESULT_KIND_QUERY) {
        if (r->mysql_res) mysql_free_result(r->mysql_res);
    } else if (r->kind == SEEKDB_RESULT_KIND_STMT) {
        if (r->result_str_buffers) {
            for (int i = 0; i < r->column_count; ++i)
                xfree(r->result_str_buffers[i]);
            free(r->result_str_buffers);
        }
        free(r->result_binds);
        free(r->result_str_lens);
        free(r->result_is_null);
        free(r->result_error);
        free(r->result_field_types);
        if (r->stmt_ref) mysql_stmt_free_result(r->stmt_ref);
    }
    free(r);
    return SEEKDB_SUCCESS;
}

int seekdb_result_column_count(SeekdbResult result, int64_t *out_ncolumn)
{
    if (!result || !out_ncolumn) return SEEKDB_INVALID_ARGUMENT;
    *out_ncolumn = ((SeekdbResultImpl *)result)->column_count;
    return SEEKDB_SUCCESS;
}

int seekdb_result_column_name(SeekdbResult result, int64_t index,
                              const char **out_name)
{
    if (!result || !out_name) return SEEKDB_INVALID_ARGUMENT;
    SeekdbResultImpl *r = result;
    if (index < 0 || index >= r->column_count) return SEEKDB_INVALID_ARGUMENT;

    if (r->kind == SEEKDB_RESULT_KIND_QUERY) {
        MYSQL_FIELD *f = mysql_fetch_field_direct(r->mysql_res, (unsigned int)index);
        if (!f) return SEEKDB_INTERNAL_ERROR;
        *out_name = f->name;
        return SEEKDB_SUCCESS;
    } else {
        MYSQL_RES *meta = mysql_stmt_result_metadata(r->stmt_ref);
        if (!meta) return SEEKDB_INTERNAL_ERROR;
        MYSQL_FIELD *f = mysql_fetch_field_direct(meta, (unsigned int)index);
        if (!f) { mysql_free_result(meta); return SEEKDB_INTERNAL_ERROR; }
        *out_name = f->name;
        /* NB: name lifetime is tied to meta; leak meta intentionally for
         * Phase 1 simplicity — TODO: cache metadata in r at setup time. */
        return SEEKDB_SUCCESS;
    }
}

int seekdb_result_column_type_id(SeekdbResult result, int64_t index,
                                 SeekdbTypeId *out_typeid)
{
    if (!result || !out_typeid) return SEEKDB_INVALID_ARGUMENT;
    SeekdbResultImpl *r = result;
    if (index < 0 || index >= r->column_count) return SEEKDB_INVALID_ARGUMENT;

    if (r->kind == SEEKDB_RESULT_KIND_QUERY) {
        MYSQL_FIELD *f = mysql_fetch_field_direct(r->mysql_res, (unsigned int)index);
        if (!f) return SEEKDB_INTERNAL_ERROR;
        *out_typeid = map_field_type(f->type);
    } else {
        *out_typeid = map_field_type(r->result_field_types[index]);
    }
    return SEEKDB_SUCCESS;
}

int seekdb_result_row_count(SeekdbResult result, int64_t *out_nrows)
{
    if (!result || !out_nrows) return SEEKDB_INVALID_ARGUMENT;
    SeekdbResultImpl *r = result;
    if (r->kind == SEEKDB_RESULT_KIND_QUERY) {
        *out_nrows = r->mysql_res ? (int64_t)mysql_num_rows(r->mysql_res) : 0;
    } else {
        *out_nrows = (int64_t)mysql_stmt_num_rows(r->stmt_ref);
    }
    return SEEKDB_SUCCESS;
}

int seekdb_result_next(SeekdbResult result)
{
    if (!result) return SEEKDB_INVALID_ARGUMENT;
    SeekdbResultImpl *r = result;

    if (r->kind == SEEKDB_RESULT_KIND_QUERY) {
        if (!r->mysql_res) return SEEKDB_INTERNAL_ERROR; /* no rows */
        r->current_row     = mysql_fetch_row(r->mysql_res);
        r->current_lengths = mysql_fetch_lengths(r->mysql_res);
        return r->current_row ? SEEKDB_SUCCESS : SEEKDB_INTERNAL_ERROR;
    } else {
        int rc = mysql_stmt_fetch(r->stmt_ref);
        if (rc == 0)            return SEEKDB_SUCCESS;
        if (rc == MYSQL_NO_DATA) return SEEKDB_INTERNAL_ERROR; /* TODO: dedicated end-of-rows code */
        return SEEKDB_INTERNAL_ERROR;
    }
}

static int read_cell_str(SeekdbResultImpl *r, int64_t index,
                         const char **out_data, size_t *out_len, int *out_is_null)
{
    if (index < 0 || index >= r->column_count) return SEEKDB_INVALID_ARGUMENT;

    if (r->kind == SEEKDB_RESULT_KIND_QUERY) {
        if (!r->current_row) return SEEKDB_INTERNAL_ERROR;
        const char *cell = r->current_row[index];
        *out_is_null = (cell == NULL);
        *out_data = cell;
        *out_len  = cell ? r->current_lengths[index] : 0;
        return SEEKDB_SUCCESS;
    } else {
        *out_is_null = r->result_is_null[index];
        *out_data    = r->result_str_buffers[index];
        *out_len     = r->result_str_lens[index];
        return SEEKDB_SUCCESS;
    }
}

int seekdb_result_get_int64(SeekdbResult result, int64_t index, int64_t *out_value)
{
    if (!result || !out_value) return SEEKDB_INVALID_ARGUMENT;
    const char *data; size_t len; int is_null;
    int rc = read_cell_str(result, index, &data, &len, &is_null);
    if (rc != SEEKDB_SUCCESS) return rc;
    if (is_null) { *out_value = 0; return SEEKDB_SUCCESS; }

    char buf[32];
    if (len >= sizeof(buf)) return SEEKDB_INTERNAL_ERROR;
    memcpy(buf, data, len);
    buf[len] = '\0';
    errno = 0;
    char *endp = NULL;
    long long v = strtoll(buf, &endp, 10);
    if (errno || endp == buf) return SEEKDB_INTERNAL_ERROR;
    *out_value = (int64_t)v;
    return SEEKDB_SUCCESS;
}

int seekdb_result_get_float(SeekdbResult result, int64_t index, double *out_value)
{
    if (!result || !out_value) return SEEKDB_INVALID_ARGUMENT;
    const char *data; size_t len; int is_null;
    int rc = read_cell_str(result, index, &data, &len, &is_null);
    if (rc != SEEKDB_SUCCESS) return rc;
    if (is_null) { *out_value = 0.0; return SEEKDB_SUCCESS; }

    char buf[64];
    if (len >= sizeof(buf)) return SEEKDB_INTERNAL_ERROR;
    memcpy(buf, data, len);
    buf[len] = '\0';
    errno = 0;
    char *endp = NULL;
    double v = strtod(buf, &endp);
    if (errno || endp == buf) return SEEKDB_INTERNAL_ERROR;
    *out_value = v;
    return SEEKDB_SUCCESS;
}

int seekdb_result_get_value(SeekdbResult result, int64_t index, SeekdbValue *out_value)
{
    if (!result || !out_value) return SEEKDB_INVALID_ARGUMENT;
    SeekdbResultImpl *r = result;
    if (index < 0 || index >= r->column_count) return SEEKDB_INVALID_ARGUMENT;

    SeekdbTypeId tid;
    if (r->kind == SEEKDB_RESULT_KIND_QUERY) {
        MYSQL_FIELD *f = mysql_fetch_field_direct(r->mysql_res, (unsigned int)index);
        if (!f) return SEEKDB_INTERNAL_ERROR;
        tid = map_field_type(f->type);
    } else {
        tid = map_field_type(r->result_field_types[index]);
    }

    SeekdbValueImpl *v = calloc(1, sizeof(*v));
    if (!v) return SEEKDB_INTERNAL_ERROR;
    v->type = tid;

    int rc;
    if (tid == SEEKDB_TYPE_INT64) {
        rc = seekdb_result_get_int64(result, index, &v->v.i64);
    } else if (tid == SEEKDB_TYPE_FLOAT) {
        rc = seekdb_result_get_float(result, index, &v->v.f64);
    } else {
        const char *data; size_t len; int is_null;
        rc = read_cell_str(r, index, &data, &len, &is_null);
        if (rc == SEEKDB_SUCCESS && !is_null) {
            v->v.str.data = malloc(len + 1);
            if (!v->v.str.data) { free(v); return SEEKDB_INTERNAL_ERROR; }
            memcpy(v->v.str.data, data, len);
            v->v.str.data[len] = '\0';
            v->v.str.len = len;
        }
    }
    if (rc != SEEKDB_SUCCESS) { free(v); return rc; }

    *out_value = (SeekdbValue)v;
    return SEEKDB_SUCCESS;
}

/* ============================================================ value ==== */

int seekdb_value_free(SeekdbValue value)
{
    if (!value) return SEEKDB_INVALID_ARGUMENT;
    SeekdbValueImpl *v = value;
    if (v->type == SEEKDB_TYPE_VARCHAR ||
        v->type == SEEKDB_TYPE_DECIMAL ||
        v->type == SEEKDB_TYPE_DATE    ||
        v->type == SEEKDB_TYPE_DATETIME||
        v->type == SEEKDB_TYPE_TIMESTAMP) {
        xfree(v->v.str.data);
    }
    free(v);
    return SEEKDB_SUCCESS;
}

int seekdb_value_create_int64(int64_t int_value, SeekdbValue *out_value)
{
    if (!out_value) return SEEKDB_INVALID_ARGUMENT;
    SeekdbValueImpl *v = calloc(1, sizeof(*v));
    if (!v) return SEEKDB_INTERNAL_ERROR;
    v->type   = SEEKDB_TYPE_INT64;
    v->v.i64  = int_value;
    *out_value = (SeekdbValue)v;
    return SEEKDB_SUCCESS;
}

int seekdb_value_get_int64(SeekdbValue value, int64_t *out_value)
{
    if (!value || !out_value) return SEEKDB_INVALID_ARGUMENT;
    SeekdbValueImpl *v = value;
    if (v->type != SEEKDB_TYPE_INT64) return SEEKDB_INVALID_ARGUMENT;
    *out_value = v->v.i64;
    return SEEKDB_SUCCESS;
}
