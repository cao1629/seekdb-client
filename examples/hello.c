/*
 * Smoke test for seekdb_client.
 *
 * Assumes a SeekDB (or MariaDB / MySQL) server is running on 127.0.0.1
 * at the port passed on the command line, with user 'root' and no password.
 *
 * Usage: ./seekdb_hello [port]   (default port 3306)
 */

#include "seekdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(rc, what) do {                                          \
    if ((rc) != SEEKDB_SUCCESS) {                                     \
        fprintf(stderr, "%s failed: rc=%d\n", (what), (rc));          \
        return 1;                                                     \
    }                                                                 \
} while (0)

int main(int argc, char **argv)
{
    int port = (argc >= 2) ? atoi(argv[1]) : 3306;

    SeekdbHandle h = NULL;
    CHECK(seekdb_open("/tmp/seekdb-data", port, &h), "seekdb_open");

    SeekdbConnection c = NULL;
    CHECK(seekdb_connect(h, NULL /* default db */, true, &c), "seekdb_connect");

    /* 1) Plain query path. */
    SeekdbResult r = NULL;
    const char *sql = "SELECT 1+2 AS sum, 'hello' AS greeting";
    CHECK(seekdb_query(c, sql, (int64_t)strlen(sql), &r), "seekdb_query");

    int64_t ncols = 0;
    seekdb_result_column_count(r, &ncols);
    printf("columns: %lld\n", (long long)ncols);
    for (int64_t i = 0; i < ncols; ++i) {
        const char *name = NULL;
        SeekdbTypeId tid;
        seekdb_result_column_name(r, i, &name);
        seekdb_result_column_type_id(r, i, &tid);
        printf("  col[%lld] = %s (typeid=%d)\n", (long long)i, name, tid);
    }

    while (seekdb_result_next(r) == SEEKDB_SUCCESS) {
        int64_t v = 0;
        seekdb_result_get_int64(r, 0, &v);
        SeekdbValue gv = NULL;
        seekdb_result_get_value(r, 1, &gv);
        printf("row: sum=%lld\n", (long long)v);
        if (gv) seekdb_value_free(gv);
    }
    seekdb_result_free(r);

    /* 2) Prepared statement path with int64 parameter. */
    SeekdbStmt s = NULL;
    CHECK(seekdb_stmt_prepare(c, "SELECT ? * 10", &s), "seekdb_stmt_prepare");
    printf("stmt param count: %d\n", seekdb_stmt_param_count(s));
    CHECK(seekdb_stmt_bind_int64(s, 0, 7), "seekdb_stmt_bind_int64");

    SeekdbResult r2 = NULL;
    CHECK(seekdb_stmt_execute(s, &r2), "seekdb_stmt_execute");
    while (seekdb_result_next(r2) == SEEKDB_SUCCESS) {
        int64_t v = 0;
        seekdb_result_get_int64(r2, 0, &v);
        printf("stmt row: %lld\n", (long long)v);
    }
    seekdb_result_free(r2);
    seekdb_stmt_free(s);

    /* 3) Transaction trio. */
    CHECK(seekdb_trx_begin(c),    "seekdb_trx_begin");
    CHECK(seekdb_trx_rollback(c), "seekdb_trx_rollback");

    seekdb_disconnect(c);
    seekdb_close(h);

    puts("OK");
    return 0;
}
