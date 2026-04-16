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

static int run_sql(SeekdbConnection c, const char *sql)
{
    SeekdbResult r = NULL;
    int rc = seekdb_query(c, sql, (int64_t)strlen(sql), &r);
    if (r) seekdb_result_free(r);
    return rc;
}

int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <seekdb_bin> <data_dir> [port]\n", argv[0]);
        return 1;
    }
    const char *bin_path = argv[1];
    const char *data_dir = argv[2];
    int port = (argc >= 4) ? atoi(argv[3]) : 0;

    SeekdbHandle h = NULL;
    CHECK(seekdb_open(bin_path, data_dir, port, &h), "seekdb_open");

    SeekdbConnection c = NULL;
    CHECK(seekdb_connect(h, "test", true, &c), "seekdb_connect");

    CHECK(run_sql(c, "DROP TABLE IF EXISTS t1"),  "DROP TABLE");
    CHECK(run_sql(c, "CREATE TABLE t1 (v INT)"),  "CREATE TABLE");
    CHECK(run_sql(c, "INSERT INTO t1 VALUES (1)"), "INSERT 1");
    CHECK(run_sql(c, "INSERT INTO t1 VALUES (2)"), "INSERT 2");
    CHECK(run_sql(c, "INSERT INTO t1 VALUES (3)"), "INSERT 3");

    const char *sql = "SELECT * FROM t1";
    SeekdbResult r = NULL;
    CHECK(seekdb_query(c, sql, (int64_t)strlen(sql), &r), "SELECT");

    int64_t ncols = 0;
    seekdb_result_column_count(r, &ncols);

    int64_t nrows = 0;
    seekdb_result_row_count(r, &nrows);
    printf("t1: %lld rows, %lld columns\n", (long long)nrows, (long long)ncols);

    while (seekdb_result_next(r) == SEEKDB_SUCCESS) {
        int64_t v = 0;
        seekdb_result_get_int64(r, 0, &v);
        printf("  v = %lld\n", (long long)v);
    }

    seekdb_result_free(r);
    seekdb_disconnect(c);
    seekdb_close(h);

    puts("OK");
    return 0;
}
