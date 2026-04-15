# seekdb-client

Tiny C client library for [SeekDB](https://github.com/), wrapping libmariadb to talk to a SeekDB server over the MySQL TCP protocol.

## Status

Phase 1 — TCP only, no Unix-socket support, no embedded server. Suitable for connecting to a separately-running SeekDB (or any MySQL-protocol-compatible) instance.

## Build

Requires `libmariadb` (MariaDB Connector/C).

```
# macOS
brew install mariadb-connector-c

# Debian/Ubuntu
sudo apt install libmariadb-dev

# RHEL/Fedora
sudo dnf install mariadb-connector-c-devel
```

Then:

```
mkdir build && cd build
cmake ..                  # auto-discovers libmariadb via pkg-config or std paths
# or:
cmake -DSEEKDB_LIBMARIADB_DIR=/path/to/libmariadb-prefix ..
make
```

## Run the smoke test

The example expects a SeekDB / MariaDB / MySQL server on `127.0.0.1:<port>` with `root` and no password.

```
./build/seekdb_hello 3306
```

## API

See [`include/seekdb.h`](include/seekdb.h). Highlights:

```c
SeekdbHandle h;
seekdb_open("/tmp/seekdb-data", 3306, &h);

SeekdbConnection c;
seekdb_connect(h, "mydb", true, &c);

SeekdbResult r;
const char *sql = "SELECT 1+2";
seekdb_query(c, sql, strlen(sql), &r);

while (seekdb_result_next(r) == SEEKDB_SUCCESS) {
    int64_t v;
    seekdb_result_get_int64(r, 0, &v);
    printf("%lld\n", v);
}
seekdb_result_free(r);

seekdb_disconnect(c);
seekdb_close(h);
```

Supports text-protocol queries, prepared statements with int64 parameter binding, transaction begin/commit/rollback, and basic int64 / float / value accessors on result rows.

## Known limitations

- Hardcoded `127.0.0.1` host (no host parameter yet)
- Hardcoded `root` user, empty password
- Prepared-statement output rows fetched as 1024-byte string buffers (long varchars truncate)
- `seekdb_stmt_bind` only handles `SEEKDB_TYPE_INT64`
- No `SEEKDB_ITERATION_END` distinct from internal errors

## License

TBD.
