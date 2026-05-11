// pybind11 bindings for seekdb-client.
//
// Surface intentionally mirrors seekdb's `ob_embed_impl.cpp` so user code
// looks the same:
//
//     import seekdb_pyclient as sc
//     sc.open(db_dir="./seekdb.db")
//     conn = sc.connect(database="test", autocommit=False)
//     cur = conn.cursor()
//     cur.execute("SELECT 1")
//     print(cur.fetchall())
//
// Difference from ob_embed_impl.cpp: that one binds the in-process
// ObLiteEmbed/ObLiteEmbedConn/ObLiteEmbedCursor classes; this one binds the
// out-of-process seekdb-client C API (libseekdb_client.so).

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <cstdlib>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

extern "C" {
#include "seekdb.h"
}

namespace py = pybind11;

// ---------- error translation ----------

class SeekdbError : public std::runtime_error {
public:
    SeekdbError(int code, const char *where)
        : std::runtime_error(std::string(where) + " failed: code=" + std::to_string(code)),
          code_(code) {}
    int code() const { return code_; }
private:
    int code_;
};

#define SDB_CHECK(expr)                                              \
    do {                                                             \
        int _rc = (expr);                                            \
        if (_rc != SEEKDB_SUCCESS) throw SeekdbError(_rc, #expr);    \
    } while (0)

// ============================================================
// namespace seekdb — mirrors oceanbase::embed in ob_embed_impl.cpp
// ============================================================
namespace seekdb {

class Cursor;  // forward decl — Connection::cursor() returns one

// ---------- Connection (mirrors ObLiteEmbedConn) ----------

class Connection : public std::enable_shared_from_this<Connection> {
public:
    Connection() : c_(nullptr) {}
    explicit Connection(SeekdbConnection c) : c_(c) {}
    ~Connection() { reset(); }
    Connection(const Connection &) = delete;
    Connection &operator=(const Connection &) = delete;

    Cursor cursor();   // out-of-line; needs Cursor's full type

    void reset() {
        if (c_) { seekdb_disconnect(c_); c_ = nullptr; }
    }
    void begin()    { SDB_CHECK(seekdb_trx_begin(c_)); }
    void commit()   { SDB_CHECK(seekdb_trx_commit(c_)); }
    void rollback() { SDB_CHECK(seekdb_trx_rollback(c_)); }

    SeekdbConnection raw() const { return c_; }
private:
    SeekdbConnection c_;
};

// ---------- Cursor (mirrors ObLiteEmbedCursor) ----------

class Cursor {
public:
    Cursor() : result_(nullptr) {}
    explicit Cursor(std::shared_ptr<Connection> conn)
        : conn_(std::move(conn)), result_(nullptr) {}
    ~Cursor() { close(); }
    Cursor(const Cursor &) = delete;
    Cursor &operator=(const Cursor &) = delete;
    Cursor(Cursor &&o) noexcept : conn_(std::move(o.conn_)), result_(o.result_) {
        o.result_ = nullptr;
    }

    uint64_t execute(const std::string &sql) {
        if (!conn_ || !conn_->raw()) {
            throw std::runtime_error("Cursor.execute: no connection");
        }
        free_result();
        SDB_CHECK(seekdb_query(conn_->raw(), sql.c_str(),
                               static_cast<int64_t>(sql.size()), &result_));
        int64_t n = 0;
        if (seekdb_result_row_count(result_, &n) != SEEKDB_SUCCESS) n = 0;
        return static_cast<uint64_t>(n);
    }

    py::object fetchone() {
        if (!result_) return py::none();
        if (seekdb_result_next(result_) != SEEKDB_SUCCESS) return py::none();
        return build_row();
    }

    std::vector<py::tuple> fetchall() {
        std::vector<py::tuple> rows;
        if (!result_) return rows;
        while (seekdb_result_next(result_) == SEEKDB_SUCCESS) {
            rows.push_back(build_row());
        }
        return rows;
    }

    void close() { free_result(); }

private:
    void free_result() {
        if (result_) { seekdb_result_free(result_); result_ = nullptr; }
    }

    py::tuple build_row() {
        int64_t ncol = 0;
        SDB_CHECK(seekdb_result_column_count(result_, &ncol));
        py::tuple t(ncol);
        for (int64_t i = 0; i < ncol; ++i) {
            t[i] = get_value(i);
        }
        return t;
    }

    py::object get_value(int64_t idx) {
        SeekdbTypeId t = SEEKDB_TYPE_NULL;
        SDB_CHECK(seekdb_result_column_type_id(result_, idx, &t));
        switch (t) {
            case SEEKDB_TYPE_INT64: {
                int64_t v = 0;
                SDB_CHECK(seekdb_result_get_int64(result_, idx, &v));
                return py::int_(v);
            }
            case SEEKDB_TYPE_FLOAT: {
                double v = 0.0;
                SDB_CHECK(seekdb_result_get_float(result_, idx, &v));
                return py::float_(v);
            }
            default:
                throw std::runtime_error(
                    "Cursor.get_value: column type not yet supported (id=" +
                    std::to_string(static_cast<int>(t)) + ")");
        }
    }

    std::shared_ptr<Connection> conn_;  // keeps connection alive while cursor uses it
    SeekdbResult result_;
};

Cursor Connection::cursor() {
    return Cursor(shared_from_this());
}

// ---------- module singleton (mirrors ObLiteEmbed::open/connect/close) ----------

static SeekdbHandle handle = nullptr;

static const char *resolve_bin_path() {
    // ob_embed has the engine in-process so its open() takes only db_dir.
    // seekdb-client spawns the server, so it needs a server binary path.
    // We pull it from SEEKDB_BIN to keep the Python open() signature matching.
    const char *bin = std::getenv("SEEKDB_BIN");
    if (!bin || !*bin) {
        throw std::runtime_error(
            "seekdb-client requires SEEKDB_BIN env var (path to seekdb server binary)");
    }
    return bin;
}

void open(const std::string &db_dir) {
    if (handle) return;
    SDB_CHECK(seekdb_open(resolve_bin_path(), db_dir.c_str(), 0, &handle));
}

void open_with_service(const std::string &db_dir, int port) {
    if (handle) return;
    SDB_CHECK(seekdb_open(resolve_bin_path(), db_dir.c_str(), port, &handle));
}

std::shared_ptr<Connection> connect(const std::string &database, bool autocommit) {
    if (!handle) {
        throw std::runtime_error(
            "seekdb not opened — call open() or open_with_service() first");
    }
    SeekdbConnection c = nullptr;
    SDB_CHECK(seekdb_connect(handle, database.c_str(), autocommit, &c));
    return std::make_shared<Connection>(c);
}

void close() {
    if (handle) {
        seekdb_close(handle);
        handle = nullptr;
    }
}

} // namespace seekdb

// ---------- module ----------

PYBIND11_MODULE(seekdb_pyclient, m) {
    m.doc() = "Python bindings for seekdb-client (out-of-process MySQL-compatible client). "
              "Surface mirrors seekdb's ob_embed_impl.cpp.";
    m.attr("__version__") = "0.1.0";

    py::register_exception<SeekdbError>(m, "SeekdbError");

    const char *default_service_path = "./seekdb.db";

    m.def("open", &seekdb::open,
          py::arg("db_dir") = default_service_path,
          "open db");

    m.def("open_with_service", &seekdb::open_with_service,
          py::arg("db_dir") = default_service_path,
          py::arg("port") = 2881,
          "open db");

    m.def("connect", &seekdb::connect,
          py::arg("database") = "test",
          py::arg("autocommit") = false,
          "connect seekdb");

    py::class_<seekdb::Connection, std::shared_ptr<seekdb::Connection>>(m, "Connection")
        .def(py::init<>())
        .def("cursor",   &seekdb::Connection::cursor)
        .def("close",    &seekdb::Connection::reset)
        .def("begin",    &seekdb::Connection::begin,    py::call_guard<py::gil_scoped_release>())
        .def("commit",   &seekdb::Connection::commit,   py::call_guard<py::gil_scoped_release>())
        .def("rollback", &seekdb::Connection::rollback, py::call_guard<py::gil_scoped_release>());

    py::class_<seekdb::Cursor>(m, "Cursor")
        .def("execute",  &seekdb::Cursor::execute,  py::call_guard<py::gil_scoped_release>())
        .def("fetchone", &seekdb::Cursor::fetchone)
        .def("fetchall", &seekdb::Cursor::fetchall)
        .def("close",    &seekdb::Cursor::close);

    py::object atexit = py::module::import("atexit");
    atexit.attr("register")(py::cpp_function(&seekdb::close));
}
