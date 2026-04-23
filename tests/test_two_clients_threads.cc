// Two scenarios for a second client arriving while a first is open:
//
//   BArrivesDuringAStartup -- B arrives while A still holds seekdb.startup.
//     Both clients race on that lock; A wins and spawns S1, B then spawns
//     S2 which loses the seekdb.pid race and _exit(0)s. Survivor is A's S1.
//
//   BArrivesAfterAStartup -- B arrives after A's seekdb_open returns.
//     A's server is already up, so B's try_connect succeeds immediately;
//     B never reaches seekdb.startup or posix_spawn at all.
//
// Env:
//   SEEKDB_BIN   path to the seekdb binary

#include <gtest/gtest.h>

#include "seekdb.h"
#include "test_utils.h"

#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>
#include <mutex>
#include <string>
#include <thread>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

class TwoClientsOpen : public ::testing::Test {
protected:
    std::string bin_path_;
    std::string db_dir_;

    void SetUp() override {
        const char *bin = std::getenv("SEEKDB_BIN");
        ASSERT_NE(bin, nullptr) << "set SEEKDB_BIN to the seekdb binary";
        bin_path_ = bin;
        ASSERT_TRUE(fs::exists(bin_path_));

        db_dir_ = "/tmp/seekdb_test_db";
        fs::remove_all(db_dir_);
        fs::create_directories(db_dir_);
    }

    void TearDown() override {
        pid_t pid = read_pid(db_dir_);
        if (pid > 0 && alive(pid)) {
            wait_until_gone(pid, 5s);
            if (alive(pid)) ::kill(pid, SIGKILL);
        }
        fs::remove_all(db_dir_);
    }
};

// ---------------------------------------------------------------------------
// Case 1: two clients call seekdb_open concurrently. Whichever wins the
// seekdb.startup race spawns the server; the other takes the fast path
// (or loses the seekdb.pid race and reconnects). Both seekdb_open calls
// must return SEEKDB_SUCCESS.
// ---------------------------------------------------------------------------
TEST_F(TwoClientsOpen, BArrivesDuringAStartup)
{
    std::mutex m;
    std::condition_variable cv;
    bool a_opened = false, b_opened = false;
    int a_open_rc = -1, b_open_rc = -1;

    auto run_client = [&](int &open_rc, bool &opened_flag) {
        SeekdbHandle h = nullptr;
        open_rc = seekdb_open(bin_path_.c_str(), db_dir_.c_str(), 0, &h);
        { std::lock_guard<std::mutex> lk(m); opened_flag = true; }
        cv.notify_all();
        if (h) seekdb_close(h);
    };

    std::thread ta(run_client, std::ref(a_open_rc), std::ref(a_opened));
    std::thread tb(run_client, std::ref(b_open_rc), std::ref(b_opened));

    {
        std::unique_lock<std::mutex> lk(m);
        cv.wait(lk, [&] { return a_opened && b_opened; });
    }
    ASSERT_EQ(a_open_rc, SEEKDB_SUCCESS) << "client A failed to seekdb_open";
    ASSERT_EQ(b_open_rc, SEEKDB_SUCCESS) << "client B failed to seekdb_open";

    ta.join();
    tb.join();
}

// ---------------------------------------------------------------------------
// Case 2: B arrives after A's seekdb_open has fully returned.
// ---------------------------------------------------------------------------
TEST_F(TwoClientsOpen, BArrivesAfterAStartup)
{
    std::mutex m;
    std::condition_variable cv;
    bool a_opened = false;
    bool b_opened = false;
    bool close_signal = false;

    SeekdbHandle h_a = nullptr, h_b = nullptr;
    SeekdbConnection c_a = nullptr, c_b = nullptr;
    int a_open_rc = -1, a_query_rc = -1;
    int b_open_rc = -1, b_query_rc = -1;

    auto run_client = [&](SeekdbHandle &h, SeekdbConnection &c,
                          int &open_rc, int &query_rc, bool &opened_flag) {
        open_rc = seekdb_open(bin_path_.c_str(), db_dir_.c_str(), 0, &h);
        if (open_rc == SEEKDB_SUCCESS) {
            if (seekdb_connect(h, nullptr, true, &c) == SEEKDB_SUCCESS) {
                SeekdbResult r = nullptr;
                query_rc = seekdb_query(c, "SELECT 1", 8, &r);
                if (r) seekdb_result_free(r);
            }
        }
        {
            std::lock_guard<std::mutex> lk(m);
            opened_flag = true;
        }
        cv.notify_all();
        {
            std::unique_lock<std::mutex> lk(m);
            cv.wait(lk, [&] { return close_signal; });
        }
        if (c) seekdb_disconnect(c);
        if (h) seekdb_close(h);
    };

    std::thread ta(run_client, std::ref(h_a), std::ref(c_a),
                   std::ref(a_open_rc), std::ref(a_query_rc),
                   std::ref(a_opened));

    // Wait until A's seekdb_open has fully returned. Only then is it
    // guaranteed that B will take the "server already up" fast path and
    // skip the spawn logic entirely.
    {
        std::unique_lock<std::mutex> lk(m);
        cv.wait(lk, [&] { return a_opened; });
    }
    ASSERT_EQ(a_open_rc, SEEKDB_SUCCESS);
    ASSERT_EQ(a_query_rc, SEEKDB_SUCCESS);

    const pid_t server_pid = read_pid(db_dir_);
    ASSERT_GT(server_pid, 0);
    ASSERT_TRUE(alive(server_pid));

    std::thread tb(run_client, std::ref(h_b), std::ref(c_b),
                   std::ref(b_open_rc), std::ref(b_query_rc),
                   std::ref(b_opened));

    {
        std::unique_lock<std::mutex> lk(m);
        cv.wait(lk, [&] { return b_opened; });
    }
    ASSERT_EQ(b_open_rc, SEEKDB_SUCCESS);
    ASSERT_EQ(b_query_rc, SEEKDB_SUCCESS);

    // Headline invariants: B didn't spawn anything.
    EXPECT_EQ(read_pid(db_dir_), server_pid)
        << "seekdb.pid changed -- B unexpectedly spawned";
    EXPECT_TRUE(alive(server_pid));

    EXPECT_TRUE(fs::exists(db_dir_ + "/run/sql.sock"));

    {
        std::lock_guard<std::mutex> lk(m);
        close_signal = true;
    }
    cv.notify_all();
    ta.join();
    tb.join();

    EXPECT_TRUE(wait_until_gone(server_pid, 15s))
        << "server " << server_pid << " still alive 15s after both clients closed";
}

// Cross-client visibility: thread A writes a row, thread B reads it back
// through its own connection. Both threads live in the test process; each
// holds its own SeekdbHandle, and the test orchestrates the order of the
// writes and the read via a single mutex + cv.
TEST_F(TwoClientsOpen, ClientBSeesClientAWrite)
{
    std::mutex m;
    std::condition_variable cv;
    bool a_opened = false, b_opened = false;
    bool a_write_go = false, a_write_done = false;
    bool b_query_go = false, b_query_done = false;
    bool close_signal = false;

    int a_open_rc = -1, a_write_rc = -1;
    int b_open_rc = -1, b_query_rc = -1;
    int64_t b_seen_value = 0;

    auto run_a = [&]() {
        SeekdbHandle h = nullptr;
        a_open_rc = seekdb_open(bin_path_.c_str(), db_dir_.c_str(), 0, &h);
        { std::lock_guard<std::mutex> lk(m); a_opened = true; }
        cv.notify_all();
        if (a_open_rc != SEEKDB_SUCCESS) { if (h) seekdb_close(h); return; }

        { std::unique_lock<std::mutex> lk(m); cv.wait(lk, [&] { return a_write_go; }); }

        SeekdbConnection c = nullptr;
        a_write_rc = seekdb_connect(h, nullptr, true, &c);
        if (a_write_rc == SEEKDB_SUCCESS) {
            SeekdbResult r = nullptr;
            a_write_rc = seekdb_query(c, "USE test", 8, &r);
            if (r) { seekdb_result_free(r); r = nullptr; }
            if (a_write_rc == SEEKDB_SUCCESS)
                a_write_rc = seekdb_query(c, "CREATE TABLE t1(v int)", 22, &r);
            if (r) { seekdb_result_free(r); r = nullptr; }
            if (a_write_rc == SEEKDB_SUCCESS)
                a_write_rc = seekdb_query(c, "INSERT INTO t1 VALUES (1)", 25, &r);
            if (r) { seekdb_result_free(r); r = nullptr; }
            seekdb_disconnect(c);
        }

        { std::lock_guard<std::mutex> lk(m); a_write_done = true; }
        cv.notify_all();

        { std::unique_lock<std::mutex> lk(m); cv.wait(lk, [&] { return close_signal; }); }
        seekdb_close(h);
    };

    auto run_b = [&]() {
        SeekdbHandle h = nullptr;
        b_open_rc = seekdb_open(bin_path_.c_str(), db_dir_.c_str(), 0, &h);
        { std::lock_guard<std::mutex> lk(m); b_opened = true; }
        cv.notify_all();
        if (b_open_rc != SEEKDB_SUCCESS) { if (h) seekdb_close(h); return; }

        { std::unique_lock<std::mutex> lk(m); cv.wait(lk, [&] { return b_query_go; }); }

        SeekdbConnection c = nullptr;
        b_query_rc = seekdb_connect(h, nullptr, true, &c);
        if (b_query_rc == SEEKDB_SUCCESS) {
            SeekdbResult r = nullptr;
            b_query_rc = seekdb_query(c, "SELECT * FROM test.t1", 21, &r);
            if (b_query_rc == SEEKDB_SUCCESS) {
                if (seekdb_result_next(r) != SEEKDB_SUCCESS)
                    b_query_rc = SEEKDB_INTERNAL_ERROR;
                else if (seekdb_result_get_int64(r, 0, &b_seen_value) != SEEKDB_SUCCESS)
                    b_query_rc = SEEKDB_INTERNAL_ERROR;
            }
            if (r) seekdb_result_free(r);
            seekdb_disconnect(c);
        }

        { std::lock_guard<std::mutex> lk(m); b_query_done = true; }
        cv.notify_all();

        { std::unique_lock<std::mutex> lk(m); cv.wait(lk, [&] { return close_signal; }); }
        seekdb_close(h);
    };

    std::thread ta(run_a);
    std::thread tb(run_b);

    { std::unique_lock<std::mutex> lk(m); cv.wait(lk, [&] { return a_opened && b_opened; }); }
    ASSERT_EQ(a_open_rc, SEEKDB_SUCCESS);
    ASSERT_EQ(b_open_rc, SEEKDB_SUCCESS);

    { std::lock_guard<std::mutex> lk(m); a_write_go = true; }
    cv.notify_all();
    { std::unique_lock<std::mutex> lk(m); cv.wait(lk, [&] { return a_write_done; }); }
    ASSERT_EQ(a_write_rc, SEEKDB_SUCCESS)
        << "A's USE/CREATE/INSERT sequence failed";

    { std::lock_guard<std::mutex> lk(m); b_query_go = true; }
    cv.notify_all();
    { std::unique_lock<std::mutex> lk(m); cv.wait(lk, [&] { return b_query_done; }); }
    ASSERT_EQ(b_query_rc, SEEKDB_SUCCESS) << "B's SELECT failed";
    EXPECT_EQ(b_seen_value, 1) << "B did not read the row A inserted";

    { std::lock_guard<std::mutex> lk(m); close_signal = true; }
    cv.notify_all();
    ta.join();
    tb.join();
}

}  // namespace
