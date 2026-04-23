// KillTwoClientProcessesOneByOne -- fork two client processes that each call
// seekdb_open (B takes the "server already up" fast path). SIGKILL A and
// assert the server stays up because B still holds its SH on seekdb.clients;
// then SIGKILL B and assert the server shuts itself down.
//
// Env:
//   SEEKDB_BIN   path to the seekdb binary

#include <gtest/gtest.h>

#include "seekdb.h"
#include "test_utils.h"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <signal.h>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

class TwoClientsProcesses : public ::testing::Test {
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

TEST_F(TwoClientsProcesses, KillTwoClientProcessesOneByOne)
{
    // Fork A: open, signal ready, wait to be killed.
    int ready_a[2];
    ASSERT_EQ(::pipe(ready_a), 0);
    pid_t a_pid = ::fork();
    ASSERT_GE(a_pid, 0);
    if (a_pid == 0) {
        ::close(ready_a[0]);
        SeekdbHandle h = nullptr;
        if (seekdb_open(bin_path_.c_str(), db_dir_.c_str(), 0, &h)
            != SEEKDB_SUCCESS) _exit(10);
        char byte = 'Y';
        if (::write(ready_a[1], &byte, 1) != 1) _exit(13);
        ::close(ready_a[1]);
        for (;;) ::pause();
    }
    ::close(ready_a[1]);

    char buf;
    ASSERT_EQ(::read(ready_a[0], &buf, 1), 1) << "A child died before open";

    const pid_t server_pid = read_pid(db_dir_);
    ASSERT_GT(server_pid, 0);
    ASSERT_TRUE(alive(server_pid));

    // Fork B with three single-purpose pipes:
    //   open_done   B→parent: B's seekdb_open succeeded
    //   run_query   parent→B: parent wants B to run SELECT 1 now
    //   query_done  B→parent: B's SELECT 1 succeeded
    // Because A's server is already up, B's seekdb_open takes the fast path
    // (try_connect succeeds) and never spawns a server.
    int open_done[2];
    int run_query[2];
    int query_done[2];
    ASSERT_EQ(::pipe(open_done),  0);
    ASSERT_EQ(::pipe(run_query),  0);
    ASSERT_EQ(::pipe(query_done), 0);
    pid_t b_pid = ::fork();
    ASSERT_GE(b_pid, 0);
    if (b_pid == 0) {
        ::close(open_done[0]);
        ::close(run_query[1]);
        ::close(query_done[0]);

        char byte = 'Y';
        SeekdbHandle h = nullptr;
        if (seekdb_open(bin_path_.c_str(), db_dir_.c_str(), 0, &h)
            != SEEKDB_SUCCESS) _exit(10);
        if (::write(open_done[1], &byte, 1) != 1) _exit(13);

        // Wait for parent's go signal, then connect+query.
        char in;
        if (::read(run_query[0], &in, 1) != 1) _exit(20);
        SeekdbConnection c = nullptr;
        if (seekdb_connect(h, nullptr, true, &c) != SEEKDB_SUCCESS) _exit(21);
        SeekdbResult r = nullptr;
        if (seekdb_query(c, "SELECT 1", 8, &r) != SEEKDB_SUCCESS) _exit(22);
        if (r) seekdb_result_free(r);
        seekdb_disconnect(c);

        if (::write(query_done[1], &byte, 1) != 1) _exit(23);

        ::close(open_done[1]);
        ::close(run_query[0]);
        ::close(query_done[1]);
        for (;;) ::pause();
    }
    ::close(open_done[1]);
    ::close(run_query[0]);
    ::close(query_done[1]);

    ASSERT_EQ(::read(open_done[0], &buf, 1), 1) << "B child died before open";

    // B must not have spawned a second server.
    EXPECT_EQ(read_pid(db_dir_), server_pid)
        << "seekdb.pid changed -- B unexpectedly spawned";
    EXPECT_TRUE(alive(server_pid));

    // Kill A; B still holds its SH on seekdb.clients, so the server should
    // keep running.
    ASSERT_EQ(::kill(a_pid, SIGKILL), 0);
    ASSERT_EQ(::waitpid(a_pid, NULL, 0), a_pid);
    ::close(ready_a[0]);

    std::this_thread::sleep_for(6s);

    // Tell B to run SELECT 1. B writes a byte on query_done once the query
    // succeeds; if anything in connect/query fails, B _exit()s and the read
    // below returns 0 (EOF).
    char byte = 'Y';
    ASSERT_EQ(::write(run_query[1], &byte, 1), 1);
    ::close(run_query[1]);
    ASSERT_EQ(::read(query_done[0], &buf, 1), 1)
        << "B's query failed after A was killed";

    // Kill B. Now no client holds the SH; server should shut down.
    ASSERT_EQ(::kill(b_pid, SIGKILL), 0);
    ASSERT_EQ(::waitpid(b_pid, NULL, 0), b_pid);
    ::close(open_done[0]);
    ::close(query_done[0]);

    EXPECT_TRUE(wait_until_gone(server_pid, 15s))
        << "server " << server_pid << " still alive 15s after last client was killed";
}

}  // namespace
