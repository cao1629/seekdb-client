// One client, two ways it can go away:
//   ServerShutdownAfterClientClose — client calls seekdb_close cleanly.
//     The test process itself is the client.
//   ServerShutdownAfterClientExit  — client process is killed before close.
//     A forked child runs as the client; parent SIGKILLs it.
// In both cases the last-client-gone detection on seekdb.clients should
// make the server shut itself down.
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
#include <unistd.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

class OneClientProcess : public ::testing::Test {
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

TEST_F(OneClientProcess, ServerShutdownAfterClientClose)
{
    SeekdbHandle h = nullptr;
    ASSERT_EQ(seekdb_open(bin_path_.c_str(), db_dir_.c_str(), 0, &h),
              SEEKDB_SUCCESS);

    const pid_t server_pid = read_pid(db_dir_);
    EXPECT_TRUE(alive(server_pid));

    SeekdbConnection c = nullptr;
    ASSERT_EQ(seekdb_connect(h, nullptr, true, &c), SEEKDB_SUCCESS);
    SeekdbResult r = nullptr;
    EXPECT_EQ(seekdb_query(c, "SELECT 1", 8, &r), SEEKDB_SUCCESS);
    if (r) seekdb_result_free(r);

    seekdb_disconnect(c);
    seekdb_close(h);

    EXPECT_TRUE(wait_until_gone(server_pid, 15s))
        << "server " << server_pid << " still alive 15s after client closed";
}

TEST_F(OneClientProcess, ServerShutdownAfterClientExit)
{
    Client c = fork_client(bin_path_, db_dir_);

    // Wait for child to report open+query succeeded.
    char buf;
    ASSERT_EQ(::read(c.parent_wait, &buf, 1), 1) << "client died before open";

    const pid_t server_pid = read_pid(db_dir_);
    EXPECT_TRUE(alive(server_pid));

    // Kill the client before it can reach seekdb_close. The SH lock on
    // seekdb.clients is OFD-scoped, so it's released when the process dies.
    ASSERT_EQ(::kill(c.pid, SIGKILL), 0);

    ASSERT_EQ(::waitpid(c.pid, NULL, 0), c.pid);
    ::close(c.parent_wait);
    ::close(c.child_wait);

    EXPECT_TRUE(wait_until_gone(server_pid, 15s))
        << "server " << server_pid << " still alive 15s after client was killed";
}

}  // namespace
