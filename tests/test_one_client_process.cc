// One client, in its own forked process: verify that seekdb_open spawns a
// live server whose pid is recorded in run/seekdb.pid, that a query succeeds,
// and that once the client closes its handle the server shuts itself down
// (it's the last client, so seekdb.clients goes fully free).
//
// Env:
//   SEEKDB_BIN   path to the seekdb binary

#include <gtest/gtest.h>

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

TEST_F(OneClientProcess, ServerShutsDownAfterClose)
{
    Client c = fork_client(bin_path_, db_dir_);

    // Block until child reports open + query succeeded.
    char buf;
    ASSERT_EQ(::read(c.ready_read, &buf, 1), 1) << "client died before open";

    // Server should be alive, and its pid recorded in run/seekdb.pid.
    const pid_t server_pid = read_pid(db_dir_);
    ASSERT_GT(server_pid, 0) << "server did not record its pid";
    EXPECT_TRUE(alive(server_pid));

    // Tell the child to close its handle and exit.
    ::close(c.close_write);

    int status;
    ASSERT_EQ(::waitpid(c.pid, &status, 0), c.pid);
    EXPECT_TRUE(WIFEXITED(status) && WEXITSTATUS(status) == 0)
        << "client exit status: " << (WIFEXITED(status) ? WEXITSTATUS(status) : -1);
    ::close(c.ready_read);

    // With the last client gone, the server should notice and exit.
    EXPECT_TRUE(wait_until_gone(server_pid, 15s))
        << "server " << server_pid << " still alive 15s after client closed";
}

}  // namespace
