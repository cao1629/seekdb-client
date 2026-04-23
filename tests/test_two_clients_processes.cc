// Same two scenarios as test_two_clients_threads, but each client runs in its
// own forked process instead of a thread. This exercises the real multi-process
// topology the client library is designed for.
//
//   BArrivesDuringAStartup -- B arrives while A still holds seekdb.startup.
//     Both clients race on that lock; A wins and spawns S1, B then spawns
//     S2 which loses the seekdb.pid race and _exit(0)s. Survivor is A's S1.
//
//   BArrivesAfterAStartup -- B arrives after A's seekdb_open returns.
//     A's server is already up, so B's try_connect succeeds immediately;
//     B never reaches seekdb.startup or posix_spawn at all.
//
// Parent/child handshake per client:
//   ready pipe: child writes one byte after open+connect+query succeeds;
//               parent blocks reading it to know the open phase finished.
//   close pipe: parent close()s its write end when it wants the child to
//               close its handle and exit; child's blocking read returns 0,
//               unblocking it.
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

// ---------------------------------------------------------------------------
// Case 1: B arrives while A still holds flock(EX) seekdb.startup.
// ---------------------------------------------------------------------------
TEST_F(TwoClientsProcesses, BArrivesDuringAStartup)
{
    Client a = fork_client(bin_path_, db_dir_);

    // Wait for A to actually take flock(EX) seekdb.startup. Only then
    // is it guaranteed B will race on the same lock.
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    const std::string startup_path = db_dir_ + "/run/seekdb.startup";
    while (!someone_holds_flock(startup_path, "WRITE")) {
        ASSERT_LT(std::chrono::steady_clock::now(), deadline)
            << "A never took flock(EX) seekdb.startup";
        std::this_thread::sleep_for(1ms);
    }

    // Capture A's server pid before B exists.
    pid_t a_server_pid = -1;
    const auto pid_deadline = std::chrono::steady_clock::now() + 10s;
    while ((a_server_pid = read_pid(db_dir_)) <= 0) {
        ASSERT_LT(std::chrono::steady_clock::now(), pid_deadline)
            << "A's server never wrote run/seekdb.pid";
        std::this_thread::sleep_for(1ms);
    }
    ASSERT_TRUE(alive(a_server_pid));

    Client b = fork_client(bin_path_, db_dir_);

    char buf;
    ASSERT_EQ(::read(a.parent_wait, &buf, 1), 1) << "A child died before open";
    ASSERT_EQ(::read(b.parent_wait, &buf, 1), 1) << "B child died before open";

    // Headline invariant: survivor is exactly the process A spawned.
    EXPECT_EQ(read_pid(db_dir_), a_server_pid)
        << "seekdb.pid changed after B's open -- B's spawn was not supposed to win";
    EXPECT_TRUE(alive(a_server_pid));

    ::close(a.child_wait);
    ::close(b.child_wait);

    int status;
    ASSERT_EQ(::waitpid(a.pid, &status, 0), a.pid);
    EXPECT_TRUE(WIFEXITED(status) && WEXITSTATUS(status) == 0)
        << "A child exit status: " << (WIFEXITED(status) ? WEXITSTATUS(status) : -1);
    ASSERT_EQ(::waitpid(b.pid, &status, 0), b.pid);
    EXPECT_TRUE(WIFEXITED(status) && WEXITSTATUS(status) == 0)
        << "B child exit status: " << (WIFEXITED(status) ? WEXITSTATUS(status) : -1);

    ::close(a.parent_wait);
    ::close(b.parent_wait);

    EXPECT_TRUE(wait_until_gone(a_server_pid, 15s))
        << "server " << a_server_pid << " still alive 15s after both clients closed";
}

// ---------------------------------------------------------------------------
// Case 2: B arrives after A's seekdb_open has fully returned.
// ---------------------------------------------------------------------------
TEST_F(TwoClientsProcesses, BArrivesAfterAStartup)
{
    Client a = fork_client(bin_path_, db_dir_);

    char buf;
    ASSERT_EQ(::read(a.parent_wait, &buf, 1), 1) << "A child died before open";

    const pid_t server_pid = read_pid(db_dir_);
    ASSERT_GT(server_pid, 0);
    ASSERT_TRUE(alive(server_pid));

    Client b = fork_client(bin_path_, db_dir_);
    ASSERT_EQ(::read(b.parent_wait, &buf, 1), 1) << "B child died before open";

    // Headline invariants: B didn't spawn anything.
    EXPECT_EQ(read_pid(db_dir_), server_pid)
        << "seekdb.pid changed -- B unexpectedly spawned";
    EXPECT_TRUE(alive(server_pid));

    EXPECT_TRUE(fs::exists(db_dir_ + "/run/sql.sock"));

    ::close(a.child_wait);
    ::close(b.child_wait);

    int status;
    ASSERT_EQ(::waitpid(a.pid, &status, 0), a.pid);
    EXPECT_TRUE(WIFEXITED(status) && WEXITSTATUS(status) == 0)
        << "A child exit status: " << (WIFEXITED(status) ? WEXITSTATUS(status) : -1);
    ASSERT_EQ(::waitpid(b.pid, &status, 0), b.pid);
    EXPECT_TRUE(WIFEXITED(status) && WEXITSTATUS(status) == 0)
        << "B child exit status: " << (WIFEXITED(status) ? WEXITSTATUS(status) : -1);

    ::close(a.parent_wait);
    ::close(b.parent_wait);

    EXPECT_TRUE(wait_until_gone(server_pid, 15s))
        << "server " << server_pid << " still alive 15s after both clients closed";
}

}  // namespace
