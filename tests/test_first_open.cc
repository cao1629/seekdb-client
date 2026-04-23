// First seekdb_open call -- base case.
//
// No server is running when the client calls seekdb_open. The library
// takes SH on seekdb.clients, takes EX on seekdb.startup, posix_spawns
// the server, waits for it to come up, connects, runs a query, and on
// close the server auto-shuts-down within one poll tick.
//
// Env:
//   SEEKDB_BIN   path to the seekdb binary

#include <gtest/gtest.h>

#include "seekdb.h"

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <signal.h>
#include <string>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

class FirstOpenTest : public ::testing::Test {
protected:
    std::string bin_path_;
    std::string db_dir_;

    void SetUp() override {
        const char *bin = std::getenv("SEEKDB_BIN");
        ASSERT_NE(bin, nullptr) << "set SEEKDB_BIN to the seekdb binary";
        bin_path_ = bin;
        ASSERT_TRUE(fs::exists(bin_path_)) << "no such file: " << bin_path_;

        db_dir_ = "/tmp/seekdb_test_db";
        fs::remove_all(db_dir_);
        fs::create_directories(db_dir_);

        ASSERT_FALSE(fs::exists(db_dir_ + "/run/seekdb.pid"));
    }

    void TearDown() override {
        pid_t pid = read_pid();
        if (pid > 0 && alive(pid)) {
            wait_until_gone(pid, 5s);
            if (alive(pid)) ::kill(pid, SIGKILL);
        }
        fs::remove_all(db_dir_);
    }

    pid_t read_pid() const {
        std::FILE *f = std::fopen((db_dir_ + "/run/seekdb.pid").c_str(), "r");
        if (!f) return -1;
        long pid = 0;
        int got = std::fscanf(f, "%ld", &pid);
        std::fclose(f);
        return got == 1 ? (pid_t)pid : -1;
    }

    static bool alive(pid_t pid) {
        return pid > 0 && ::kill(pid, 0) == 0;
    }

    static bool wait_until_gone(pid_t pid, std::chrono::milliseconds timeout) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (alive(pid) && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(200ms);
        }
        return !alive(pid);
    }

    bool ex_lock_available(const std::string &path) const {
        int fd = ::open(path.c_str(), O_RDWR);
        if (fd < 0) return false;
        const bool ok = ::flock(fd, LOCK_EX | LOCK_NB) == 0;
        ::close(fd);
        return ok;
    }
};

TEST_F(FirstOpenTest, FreshSpawnSucceedsAndServerAutoShutsDown)
{
    // Act: open. The library should spawn a server and connect.
    SeekdbHandle h = nullptr;
    ASSERT_EQ(seekdb_open(bin_path_.c_str(), db_dir_.c_str(), 0, &h),
              SEEKDB_SUCCESS);
    ASSERT_NE(h, nullptr);

    // Post-open invariants from the sequence diagram.
    const pid_t server_pid = read_pid();
    ASSERT_GT(server_pid, 0)        << "seekdb.pid missing or unreadable";
    ASSERT_TRUE(alive(server_pid))  << "server pid " << server_pid << " not alive";
    EXPECT_TRUE(fs::exists(db_dir_ + "/run/sql.sock"));
    EXPECT_TRUE(fs::exists(db_dir_ + "/run/seekdb.clients"));

    // Client holds SH on seekdb.clients -> external EX must fail.
    EXPECT_FALSE(ex_lock_available(db_dir_ + "/run/seekdb.clients"))
        << "seekdb.clients EX was acquirable while client is open";

    // Functional smoke.
    SeekdbConnection c = nullptr;
    ASSERT_EQ(seekdb_connect(h, nullptr, true, &c), SEEKDB_SUCCESS);
    SeekdbResult r = nullptr;
    ASSERT_EQ(seekdb_query(c, "SELECT 1", 8, &r), SEEKDB_SUCCESS);
    if (r) seekdb_result_free(r);

    seekdb_disconnect(c);
    seekdb_close(h);

    // Auto-shutdown: one poll tick (~5s) + generous margin.
    EXPECT_TRUE(wait_until_gone(server_pid, 15s))
        << "server " << server_pid
        << " still alive 15s after seekdb_close";
}

}  // namespace
