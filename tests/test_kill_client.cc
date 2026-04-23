// Server's no-clients detection + self-shutdown when a client dies abruptly.
//
// A client that is SIGKILL'd has no opportunity to run seekdb_close, so
// flock release on seekdb.clients comes purely from the kernel closing
// the dead process's fds. This test exercises that path end-to-end:
// once all clients are gone (however they went), the server's poll loop
// must acquire flock(EX) seekdb.clients and exit.
//
// This test requires the client to be a separate process -- you cannot
// SIGKILL a thread. It also acts as a regression guard for the O_CLOEXEC
// fix on the client's lock fds: without O_CLOEXEC, the spawned server
// would inherit the client's locked fd, keeping the OFD alive past the
// client's death and preventing auto-shutdown.
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
#include <vector>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

class KillClientTest : public ::testing::Test {
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

    // Fork a child that opens a client, runs SELECT 1, signals "ready" on
    // ready_w, then pause()s forever. Returns the child's pid; parent takes
    // ownership of reading from ready_r.
    pid_t spawn_client_child(int ready_w) {
        pid_t pid = fork();
        if (pid == 0) {
            SeekdbHandle h = nullptr;
            if (seekdb_open(bin_path_.c_str(), db_dir_.c_str(), 0, &h) != SEEKDB_SUCCESS)
                _exit(10);
            SeekdbConnection c = nullptr;
            if (seekdb_connect(h, nullptr, true, &c) != SEEKDB_SUCCESS)
                _exit(11);
            SeekdbResult r = nullptr;
            if (seekdb_query(c, "SELECT 1", 8, &r) != SEEKDB_SUCCESS)
                _exit(12);
            if (r) seekdb_result_free(r);

            // Tell parent we're fully open; then wait to be killed.
            char b = 'r';
            if (write(ready_w, &b, 1) != 1) _exit(13);
            for (;;) pause();
        }
        return pid;
    }
};

TEST_F(KillClientTest, SingleClientSigkillTriggersAutoShutdown)
{
    int ready[2];
    ASSERT_EQ(pipe(ready), 0) << std::strerror(errno);

    pid_t child = spawn_client_child(ready[1]);
    ASSERT_GT(child, 0);
    close(ready[1]);

    // Wait for child to signal "fully open".
    char b;
    ASSERT_EQ(read(ready[0], &b, 1), 1) << "child failed to reach ready state";
    close(ready[0]);

    // Server must be up and owning seekdb.pid.
    const pid_t server_pid = read_pid();
    ASSERT_GT(server_pid, 0);
    ASSERT_TRUE(alive(server_pid));
    EXPECT_FALSE(ex_lock_available(db_dir_ + "/run/seekdb.clients"))
        << "clients SH lock missing while a client is open";

    // Kill the client abruptly.
    ASSERT_EQ(::kill(child, SIGKILL), 0) << std::strerror(errno);

    int status = 0;
    ASSERT_EQ(waitpid(child, &status, 0), child);
    EXPECT_TRUE(WIFSIGNALED(status));
    EXPECT_EQ(WTERMSIG(status), SIGKILL);

    // The server's poll should detect no clients within ~one tick (~5s)
    // and exit. Generous deadline for headroom.
    EXPECT_TRUE(wait_until_gone(server_pid, 15s))
        << "server " << server_pid << " did not exit 15s after SIGKILL'ing its client";

    // After the server exits, seekdb.clients EX must be acquirable.
    EXPECT_TRUE(ex_lock_available(db_dir_ + "/run/seekdb.clients"))
        << "seekdb.clients still locked after server exited";
}

TEST_F(KillClientTest, PartialKillLeavesServerUpAndFullKillShutsItDown)
{
    constexpr int N = 3;

    std::vector<pid_t> kids;
    std::vector<int> ready_fds;
    for (int i = 0; i < N; ++i) {
        int ready[2];
        ASSERT_EQ(pipe(ready), 0) << std::strerror(errno);

        pid_t pid = spawn_client_child(ready[1]);
        ASSERT_GT(pid, 0);
        close(ready[1]);
        kids.push_back(pid);
        ready_fds.push_back(ready[0]);
    }

    // All children reach ready state.
    for (int i = 0; i < N; ++i) {
        char b;
        ASSERT_EQ(read(ready_fds[i], &b, 1), 1)
            << "child " << i << " did not become ready";
        close(ready_fds[i]);
    }

    const pid_t server_pid = read_pid();
    ASSERT_GT(server_pid, 0);
    ASSERT_TRUE(alive(server_pid));

    // Kill all but one; assert the server stays up.
    for (int i = 0; i < N - 1; ++i) {
        ASSERT_EQ(::kill(kids[i], SIGKILL), 0);
        int status = 0;
        ASSERT_EQ(waitpid(kids[i], &status, 0), kids[i]);
        EXPECT_TRUE(WIFSIGNALED(status));
    }

    // Give the server at least two poll ticks to (wrongly) exit.
    std::this_thread::sleep_for(12s);
    ASSERT_TRUE(alive(server_pid))
        << "server exited while a client was still holding seekdb.clients";

    // Kill the last client -- now the server should exit.
    ASSERT_EQ(::kill(kids.back(), SIGKILL), 0);
    int status = 0;
    ASSERT_EQ(waitpid(kids.back(), &status, 0), kids.back());
    EXPECT_TRUE(WIFSIGNALED(status));

    EXPECT_TRUE(wait_until_gone(server_pid, 15s))
        << "server " << server_pid << " did not exit after the last client was killed";

    EXPECT_TRUE(ex_lock_available(db_dir_ + "/run/seekdb.clients"));
}

}  // namespace
