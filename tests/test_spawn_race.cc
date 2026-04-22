// Two scenarios for a second client arriving while a first is open:
//
//   AWinsStartupAndSurvives -- B arrives while A still holds seekdb.startup.
//     Both clients race on that lock; A wins and spawns S1, B then spawns
//     S2 which loses the seekdb.pid race and _exit(0)s. Survivor is A's S1.
//
//   BJoinsAlreadyRunningServer -- B arrives after A's seekdb_open returns.
//     A's server is already up, so B's try_connect succeeds immediately;
//     B never reaches seekdb.startup or posix_spawn at all.
//
// Env:
//   SEEKDB_BIN   path to the seekdb binary

#include <gtest/gtest.h>

#include "seekdb.h"

#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <mutex>
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

class SpawnRaceTest : public ::testing::Test {
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
            wait_until_dead(pid, 5s);
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

    static bool wait_until_dead(pid_t pid, std::chrono::milliseconds budget) {
        const auto deadline = std::chrono::steady_clock::now() + budget;
        while (alive(pid) && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(200ms);
        }
        return !alive(pid);
    }

    // Read /proc/locks and look for a FLOCK entry on the given file's inode.
    // If mode_filter is non-null, only entries with that mode match ("READ" or
    // "WRITE"); otherwise any FLOCK entry counts. Pure observation, no
    // interaction with the lock.
    bool someone_holds_flock(const std::string &path,
                             const char *mode_filter = nullptr) const {
        struct stat st;
        if (::stat(path.c_str(), &st) != 0) return false;
        const unsigned long want_inode = st.st_ino;

        std::FILE *f = std::fopen("/proc/locks", "r");
        if (!f) return false;

        char line[256];
        bool found = false;
        while (std::fgets(line, sizeof(line), f)) {
            // Example: "4: FLOCK  ADVISORY  WRITE 12345 103:01:29516501 0 EOF"
            char type[16] = {}, adv[16] = {}, mode[16] = {};
            unsigned long pid = 0;
            unsigned major = 0, minor = 0;
            unsigned long inode = 0;
            if (std::sscanf(line, "%*d: %15s %15s %15s %lu %u:%u:%lu",
                            type, adv, mode, &pid, &major, &minor, &inode) == 7) {
                if (std::strcmp(type, "FLOCK") == 0 &&
                    inode == want_inode &&
                    (mode_filter == nullptr || std::strcmp(mode, mode_filter) == 0)) {
                    found = true;
                    break;
                }
            }
        }
        std::fclose(f);
        return found;
    }

    int count_seekdb_processes() const {
        std::FILE *pipe = ::popen("pgrep -xc seekdb", "r");
        if (!pipe) return -1;
        char buf[32] = {};
        std::fread(buf, 1, sizeof(buf) - 1, pipe);
        ::pclose(pipe);
        return std::atoi(buf);
    }
};

// ---------------------------------------------------------------------------
// Case 1: B arrives while A still holds flock(EX) seekdb.startup.
// ---------------------------------------------------------------------------
TEST_F(SpawnRaceTest, AWinsStartupAndSurvives)
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

    // Wait for A to actually take flock(EX) seekdb.startup. Only then
    // is it guaranteed B will race on the same lock.
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    const std::string startup_path = db_dir_ + "/run/seekdb.startup";
    while (!someone_holds_flock(startup_path, "WRITE")) {
        ASSERT_LT(std::chrono::steady_clock::now(), deadline)
            << "A never took flock(EX) seekdb.startup";
        std::this_thread::sleep_for(1ms);
    }

    std::thread tb(run_client, std::ref(h_b), std::ref(c_b),
                   std::ref(b_open_rc), std::ref(b_query_rc),
                   std::ref(b_opened));

    {
        std::unique_lock<std::mutex> lk(m);
        cv.wait(lk, [&] { return a_opened; });
    }
    ASSERT_EQ(a_open_rc, SEEKDB_SUCCESS);
    ASSERT_EQ(a_query_rc, SEEKDB_SUCCESS);

    const pid_t a_server_pid = read_pid();
    ASSERT_GT(a_server_pid, 0);
    ASSERT_TRUE(alive(a_server_pid));

    {
        std::unique_lock<std::mutex> lk(m);
        cv.wait(lk, [&] { return b_opened; });
    }
    ASSERT_EQ(b_open_rc, SEEKDB_SUCCESS);
    ASSERT_EQ(b_query_rc, SEEKDB_SUCCESS);

    // Headline invariant: survivor is exactly the process A spawned.
    EXPECT_EQ(read_pid(), a_server_pid)
        << "seekdb.pid changed after B's open -- B's spawn was not supposed to win";
    EXPECT_TRUE(alive(a_server_pid));

    EXPECT_TRUE(fs::exists(db_dir_ + "/run/sql.sock"));
    EXPECT_TRUE(fs::exists(db_dir_ + "/run/seekdb.clients"));

    {
        std::lock_guard<std::mutex> lk(m);
        close_signal = true;
    }
    cv.notify_all();
    ta.join();
    tb.join();

    EXPECT_TRUE(wait_until_dead(a_server_pid, 15s))
        << "server " << a_server_pid << " still alive 15s after both clients closed";
}

// ---------------------------------------------------------------------------
// Case 2: B arrives after A's seekdb_open has fully returned.
// ---------------------------------------------------------------------------
TEST_F(SpawnRaceTest, BJoinsAlreadyRunningServer)
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

    const pid_t server_pid = read_pid();
    ASSERT_GT(server_pid, 0);
    ASSERT_TRUE(alive(server_pid));
    ASSERT_EQ(count_seekdb_processes(), 1) << "more than one seekdb before B starts";

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
    EXPECT_EQ(read_pid(), server_pid)
        << "seekdb.pid changed -- B unexpectedly spawned";
    EXPECT_TRUE(alive(server_pid));
    EXPECT_EQ(count_seekdb_processes(), 1)
        << "more than one seekdb alive -- B unexpectedly spawned";

    EXPECT_TRUE(fs::exists(db_dir_ + "/run/sql.sock"));

    {
        std::lock_guard<std::mutex> lk(m);
        close_signal = true;
    }
    cv.notify_all();
    ta.join();
    tb.join();

    EXPECT_TRUE(wait_until_dead(server_pid, 15s))
        << "server " << server_pid << " still alive 15s after both clients closed";
}

}  // namespace
