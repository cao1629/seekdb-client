// KillTwoClientProcessesOneByOne — Windows version. Spawns two clients
// (A, B) by re-exec'ing the test binary in --client-a / --client-b mode;
// uses file-presence files for parent/child synchronization (Win32 has
// no fork()+pipe inheritance, so we don't try to mimic POSIX pipes).
//
// Flow:
//   1. Spawn A; A opens db, signals .a_ready, then sleeps forever.
//   2. Spawn B; B opens db (fast path, server already up), signals
//      .b_ready, then waits for .b_go_query.
//   3. Parent kills A. Server should still be alive because B holds SH.
//   4. Parent creates .b_go_query. B runs SELECT 1 and creates
//      .b_query_done.
//   5. Parent kills B. Server should shut itself down (last-client-gone).
//
// Env:
//   SEEKDB_BIN   path to the seekdb binary

#include <gtest/gtest.h>

#include "port.h"
#include "seekdb.h"
#include "test_utils.h"

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

std::string g_test_bin;

int run_client_a(const char *bin_path, const char *db_dir, const char *ready_path)
{
    SeekdbHandle h = nullptr;
    if (seekdb_open(bin_path, db_dir, 0, &h) != SEEKDB_SUCCESS) return 10;

    std::FILE *f = std::fopen(ready_path, "w");
    if (!f) return 11;
    std::fclose(f);

    while (true) std::this_thread::sleep_for(60s);
    return 0;
}

int run_client_b(const char *bin_path, const char *db_dir,
                 const char *ready_path,
                 const char *go_query_path,
                 const char *query_done_path)
{
    SeekdbHandle h = nullptr;
    if (seekdb_open(bin_path, db_dir, 0, &h) != SEEKDB_SUCCESS) return 10;

    std::FILE *f = std::fopen(ready_path, "w");
    if (!f) return 11;
    std::fclose(f);

    /* Wait for parent's "go query" signal. */
    while (!fs::exists(go_query_path))
        std::this_thread::sleep_for(100ms);

    SeekdbConnection c = nullptr;
    if (seekdb_connect(h, nullptr, true, &c) != SEEKDB_SUCCESS) return 20;
    SeekdbResult r = nullptr;
    if (seekdb_query(c, "SELECT 1", 8, &r) != SEEKDB_SUCCESS) return 21;
    if (r) seekdb_result_free(r);
    seekdb_disconnect(c);

    /* Signal "query done". */
    f = std::fopen(query_done_path, "w");
    if (!f) return 22;
    std::fclose(f);

    while (true) std::this_thread::sleep_for(60s);
    return 0;
}

class TwoClientsProcesses : public ::testing::Test {
protected:
    std::string bin_path_;
    std::string db_dir_;

    void SetUp() override {
        const char *bin = std::getenv("SEEKDB_BIN");
        ASSERT_NE(bin, nullptr) << "set SEEKDB_BIN to the seekdb binary";
        bin_path_ = bin;
        ASSERT_TRUE(fs::exists(bin_path_));

        db_dir_ = (fs::temp_directory_path() / "seekdb_test_db").string();
        fs::remove_all(db_dir_);
        fs::create_directories(db_dir_);
    }

    void TearDown() override {
        int64_t pid = read_server_pid(db_dir_);
        while (!is_server_reaped(pid))
            std::this_thread::sleep_for(200ms);
    }
};

TEST_F(TwoClientsProcesses, KillTwoClientProcessesOneByOne)
{
    const std::string a_ready      = db_dir_ + "/.a_ready";
    const std::string b_ready      = db_dir_ + "/.b_ready";
    const std::string b_go_query   = db_dir_ + "/.b_go_query";
    const std::string b_query_done = db_dir_ + "/.b_query_done";

    /* Spawn A. */
    {
        char *argv[] = {
            const_cast<char *>(g_test_bin.c_str()),
            const_cast<char *>("--client-a"),
            const_cast<char *>(bin_path_.c_str()),
            const_cast<char *>(db_dir_.c_str()),
            const_cast<char *>(a_ready.c_str()),
            nullptr,
        };
        Process *a = nullptr;
        ASSERT_EQ(spawn_process(g_test_bin.c_str(), argv, &a), OK);
        while (!fs::exists(a_ready))
            std::this_thread::sleep_for(100ms);

        const int64_t server_pid = read_server_pid(db_dir_);
        ASSERT_GT(server_pid, 0);
        ASSERT_FALSE(is_server_reaped(server_pid));

        /* Spawn B. */
        char *b_argv[] = {
            const_cast<char *>(g_test_bin.c_str()),
            const_cast<char *>("--client-b"),
            const_cast<char *>(bin_path_.c_str()),
            const_cast<char *>(db_dir_.c_str()),
            const_cast<char *>(b_ready.c_str()),
            const_cast<char *>(b_go_query.c_str()),
            const_cast<char *>(b_query_done.c_str()),
            nullptr,
        };
        Process *b = nullptr;
        ASSERT_EQ(spawn_process(g_test_bin.c_str(), b_argv, &b), OK);
        while (!fs::exists(b_ready))
            std::this_thread::sleep_for(100ms);

        /* B must not have spawned a second server. */
        EXPECT_EQ(read_server_pid(db_dir_), server_pid)
            << "seekdb.pid changed -- B unexpectedly spawned";
        EXPECT_FALSE(is_server_reaped(server_pid));

        /* Kill A. B still holds its SH on seekdb.clients, so the server
         * should stay alive. */
        terminate_process(a->pid, /*graceful=*/0);
        while (reap_process(a) != 1)
            std::this_thread::sleep_for(100ms);
        free(a);

        std::this_thread::sleep_for(6s);
        EXPECT_FALSE(is_server_reaped(server_pid))
            << "server died with B still holding SH";

        /* Tell B to run SELECT 1; wait for it to finish. */
        {
            std::FILE *f = std::fopen(b_go_query.c_str(), "w");
            ASSERT_NE(f, nullptr);
            std::fclose(f);
        }
        while (!fs::exists(b_query_done))
            std::this_thread::sleep_for(100ms);

        /* Kill B; now no client holds the SH; server should shut down. */
        terminate_process(b->pid, /*graceful=*/0);
        while (reap_process(b) != 1)
            std::this_thread::sleep_for(100ms);
        free(b);

        auto deadline = std::chrono::steady_clock::now() + 15s;
        while (!is_server_reaped(server_pid) && std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(200ms);
        EXPECT_TRUE(is_server_reaped(server_pid))
            << "server " << server_pid << " still alive 15s after last client was killed";
    }
}

}  // namespace

int main(int argc, char **argv)
{
    if (argc >= 5 && std::strcmp(argv[1], "--client-a") == 0) {
        return run_client_a(argv[2], argv[3], argv[4]);
    }
    if (argc >= 7 && std::strcmp(argv[1], "--client-b") == 0) {
        return run_client_b(argv[2], argv[3], argv[4], argv[5], argv[6]);
    }
    g_test_bin = argv[0];
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
