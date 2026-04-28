// Three ways a single client process can go away — Windows version.
// Win32 has no fork(), so we re-exec the test binary itself in --client
// mode via spawn_process. File-presence is used for parent/child
// synchronization since Win32 pipe-handle inheritance isn't worth the
// extra plumbing for this scope.
//
//   ClientClose — child calls seekdb_close, then exit(0).
//   ClientExit  — child exit(0) without seekdb_close (kernel releases
//                 the SH lock via handle cleanup).
//   KillClient  — child loops; parent terminates it.
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

/* Captured at main() entry so test bodies can re-spawn this binary. */
std::string g_test_bin;

/* Child mode: opens the database, signals ready by creating
 * <ready_path>, then dispatches on `mode`:
 *   "close" — call seekdb_close, exit 0
 *   "exit"  — exit 0 without seekdb_close
 *   "wait"  — sleep forever (parent will terminate_process us) */
int run_client(const char *mode,
               const char *bin_path,
               const char *db_dir,
               const char *ready_path)
{
    SeekdbHandle h = nullptr;
    if (seekdb_open(bin_path, db_dir, 0, &h) != SEEKDB_SUCCESS) return 10;

    std::FILE *f = std::fopen(ready_path, "w");
    if (!f) return 11;
    std::fclose(f);

    if (std::strcmp(mode, "close") == 0) {
        seekdb_close(h);
        return 0;
    }
    if (std::strcmp(mode, "exit") == 0) {
        return 0;
    }
    /* "wait": loop until the parent terminates us. */
    while (true) std::this_thread::sleep_for(60s);
    return 0;
}

class OneClientProcess : public ::testing::Test {
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

    /* Spawn the test binary in --client mode and block until the child
     * has created its ready marker. */
    Process *spawn_client(const char *mode) {
        const std::string ready_path = db_dir_ + "/.client_ready";
        char *argv[] = {
            const_cast<char *>(g_test_bin.c_str()),
            const_cast<char *>("--client"),
            const_cast<char *>(mode),
            const_cast<char *>(bin_path_.c_str()),
            const_cast<char *>(db_dir_.c_str()),
            const_cast<char *>(ready_path.c_str()),
            nullptr,
        };
        Process *child = nullptr;
        if (spawn_process(g_test_bin.c_str(), argv, &child) != OK) return nullptr;
        while (!fs::exists(ready_path))
            std::this_thread::sleep_for(100ms);
        return child;
    }
};

TEST_F(OneClientProcess, ClientClose)
{
    Process *child = spawn_client("close");
    ASSERT_NE(child, nullptr);

    const auto ddl = std::chrono::steady_clock::now() + 15s;
    const int64_t server_pid = read_server_pid(db_dir_);

    /* Child exits on its own — call seekdb_close + return 0. */
    while (reap_process(child) != 1)
        std::this_thread::sleep_for(100ms);
    free(child);

    while (!is_server_reaped(server_pid) && std::chrono::steady_clock::now() < ddl)
        std::this_thread::sleep_for(1s);
    ASSERT_TRUE(is_server_reaped(server_pid))
        << "server " << server_pid << " not reaped within 15s after client close";
}

TEST_F(OneClientProcess, ClientExit)
{
    Process *child = spawn_client("exit");
    ASSERT_NE(child, nullptr);

    const auto ddl = std::chrono::steady_clock::now() + 15s;
    const int64_t server_pid = read_server_pid(db_dir_);

    while (reap_process(child) != 1)
        std::this_thread::sleep_for(100ms);
    free(child);

    while (!is_server_reaped(server_pid) && std::chrono::steady_clock::now() < ddl)
        std::this_thread::sleep_for(1s);
    ASSERT_TRUE(is_server_reaped(server_pid))
        << "server " << server_pid << " not reaped within 15s after client exit";
}

TEST_F(OneClientProcess, KillClient)
{
    Process *child = spawn_client("wait");
    ASSERT_NE(child, nullptr);

    const auto ddl = std::chrono::steady_clock::now() + 15s;
    const int64_t server_pid = read_server_pid(db_dir_);

    /* Kill the client; daemon detects last-client-gone via SH-lock release. */
    terminate_process(child->pid, /*graceful=*/0);
    while (reap_process(child) != 1)
        std::this_thread::sleep_for(100ms);
    free(child);

    while (!is_server_reaped(server_pid) && std::chrono::steady_clock::now() < ddl)
        std::this_thread::sleep_for(1s);
    ASSERT_TRUE(is_server_reaped(server_pid))
        << "server " << server_pid << " not reaped within 15s after client kill";
}

}  // namespace

int main(int argc, char **argv)
{
    if (argc >= 6 && std::strcmp(argv[1], "--client") == 0) {
        return run_client(argv[2], argv[3], argv[4], argv[5]);
    }
    g_test_bin = argv[0];
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
