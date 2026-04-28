// Three ways a single client process can go away. In each case the
// last-client-gone detection on seekdb.clients should make the server
// shut itself down, and the test asserts the server is reaped within
// 15s of the client process being forked.
//
//   ClientClose — child calls seekdb_close, then _exit(0).
//   ClientExit  — child _exits without seekdb_close (kernel releases
//                 the SH lock via fd cleanup).
//   KillClient  — child loops; parent SIGKILLs it.
//
// Env:
//   SEEKDB_BIN   path to the seekdb binary

#include <gtest/gtest.h>

#include "port.h"
#include "seekdb.h"
#include "test_utils.h"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <sys/wait.h>
#include <thread>
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
        int64_t pid = read_server_pid(db_dir_);
        while (!is_server_reaped(pid))
            std::this_thread::sleep_for(200ms);
        printf("Torn down.\n");
    }
};

TEST_F(OneClientProcess, ClientClose)
{
    int ready[2];
    pipe(ready);

    pid_t client_pid = fork();
    if (client_pid == 0) {
        close(ready[0]);
        SeekdbHandle h = nullptr;
        seekdb_open(bin_path_.c_str(), db_dir_.c_str(), 0, &h);
        const char y = 'Y';
        write(ready[1], &y, 1);
        close(ready[1]);
        seekdb_close(h);
        _exit(0);
    }
    close(ready[1]);
    char buf;
    read(ready[0], &buf, 1);
    close(ready[0]);

    const auto ddl = std::chrono::steady_clock::now() + 15s;

    const int64_t server_pid = read_server_pid(db_dir_);

    while (!is_server_reaped(server_pid) && std::chrono::steady_clock::now() < ddl)
        std::this_thread::sleep_for(1s);

    ASSERT_TRUE(is_server_reaped(server_pid))
        << "server " << server_pid << " not reaped within 15s after client close";
}

TEST_F(OneClientProcess, ClientExit)
{
    int ready[2];
    pipe(ready);

    pid_t client_pid = fork();
    if (client_pid == 0) {
        close(ready[0]);
        SeekdbHandle h = nullptr;
        seekdb_open(bin_path_.c_str(), db_dir_.c_str(), 0, &h);
        const char y = 'Y';
        write(ready[1], &y, 1);
        close(ready[1]);
        _exit(0);
    }
    close(ready[1]);
    char buf;
    read(ready[0], &buf, 1);
    close(ready[0]);

    const auto ddl = std::chrono::steady_clock::now() + 15s;

    const int64_t server_pid = read_server_pid(db_dir_);

    while (!is_server_reaped(server_pid) && std::chrono::steady_clock::now() < ddl)
        std::this_thread::sleep_for(1s);
    
    ASSERT_TRUE(is_server_reaped(server_pid))
        << "server " << server_pid << " not reaped within 15s after client close";
}

TEST_F(OneClientProcess, KillClient)
{
    int ready[2];
    pipe(ready);

    pid_t client_pid = fork();
    if (client_pid == 0) {
        close(ready[0]);
        SeekdbHandle h = nullptr;
        seekdb_open(bin_path_.c_str(), db_dir_.c_str(), 0, &h);
        const char y = 'Y';
        write(ready[1], &y, 1);
        close(ready[1]);
        while (true) {
            std::this_thread::sleep_for(10s);
        };
    }
    close(ready[1]);
    char buf;
    read(ready[0], &buf, 1);
    close(ready[0]);

    const auto ddl = std::chrono::steady_clock::now() + 15s;

    const int64_t server_pid = read_server_pid(db_dir_);

    terminate_process(client_pid, true);

    while (!is_server_reaped(server_pid) && std::chrono::steady_clock::now() < ddl)
        std::this_thread::sleep_for(1s);

    ASSERT_TRUE(is_server_reaped(server_pid))
        << "server " << server_pid << " not reaped within 15s after client close";
}

}  // namespace
