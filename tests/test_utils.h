// Observation helpers and child-process harness shared across tests.

#pragma once

#include "seekdb.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdio>
#include <cstring>
#include <signal.h>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>

inline pid_t read_pid(const std::string &db_dir)
{
    std::FILE *f = std::fopen((db_dir + "/run/seekdb.pid").c_str(), "r");
    if (!f) return -1;
    long pid = 0;
    int got = std::fscanf(f, "%ld", &pid);
    std::fclose(f);
    return got == 1 ? (pid_t)pid : -1;
}

inline bool alive(pid_t pid)
{
    return pid > 0 && ::kill(pid, 0) == 0;
}

inline bool wait_until_gone(pid_t pid, std::chrono::milliseconds timeout)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (alive(pid) && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    return !alive(pid);
}

// Read /proc/locks and look for a FLOCK entry on the given file's inode.
// If mode_filter is non-null, only entries with that mode match ("READ" or
// "WRITE"); otherwise any FLOCK entry counts. Pure observation, no
// interaction with the lock.
inline bool someone_holds_flock(const std::string &path,
                                const char *mode_filter = nullptr)
{
    struct stat st;
    if (::stat(path.c_str(), &st) != 0) return false;
    const unsigned long want_inode = st.st_ino;

    std::FILE *f = std::fopen("/proc/locks", "r");
    if (!f) return false;

    char line[256];
    bool found = false;
    while (std::fgets(line, sizeof(line), f)) {
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

struct Client {
    pid_t pid;
    int   parent_wait;   // parent blocks reading here; child writes to unblock it
    int   child_wait;    // parent close()s here to unblock child's read
};

// Fork a client child. The child opens seekdb at db_dir (spawning the server
// via bin_path if needed), runs SELECT 1, writes one byte to unblock the
// parent, blocks waiting for the parent to release it, then closes and
// exits(0). Any failure in the open/query phase exits with a nonzero status.
inline Client fork_client(const std::string &bin_path,
                          const std::string &db_dir)
{
    int parent_wait[2] = {-1, -1};   // blocks parent until child writes
    int child_wait[2]  = {-1, -1};   // blocks child until parent closes
    EXPECT_EQ(::pipe(parent_wait), 0);
    EXPECT_EQ(::pipe(child_wait),  0);

    pid_t pid = ::fork();
    if (pid == 0) {
        ::close(parent_wait[0]);
        ::close(child_wait[1]);

        SeekdbHandle h = nullptr;
        SeekdbConnection c = nullptr;
        if (seekdb_open(bin_path.c_str(), db_dir.c_str(), 0, &h)
            != SEEKDB_SUCCESS) _exit(10);
        if (seekdb_connect(h, nullptr, true, &c) != SEEKDB_SUCCESS) _exit(11);
        SeekdbResult r = nullptr;
        if (seekdb_query(c, "SELECT 1", 8, &r) != SEEKDB_SUCCESS) _exit(12);
        if (r) seekdb_result_free(r);

        char byte = 'Y';
        if (::write(parent_wait[1], &byte, 1) != 1) _exit(13);
        ::close(parent_wait[1]);

        char buf;
        ::read(child_wait[0], &buf, 1);  // returns 0 when parent closes its end
        ::close(child_wait[0]);

        seekdb_disconnect(c);
        seekdb_close(h);
        _exit(0);
    }

    ::close(parent_wait[1]);
    ::close(child_wait[0]);
    return Client{pid, parent_wait[0], child_wait[1]};
}
