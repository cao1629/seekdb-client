// Observation helpers shared across seekdb-client tests.

#pragma once

#include "port.h"
#include "tlog.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>

#ifdef __linux__
#include <sys/stat.h>
#endif

// Build a unique db_dir path for the currently-running TEST_F, rooted under
// `root` (typically SEEKDB_TEST_DATA_ROOT). Combines suite + test name with a
// nanosecond timestamp so each invocation gets a fresh path — no fs::remove_all
// needed, which matters on Windows where files held open by the previous
// TEST_F's daemon (or by an orphan from a crashed prior run) can't be deleted.
inline std::string make_per_test_db_dir(const std::string &root)
{
    const auto *info = ::testing::UnitTest::GetInstance()->current_test_info();
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    return root + "/" + info->test_suite_name() + "."
                + info->name() + "_" + std::to_string(ns);
}

inline int64_t read_server_pid(const std::string &db_dir)
{
    std::FILE *f = std::fopen((db_dir + "/run/seekdb.pid").c_str(), "r");
    if (!f) return -1;
    long long pid = 0;
    int got = std::fscanf(f, "%lld", &pid);
    std::fclose(f);
    return got == 1 ? (int64_t)pid : -1;
}

#ifdef __linux__
// Read /proc/locks and look for a FLOCK entry on the given file's inode.
// Linux-only — macOS and Windows expose lock info through different APIs
// or not at all. If mode_filter is non-null, only entries with that mode
// match ("READ" or "WRITE"); otherwise any FLOCK entry counts. Pure
// observation, no interaction with the lock.
inline bool someone_holds_flock(const std::string &path,
                                const char *mode_filter = nullptr)
{
    struct stat st;
    if (stat(path.c_str(), &st) != 0) return false;
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
#endif
