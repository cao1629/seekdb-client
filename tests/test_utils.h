// Observation helpers shared across seekdb-client tests.

#pragma once

#include "port.h"

#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <functional>
#include <string>
#include <thread>

#ifdef __linux__
#include <sys/stat.h>
#endif

/* Timestamped debug print. Format: [HH:MM:SS.mmm T=<tid>] ... */
inline void tlog(const char *fmt, ...)
{
    using namespace std::chrono;
    auto now = system_clock::now();
    auto tt  = system_clock::to_time_t(now);
    auto ms  = duration_cast<milliseconds>(now.time_since_epoch()).count() % 1000;
    std::tm tm_buf;
#ifdef _WIN32
    localtime_s(&tm_buf, &tt);
#else
    localtime_r(&tt, &tm_buf);
#endif
    auto tid = std::hash<std::thread::id>{}(std::this_thread::get_id());

    char prefix[64];
    int p = std::snprintf(prefix, sizeof(prefix),
                          "[%02d:%02d:%02d.%03lld T=%llx] ",
                          tm_buf.tm_hour, tm_buf.tm_min, tm_buf.tm_sec,
                          (long long)ms, (unsigned long long)tid);

    char msg[512];
    std::va_list ap;
    va_start(ap, fmt);
    std::vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    char buf[600];
    std::snprintf(buf, sizeof(buf), "%.*s%s", p, prefix, msg);
    std::fputs(buf, stdout);
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
