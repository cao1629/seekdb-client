// Observation helpers shared across seekdb-client tests.

#pragma once

#include <chrono>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <pthread.h>
#include <signal.h>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>

/* Timestamped debug print. Format: [HH:MM:SS.mmm T=<tid>] ...
 * Single printf call so output is atomic per POSIX stream locking. */
#define tlog(fmt, ...) do { \
    struct timespec _ts; \
    clock_gettime(CLOCK_REALTIME, &_ts); \
    struct tm _tm; \
    localtime_r(&_ts.tv_sec, &_tm); \
    std::printf("[%02d:%02d:%02d.%03ld T=%lx] " fmt, \
                _tm.tm_hour, _tm.tm_min, _tm.tm_sec, _ts.tv_nsec / 1000000, \
                (unsigned long)pthread_self(), \
                ##__VA_ARGS__); \
} while (0)

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

