#pragma once

#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <ctime>
#include <functional>
#include <thread>

/* Timestamped debug print. Format: [HH:MM:SS.mmm T=<tid>] ...
 * Built into a single fputs so output is atomic per stream-locking. */
inline void tlog(const char *fmt, ...)
{
    using namespace std::chrono;
    auto now  = system_clock::now();
    auto tt   = system_clock::to_time_t(now);
    auto ms   = duration_cast<milliseconds>(now.time_since_epoch()).count() % 1000;
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
