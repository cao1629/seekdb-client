#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================ status ==== */

enum {
    OK              =  0,
    ERR             = -1,   /* generic failure */
    ERR_INVALID_ARG = -2
};

/* ============================================================ types ===== */

typedef struct Flock Flock;        /* opaque file-lock handle */

/* Spawned-process handle. `pid` is exposed for logging; `handle` is a
 * Win32 HANDLE managed exclusively by port_win32.c (kept as void* here
 * so consumers don't need <windows.h>). */
typedef struct Process {
    int64_t pid;
#ifdef _WIN32
    void *handle;
#endif
} Process;

typedef enum {
    FLOCK_SHARED,
    FLOCK_EXCLUSIVE
} FlockMode;

/* ====================================================== file locks ====== */

/*
 * Open (creating if necessary) a lock file at `path`. The returned Flock
 * initially holds no lock — call flock_acquire to take one.
 */
int flock_open(const char *path, Flock **out_flock);

/*
 * Acquire `mode` on the lock. Blocks until acquired.
 */
void flock_acquire(Flock *lock, FlockMode mode);

/*
 * Same as flock_acquire but never blocks. Returns 1 if the lock was
 * acquired, 0 if it was not.
 */
int flock_try_acquire(Flock *lock, FlockMode mode);

/*
 * Release any held lock; keep the underlying file open so the caller
 * can re-acquire without re-opening.
 */
void flock_release(Flock *lock);

/*
 * Release any held lock and close the underlying file. Frees `lock`.
 * Returns ERR if called with NULL.
 */
int flock_close(Flock *lock);

/* ===================================================== processes ====== */

/*
 * Spawn a child process running `bin_path` with `argv` (NULL-terminated,
 * argv[0] = program name).
 *
 * No fd inheritance control: matches POSIX posix_spawn defaults plus the
 * project's O_CLOEXEC discipline (the lock fds in seekdb_open carry
 * O_CLOEXEC, so they aren't inherited).
 *
 * The returned Process is freed by reap_if_exited once the child exits.
 */
int spawn(const char *bin_path, char *const argv[], Process **out_proc);

/*
 * Non-blocking exit check that performs the kernel-side reap when the
 * child has exited. Returns 1 if exited, 0 if still running. On a
 * return of 1 the caller is responsible for `free(proc)` to release
 * the userspace struct.
 *
 * POSIX: waitpid(pid, NULL, WNOHANG) reaps the zombie when it returns the pid.
 * Win32: WaitForSingleObject(handle, 0); if signaled, CloseHandle on the
 *        process handle (the Win32 equivalent of reaping).
 */
int reap_if_exited(Process *proc);

/*
 * Probe whether the process with `pid` has been reaped (no longer in
 * the kernel's process table). Returns 1 if reaped (also if pid <= 0
 * or the caller has no permission to query); 0 if the process still
 * exists (running or zombie).
 *
 * POSIX: kill(pid, 0) returning ESRCH. Win32: OpenProcess fails, or
 * WaitForSingleObject(handle, 0) returns WAIT_OBJECT_0.
 *
 * Unlike process_wait_nonblock, this works on any pid the caller can
 * see — not just direct children of this process.
 */
int is_server_reaped(int64_t pid);

/*
 * Send termination to the process identified by `pid`. Same `graceful`
 * semantics as process_terminate (POSIX SIGTERM vs SIGKILL; Win32
 * always hard). Returns OK if the signal was delivered, OK if the
 * process is already gone, non-OK on real failure.
 */
int terminate_process(int64_t pid, int graceful);

/* ====================================================== filesystem ===== */

/*
 * Create directory `path` with default permissions (POSIX 0755).
 * Returns OK if the directory exists after the call (whether
 * created or already there); non-OK on real failure.
 *
 * Named ensure_dir (not mkdir) to avoid colliding with POSIX mkdir(2)
 * from <sys/stat.h>.
 */
int ensure_dir(const char *path);

#ifdef __cplusplus
}
#endif
