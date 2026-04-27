/*
 * port.h — Platform Abstraction Layer for seekdb-client.
 *
 * Current scope: file locks and spawned-process lifecycle.
 * Future iterations may add time, threading, mutex, mkdir, etc.
 *
 * Backed by src/port_posix.c on Linux/macOS; src/port_win32.c (planned)
 * for native Windows. CMake selects which backend to compile.
 *
 * Naming: function names, type names, and status codes are all
 * unprefixed (flock_open, Process, OK, ERR, ...) for readability.
 * Caveat: OK and ERR are very generic — anyone including port.h
 * must avoid colliding macros / globals.
 */

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

typedef struct Flock     Flock;     /* opaque file-lock handle */
typedef struct Process  Process;  /* opaque spawned-process handle */

typedef enum {
    FLOCK_SHARED,
    FLOCK_EXCLUSIVE
} FlockMode;

/* ====================================================== file locks ====== */

/*
 * Open (creating if necessary) a lock file at `path`. The returned Flock
 * initially holds no lock — call flock_acquire to take one.
 *
 * On Windows the underlying handle is opened with FILE_SHARE_READ|WRITE
 * so peer processes can also open it.
 */
int flock_open(const char *path, Flock **out_flock);

/*
 * Acquire `mode` on the lock. Blocks until acquired. Has no failure
 * mode in practice — flock(LOCK_SH/LOCK_EX) can only fail on EBADF /
 * EINVAL (programmer error) or extreme conditions like EINTR /
 * ENOLCK that we don't surface in this codebase.
 */
void flock_acquire(Flock *lock, FlockMode mode);

/*
 * Same as flock_acquire but never blocks — returns ERR immediately
 * if a conflicting lock already exists.
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
 * The returned Process must be freed with process_close regardless of
 * whether the child exits.
 */
int spawn(const char *bin_path, char *const argv[], Process **out_proc);

/*
 * Non-blocking liveness check on a spawned child. Returns 1 if the
 * child is still running, 0 if it has exited (and is reaped here as a
 * side effect on POSIX) or is otherwise no longer reachable.
 *
 * POSIX: waitpid(pid, NULL, WNOHANG) — returns 0 ⇒ running.
 * Win32:  WaitForSingleObject(handle, 0) — returns WAIT_TIMEOUT ⇒ running.
 */
int is_spawned_server_running(Process *proc);

/*
 * Free the Process handle and any associated kernel resources. Does
 * NOT terminate a still-running child — call terminate_process
 * first (with process_pid(proc)) if you need that. Safe to call with NULL.
 */
int process_close(Process *proc);

/* For introspection (debug logging). Returns -1 if not applicable. */
int64_t process_pid(const Process *proc);

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
