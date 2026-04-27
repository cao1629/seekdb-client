/*
 * port.h — Platform Abstraction Layer for seekdb-client.
 *
 * Current scope: file locks and spawned-process lifecycle.
 * Future iterations may add time, threading, mutex, mkdir, etc.
 *
 * Backed by src/port_posix.c on Linux/macOS; src/port_win32.c (planned)
 * for native Windows. CMake selects which backend to compile.
 *
 * Naming: function and type names are unprefixed (flock_open, Process,
 * etc.) for readability. Status codes keep a PORT_ prefix to avoid
 * global-namespace collisions.
 */

#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================ status ==== */

enum {
    PORT_OK              =  0,
    PORT_ERR             = -1,   /* generic */
    PORT_ERR_BUSY        = -2,   /* try_acquire would block */
    PORT_ERR_INTERRUPTED = -3,   /* signal interrupted; caller may retry */
    PORT_ERR_NO_MEMORY   = -4,
    PORT_ERR_INVALID_ARG = -5
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
int flock_open(const char *path, Flock **out_lock);

/*
 * Acquire `mode` on the lock. Blocks until acquired. Retries
 * automatically on signal interruption (POSIX EINTR).
 */
int flock_acquire(Flock *lock, FlockMode mode);

/*
 * Same as flock_acquire but never blocks — returns PORT_ERR_BUSY
 * immediately if a conflicting lock exists.
 */
int flock_try_acquire(Flock *lock, FlockMode mode);

/*
 * Release any held lock; keep the underlying file open so the caller
 * can re-acquire without re-opening.
 */
int flock_release(Flock *lock);

/*
 * Release any held lock and close the underlying file. Frees `lock`.
 * Safe to call with NULL.
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
 * Non-blocking exit check. Sets *out_exited to 1 if the spawned child
 * has exited (and reaps it), 0 otherwise. Returns PORT_OK if the call
 * succeeded; non-PORT_OK on a real error.
 *
 * Idempotent: once *out_exited is 1, subsequent calls also return 1
 * without re-issuing waitpid (so calling this in a polling loop is safe).
 */
int process_wait_nonblock(Process *proc, int *out_exited);

/*
 * Send termination to the spawned child. `graceful=1` requests an
 * orderly shutdown (POSIX SIGTERM); `graceful=0` is unconditional kill
 * (POSIX SIGKILL). No-op if the child has already exited.
 */
int process_terminate(Process *proc, int graceful);

/*
 * Free the Process handle and any associated kernel resources. Does
 * NOT terminate a still-running child — call process_terminate first
 * if needed. Safe to call with NULL.
 */
int process_close(Process *proc);

/* For introspection (debug logging). Returns -1 if not applicable. */
int64_t process_pid(const Process *proc);

/* ====================================================== filesystem ===== */

/*
 * Create directory `path` with default permissions (POSIX 0755).
 * Returns PORT_OK if the directory exists after the call (whether
 * created or already there); non-PORT_OK on real failure.
 *
 * Named ensure_dir (not mkdir) to avoid colliding with POSIX mkdir(2)
 * from <sys/stat.h>.
 */
int ensure_dir(const char *path);

#ifdef __cplusplus
}
#endif
