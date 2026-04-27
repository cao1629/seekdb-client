/*
 * port_posix.c — POSIX backend for src/port.h.
 *
 * Implements file locks (via flock) and process spawn/wait/terminate
 * (via posix_spawn / waitpid / kill).
 */

#include "port.h"

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <spawn.h>
#include <stdlib.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

/* ============================================================ Flock ====== */

struct Flock {
    int fd;
};

int flock_open(const char *path, Flock **out_lock)
{
    if (!path || !out_lock) return PORT_ERR_INVALID_ARG;
    *out_lock = NULL;

    int fd = open(path, O_CREAT | O_RDWR | O_CLOEXEC, 0644);
    if (fd < 0) return PORT_ERR;

    Flock *l = malloc(sizeof(*l));
    if (!l) { close(fd); return PORT_ERR_NO_MEMORY; }
    l->fd = fd;
    *out_lock = l;
    return PORT_OK;
}

static int flock_op_for(FlockMode mode)
{
    return (mode == FLOCK_SHARED) ? LOCK_SH : LOCK_EX;
}

int flock_acquire(Flock *lock, FlockMode mode)
{
    if (!lock) return PORT_ERR_INVALID_ARG;
    int op = flock_op_for(mode);
    while (flock(lock->fd, op) != 0) {
        if (errno == EINTR) continue;        /* hide EINTR from caller */
        return PORT_ERR;
    }
    return PORT_OK;
}

int flock_try_acquire(Flock *lock, FlockMode mode)
{
    if (!lock) return PORT_ERR_INVALID_ARG;
    int op = flock_op_for(mode) | LOCK_NB;
    if (flock(lock->fd, op) != 0) {
        if (errno == EWOULDBLOCK) return PORT_ERR_BUSY;
        if (errno == EINTR)       return PORT_ERR_INTERRUPTED;
        return PORT_ERR;
    }
    return PORT_OK;
}

int flock_release(Flock *lock)
{
    if (!lock) return PORT_ERR_INVALID_ARG;
    if (flock(lock->fd, LOCK_UN) != 0) return PORT_ERR;
    return PORT_OK;
}

int flock_close(Flock *lock)
{
    if (!lock) return PORT_OK;               /* tolerate NULL */
    if (lock->fd >= 0) close(lock->fd);      /* close also releases any flock */
    free(lock);
    return PORT_OK;
}

/* ===================================================== Process ====== */

struct Process {
    pid_t pid;
    int   exited;        /* cached: 1 once waitpid has reaped */
};

int spawn(const char *bin_path, char *const argv[], Process **out_proc)
{
    if (!bin_path || !argv || !out_proc) return PORT_ERR_INVALID_ARG;
    *out_proc = NULL;

    Process *p = malloc(sizeof(*p));
    if (!p) return PORT_ERR_NO_MEMORY;
    p->exited = 0;

    if (posix_spawn(&p->pid, bin_path, NULL, NULL, argv, NULL) != 0) {
        free(p);
        return PORT_ERR;
    }

    *out_proc = p;
    return PORT_OK;
}

int process_wait_nonblock(Process *proc, int *out_exited)
{
    if (!proc || !out_exited) return PORT_ERR_INVALID_ARG;
    if (proc->exited) { *out_exited = 1; return PORT_OK; }

    pid_t r = waitpid(proc->pid, NULL, WNOHANG);
    if (r == proc->pid) {
        proc->exited = 1;
        *out_exited  = 1;
        return PORT_OK;
    }
    if (r == 0) {
        *out_exited = 0;
        return PORT_OK;
    }
    /* r == -1: error. ECHILD means already reaped (shouldn't happen here);
     * other errnos are real failures. */
    return PORT_ERR;
}

int process_terminate(Process *proc, int graceful)
{
    if (!proc) return PORT_ERR_INVALID_ARG;
    if (proc->exited) return PORT_OK;
    int sig = graceful ? SIGTERM : SIGKILL;
    if (kill(proc->pid, sig) != 0) {
        if (errno == ESRCH) {                /* process already gone */
            proc->exited = 1;
            return PORT_OK;
        }
        return PORT_ERR;
    }
    return PORT_OK;
}

int process_close(Process *proc)
{
    if (!proc) return PORT_OK;
    free(proc);
    return PORT_OK;
}

int64_t process_pid(const Process *proc)
{
    return proc ? (int64_t)proc->pid : -1;
}

/* ====================================================== filesystem ===== */

int ensure_dir(const char *path)
{
    if (!path) return PORT_ERR_INVALID_ARG;
    if (mkdir(path, 0755) == 0) return PORT_OK;
    if (errno == EEXIST)        return PORT_OK;
    return PORT_ERR;
}
