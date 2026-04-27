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

int flock_open(const char *path, Flock **out_flock)
{
    if (!path || !out_flock) return ERR_INVALID_ARG;
    *out_flock = NULL;

    int fd = open(path, O_CREAT | O_RDWR | O_CLOEXEC, 0644);
    if (fd < 0) return ERR;

    Flock *l = malloc(sizeof(Flock));
    l->fd = fd;
    *out_flock = l;
    return OK;
}

static int flock_op_for(FlockMode mode)
{
    return (mode == FLOCK_SHARED) ? LOCK_SH : LOCK_EX;
}

void flock_acquire(Flock *lock, FlockMode mode)
{
    flock(lock->fd, flock_op_for(mode));
}

int flock_try_acquire(Flock *lock, FlockMode mode)
{
    if (!lock) return ERR_INVALID_ARG;
    if (flock(lock->fd, flock_op_for(mode) | LOCK_NB) != 0) return ERR;
    return OK;
}

void flock_release(Flock *lock)
{
    flock(lock->fd, LOCK_UN);
}

int flock_close(Flock *lock)
{
    if (!lock) return ERR;
    if (lock->fd >= 0) close(lock->fd);      /* close also releases any flock */
    free(lock);
    return OK;
}

/* ===================================================== Process ====== */

struct Process {
    pid_t pid;
};

int spawn(const char *bin_path, char *const argv[], Process **out_proc)
{
    if (!bin_path || !argv || !out_proc) return ERR_INVALID_ARG;
    *out_proc = NULL;

    Process *p = malloc(sizeof(Process));

    if (posix_spawn(&p->pid, bin_path, NULL, NULL, argv, NULL) != 0) {
        free(p);
        return ERR;
    }

    *out_proc = p;
    return OK;
}

int is_spawned_server_running(Process *proc)
{
    pid_t r = waitpid(proc->pid, NULL, WNOHANG);
    return r == 0 ? 1 : 0;
}

int process_close(Process *proc)
{
    if (!proc) return OK;
    free(proc);
    return OK;
}

int64_t process_pid(const Process *proc)
{
    return proc ? (int64_t)proc->pid : -1;
}

int is_server_reaped(int64_t pid)
{
    if (pid <= 0) return 1;
    return kill((pid_t)pid, 0) == 0 ? 0 : 1;
}

int terminate_process(int64_t pid, int graceful)
{
    if (pid <= 0) return ERR_INVALID_ARG;
    int sig = graceful ? SIGTERM : SIGKILL;
    if (kill((pid_t)pid, sig) != 0) {
        if (errno == ESRCH) return OK;       /* already gone */
        return ERR;
    }
    return OK;
}

/* ====================================================== filesystem ===== */

int ensure_dir(const char *path)
{
    if (!path) return ERR_INVALID_ARG;
    if (mkdir(path, 0755) == 0) return OK;
    if (errno == EEXIST)        return OK;
    return ERR;
}
