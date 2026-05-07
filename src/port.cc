/*
 * port.cc — POSIX and Win32 backends for src/port.h.
 *
 * Whole file is conditionally compiled per platform: Win32 uses
 * LockFileEx / CreateProcess / TerminateProcess; POSIX uses flock /
 * posix_spawn / waitpid / kill.
 *
 * Notes (Win32):
 *   - The `graceful` flag on terminate_process is ignored — Windows
 *     TerminateProcess is always a hard kill. Implementing graceful
 *     would require Ctrl+C event delivery (console children only) or
 *     WM_CLOSE (GUI children only); neither fits a generic daemon.
 *   - Locks cover the entire file (offset 0, length 2^64-1), matching
 *     POSIX flock's whole-file semantics.
 */

#include "port.h"
#include "tlog.h"

#ifdef _WIN32

#include <direct.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <windows.h>

/* ============================================================ Flock ===== */

struct Flock {
    HANDLE handle;
    int    held;          /* 1 if a lock is currently held on this handle */
};

int flock_open(const char *path, Flock **out_flock)
{
    if (!path || !out_flock) return ERR_INVALID_ARG;
    *out_flock = NULL;

    HANDLE h = CreateFileA(path,
                           GENERIC_READ | GENERIC_WRITE,
                           FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                           NULL,
                           OPEN_ALWAYS,
                           FILE_ATTRIBUTE_NORMAL,
                           NULL);
    if (h == INVALID_HANDLE_VALUE) return ERR;

    Flock *l = (Flock *)malloc(sizeof(Flock));
    l->handle = h;
    l->held   = 0;
    *out_flock = l;
    return OK;
}

/* Returns 1 if the lock was acquired, 0 otherwise. On a blocking call,
 * logs the OS error if LockFileEx returned FALSE (try-acquire stays
 * silent — failure there is the expected "couldn't get it" path). */
static int do_lock(Flock *lock, FlockMode mode, int blocking)
{
    DWORD flags = (mode == FLOCK_EXCLUSIVE) ? LOCKFILE_EXCLUSIVE_LOCK : 0;
    if (!blocking) flags |= LOCKFILE_FAIL_IMMEDIATELY;

    OVERLAPPED ov;
    memset(&ov, 0, sizeof(ov));
    if (!LockFileEx(lock->handle, flags, 0,
                    /* lock the entire file */
                    MAXDWORD, MAXDWORD,
                    &ov)) {
        if (blocking) {
            DWORD err = GetLastError();
            char errmsg[256];
            DWORD n = FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM
                                     | FORMAT_MESSAGE_IGNORE_INSERTS,
                                     NULL, err, 0,
                                     errmsg, sizeof(errmsg), NULL);
            if (n == 0) errmsg[0] = '\0';
            tlog("LockFileEx(mode=%s) failed: error %lu: %s\n",
                 (mode == FLOCK_EXCLUSIVE) ? "EX" : "SH",
                 (unsigned long)err, errmsg);
        }
        return 0;
    }
    lock->held = 1;
    return 1;
}

int flock_acquire(Flock *lock, FlockMode mode)
{
    int rc = do_lock(lock, mode, /*blocking=*/1);
    if (rc) {
        tlog("flock_acquire(mode=%s) ok: handle=%p\n",
             (mode == FLOCK_EXCLUSIVE) ? "EX" : "SH",
             (void *)lock->handle);
    }
    return rc;
}


int flock_try_acquire(Flock *lock, FlockMode mode)
{
    return do_lock(lock, mode, /*blocking=*/0);
}

void flock_release(Flock *lock)
{
    if (lock->held) {
        OVERLAPPED ov;
        memset(&ov, 0, sizeof(ov));
        UnlockFileEx(lock->handle, 0, MAXDWORD, MAXDWORD, &ov);
        lock->held = 0;
    }
}

int flock_close(Flock *lock)
{
    if (!lock) return ERR;
    if (lock->held) {
        OVERLAPPED ov;
        memset(&ov, 0, sizeof(ov));
        UnlockFileEx(lock->handle, 0, MAXDWORD, MAXDWORD, &ov);
    }
    if (lock->handle != INVALID_HANDLE_VALUE) CloseHandle(lock->handle);
    free(lock);
    return OK;
}

/* ===================================================== Process ====== */

/*
 * Build a Windows command line from argv. CreateProcess wants a single
 * mutable string; this is the simplest reliable encoding (each arg
 * wrapped in double quotes, no embedded-quote handling — sufficient
 * for the seekdb daemon's known-clean argv).
 */
static char *build_cmdline(char *const argv[])
{
    size_t total = 0;
    for (size_t i = 0; argv[i]; i++)
        total += strlen(argv[i]) + 3;          /* quotes + space */
    char *cmd = (char *)malloc(total + 1);
    if (!cmd) return NULL;
    char *p = cmd;
    for (size_t i = 0; argv[i]; i++) {
        if (i > 0) *p++ = ' ';
        *p++ = '"';
        size_t n = strlen(argv[i]);
        memcpy(p, argv[i], n);
        p += n;
        *p++ = '"';
    }
    *p = '\0';
    return cmd;
}

int spawn_process(const char *bin_path, char *const argv[], Process **out_proc)
{
    if (!bin_path || !argv || !out_proc) return ERR_INVALID_ARG;
    *out_proc = NULL;

    /* Confirm the binary actually exists; CreateProcessA's "file not
       found" failure mode looks identical to other errors otherwise. */
    DWORD attr = GetFileAttributesA(bin_path);
    if (attr == INVALID_FILE_ATTRIBUTES) {
        tlog("spawn_process: bin_path '%s' not accessible: GetFileAttributesA error %lu\n",
             bin_path, (unsigned long)GetLastError());
    } else if (attr & FILE_ATTRIBUTE_DIRECTORY) {
        tlog("spawn_process: bin_path '%s' is a directory, not an executable\n",
             bin_path);
    }

    char *cmd = build_cmdline(argv);
    if (!cmd) return ERR;

    tlog("spawn_process: bin='%s'\n", bin_path);

    STARTUPINFOA si;
    PROCESS_INFORMATION pi;
    memset(&si, 0, sizeof(si));
    memset(&pi, 0, sizeof(pi));
    si.cb = sizeof(si);

    BOOL ok = CreateProcessA(bin_path,
                             cmd,
                             NULL, NULL,
                             FALSE,           /* don't inherit handles */
                             0,
                             NULL, NULL,
                             &si, &pi);
    free(cmd);
    if (!ok) {
        DWORD err = GetLastError();
        char errmsg[256];
        DWORD n = FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM
                                 | FORMAT_MESSAGE_IGNORE_INSERTS,
                                 NULL, err, 0,
                                 errmsg, sizeof(errmsg), NULL);
        if (n == 0) errmsg[0] = '\0';
        char cwd[MAX_PATH];
        DWORD cwdn = GetCurrentDirectoryA(MAX_PATH, cwd);
        tlog("spawn_process: CreateProcessA(%s) failed: error %lu: %s (cwd=%s)\n",
             bin_path, (unsigned long)err, errmsg,
             (cwdn > 0 && cwdn < MAX_PATH) ? cwd : "?");
        return ERR;
    }

    tlog("spawn_process: spawned pid=%lu\n",
         (unsigned long)pi.dwProcessId);

    /* We don't need the primary thread handle. */
    CloseHandle(pi.hThread);

    Process *p = (Process *)malloc(sizeof(*p));
    p->handle = pi.hProcess;                /* HANDLE stored as void* */
    p->pid    = (int64_t)pi.dwProcessId;
    *out_proc = p;
    return OK;
}

int reap_process(Process *proc)
{
    HANDLE h = (HANDLE)proc->handle;
    if (h == NULL) return 1;                       /* already reaped */
    DWORD r = WaitForSingleObject(h, 0);
    if (r == WAIT_TIMEOUT) return -1;              /* still running */
    CloseHandle(h);                                /* the reap */
    proc->handle = NULL;                           /* guard against double-close */
    return 1;
}

int is_server_reaped(int64_t pid)
{
    if (pid <= 0) return 1;
    HANDLE h = OpenProcess(SYNCHRONIZE, FALSE, (DWORD)pid);
    if (h == NULL) return 1;
    DWORD r = WaitForSingleObject(h, 0);
    CloseHandle(h);
    return (r == WAIT_TIMEOUT) ? 0 : 1;
}

int terminate_process(int64_t pid, int graceful)
{
    (void)graceful;   /* Windows: always hard-kill */
    if (pid <= 0) return ERR_INVALID_ARG;
    HANDLE h = OpenProcess(PROCESS_TERMINATE, FALSE, (DWORD)pid);
    if (h == NULL) return OK;             /* already gone (or no permission) */
    BOOL ok = TerminateProcess(h, 1);
    CloseHandle(h);
    return ok ? OK : ERR;
}

/* ====================================================== filesystem ===== */

int ensure_dir(const char *path)
{
    if (!path) return ERR_INVALID_ARG;
    if (_mkdir(path) == 0) return OK;
    if (errno == EEXIST)   return OK;
    return ERR;
}

#else /* POSIX */

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <spawn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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

    Flock *l = (Flock *)malloc(sizeof(Flock));
    l->fd = fd;
    *out_flock = l;
    return OK;
}

static int flock_op_for(FlockMode mode)
{
    return (mode == FLOCK_SHARED) ? LOCK_SH : LOCK_EX;
}

int flock_acquire(Flock *lock, FlockMode mode)
{
    if (flock(lock->fd, flock_op_for(mode)) != 0) {
        tlog("flock(mode=%s) failed: errno %d: %s\n",
             (mode == FLOCK_EXCLUSIVE) ? "EX" : "SH",
             errno, strerror(errno));
        return 0;
    }
    tlog("flock_acquire(mode=%s) ok: fd=%d\n",
         (mode == FLOCK_EXCLUSIVE) ? "EX" : "SH",
         lock->fd);
    return 1;
}


int flock_try_acquire(Flock *lock, FlockMode mode)
{
    return flock(lock->fd, flock_op_for(mode) | LOCK_NB) == 0 ? 1 : 0;
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

int spawn_process(const char *bin_path, char *const argv[], Process **out_proc)
{
    if (!bin_path || !argv || !out_proc) return ERR_INVALID_ARG;
    *out_proc = NULL;

    Process *p = (Process *)malloc(sizeof(Process));
    pid_t pid;
    int err = posix_spawn(&pid, bin_path, NULL, NULL, argv, NULL);
    if (err != 0) {
        tlog("spawn_process: posix_spawn(%s) failed: errno %d: %s\n",
             bin_path, err, strerror(err));
        free(p);
        return ERR;
    }
    p->pid = (int64_t)pid;
    *out_proc = p;
    return OK;
}

int reap_process(Process *proc)
{
    if (!proc) return 1;                           /* already reaped + freed */
    pid_t r = waitpid((pid_t)proc->pid, NULL, WNOHANG);
    return r == 0 ? -1 : 1;                        /* -1 still running, 1 reaped */
}

int is_server_reaped(int64_t pid) {
    printf("is_server_reaped: pid = %d\n", pid);
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

#endif
