/*
 * port_win32.c — Win32 backend for src/port.h.
 *
 * Implements file locks (via LockFileEx / UnlockFileEx), processes
 * (via CreateProcess / WaitForSingleObject / TerminateProcess), and
 * ensure_dir (via _mkdir).
 *
 * Notes:
 *   - The `graceful` flag on process_terminate is ignored — Windows
 *     TerminateProcess is always a hard kill. Implementing graceful
 *     would require Ctrl+C event delivery (console children only) or
 *     WM_CLOSE (GUI children only); neither fits a generic daemon.
 *   - Locks cover the entire file (offset 0, length 2^64-1), matching
 *     POSIX flock's whole-file semantics.
 */

#include "port.h"

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

int flock_open(const char *path, Flock **out_lock)
{
    if (!path || !out_lock) return PORT_ERR_INVALID_ARG;
    *out_lock = NULL;

    HANDLE h = CreateFileA(path,
                           GENERIC_READ | GENERIC_WRITE,
                           FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                           NULL,
                           OPEN_ALWAYS,
                           FILE_ATTRIBUTE_NORMAL,
                           NULL);
    if (h == INVALID_HANDLE_VALUE) return PORT_ERR;

    Flock *l = (Flock *)malloc(sizeof(*l));
    if (!l) { CloseHandle(h); return PORT_ERR_NO_MEMORY; }
    l->handle = h;
    l->held   = 0;
    *out_lock = l;
    return PORT_OK;
}

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
        DWORD err = GetLastError();
        if (err == ERROR_LOCK_VIOLATION || err == ERROR_IO_PENDING)
            return PORT_ERR_BUSY;
        return PORT_ERR;
    }
    lock->held = 1;
    return PORT_OK;
}

int flock_acquire(Flock *lock, FlockMode mode)
{
    if (!lock) return PORT_ERR_INVALID_ARG;
    return do_lock(lock, mode, /*blocking=*/1);
}

int flock_try_acquire(Flock *lock, FlockMode mode)
{
    if (!lock) return PORT_ERR_INVALID_ARG;
    return do_lock(lock, mode, /*blocking=*/0);
}

int flock_release(Flock *lock)
{
    if (!lock) return PORT_ERR_INVALID_ARG;
    if (lock->held) {
        OVERLAPPED ov;
        memset(&ov, 0, sizeof(ov));
        if (!UnlockFileEx(lock->handle, 0, MAXDWORD, MAXDWORD, &ov))
            return PORT_ERR;
        lock->held = 0;
    }
    return PORT_OK;
}

int flock_close(Flock *lock)
{
    if (!lock) return PORT_OK;
    if (lock->held) {
        OVERLAPPED ov;
        memset(&ov, 0, sizeof(ov));
        UnlockFileEx(lock->handle, 0, MAXDWORD, MAXDWORD, &ov);
    }
    if (lock->handle != INVALID_HANDLE_VALUE) CloseHandle(lock->handle);
    free(lock);
    return PORT_OK;
}

/* ===================================================== Process ====== */

struct Process {
    HANDLE handle;
    DWORD  pid;
    int    exited;
};

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

int spawn(const char *bin_path, char *const argv[], Process **out_proc)
{
    if (!bin_path || !argv || !out_proc) return PORT_ERR_INVALID_ARG;
    *out_proc = NULL;

    char *cmd = build_cmdline(argv);
    if (!cmd) return PORT_ERR_NO_MEMORY;

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
    if (!ok) return PORT_ERR;

    /* We don't need the primary thread handle. */
    CloseHandle(pi.hThread);

    Process *p = (Process *)malloc(sizeof(*p));
    if (!p) {
        TerminateProcess(pi.hProcess, 1);
        CloseHandle(pi.hProcess);
        return PORT_ERR_NO_MEMORY;
    }
    p->handle = pi.hProcess;
    p->pid    = pi.dwProcessId;
    p->exited = 0;
    *out_proc = p;
    return PORT_OK;
}

int process_wait_nonblock(Process *proc, int *out_exited)
{
    if (!proc || !out_exited) return PORT_ERR_INVALID_ARG;
    if (proc->exited) { *out_exited = 1; return PORT_OK; }

    DWORD r = WaitForSingleObject(proc->handle, 0);
    if (r == WAIT_OBJECT_0) {
        proc->exited = 1;
        *out_exited  = 1;
        return PORT_OK;
    }
    if (r == WAIT_TIMEOUT) {
        *out_exited = 0;
        return PORT_OK;
    }
    return PORT_ERR;
}

int process_terminate(Process *proc, int graceful)
{
    (void)graceful;   /* Windows: always hard-kill */
    if (!proc) return PORT_ERR_INVALID_ARG;
    if (proc->exited) return PORT_OK;
    if (!TerminateProcess(proc->handle, 1)) {
        if (GetLastError() == ERROR_ACCESS_DENIED) {
            /* Already gone — good as terminated. */
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
    if (proc->handle != NULL) CloseHandle(proc->handle);
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
    if (_mkdir(path) == 0) return PORT_OK;
    if (errno == EEXIST)   return PORT_OK;
    return PORT_ERR;
}
