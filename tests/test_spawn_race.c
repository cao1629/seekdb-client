/*
 * Two client processes race to spawn a seekdb-server.
 *
 * Release order is deterministic: client A is released first, reaches
 * flock(EX) seekdb.startup before client B does, and therefore wins the
 * spawn. Client B then wakes up, posix_spawns a second server which loses
 * the flock(EX, NB) seekdb.pid race and _exit(0)s. Both clients end up
 * connected to the winning server.
 *
 * Build:  cc tests/test_spawn_race.c -I include -L build -lseekdb_client
 * Usage:  ./test_spawn_race <path-to-seekdb-daemon> <db_dir>
 */

#include "seekdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

static int run_client(const char *bin_path, const char *db_dir,
                      int id, int start_fd)
{
    /* Block until the parent releases us via the start pipe. */
    char b;
    if (read(start_fd, &b, 1) != 1) return 1;
    close(start_fd);

    SeekdbHandle h = NULL;
    int rc = seekdb_open(bin_path, db_dir, 0, &h);
    if (rc != SEEKDB_SUCCESS) {
        fprintf(stderr, "client %d: seekdb_open failed: rc=%d\n", id, rc);
        return 1;
    }

    SeekdbConnection c = NULL;
    if (seekdb_connect(h, NULL, true, &c) != SEEKDB_SUCCESS) {
        fprintf(stderr, "client %d: seekdb_connect failed\n", id);
        seekdb_close(h);
        return 1;
    }

    SeekdbResult r = NULL;
    if (seekdb_query(c, "SELECT 1", 8, &r) != SEEKDB_SUCCESS) {
        fprintf(stderr, "client %d: query failed\n", id);
        seekdb_disconnect(c);
        seekdb_close(h);
        return 1;
    }
    if (r) seekdb_result_free(r);

    /* Hold the connection for a moment so an external observer (e.g. pgrep)
     * can confirm that only one seekdb-server process is alive. */
    sleep(2);

    seekdb_disconnect(c);
    seekdb_close(h);
    fprintf(stderr, "client %d: OK\n", id);
    return 0;
}

int main(int argc, char **argv)
{
    if (argc != 3) {
        fprintf(stderr, "usage: %s <seekdb-daemon> <db_dir>\n", argv[0]);
        return 2;
    }
    const char *bin_path = argv[1];
    const char *db_dir   = argv[2];

    /* One start-pipe per child so the parent can release them independently. */
    int start_a[2], start_b[2];
    if (pipe(start_a) != 0 || pipe(start_b) != 0) { perror("pipe"); return 2; }

    pid_t pid_a = fork();
    if (pid_a < 0) { perror("fork A"); return 2; }
    if (pid_a == 0) {
        close(start_a[1]); close(start_b[0]); close(start_b[1]);
        _exit(run_client(bin_path, db_dir, 1, start_a[0]));
    }

    pid_t pid_b = fork();
    if (pid_b < 0) { perror("fork B"); return 2; }
    if (pid_b == 0) {
        close(start_b[1]); close(start_a[0]); close(start_a[1]);
        _exit(run_client(bin_path, db_dir, 2, start_b[0]));
    }

    close(start_a[0]); close(start_b[0]);

    /* Release A first — A will take flock(EX) seekdb.startup and win. */
    if (write(start_a[1], "g", 1) != 1) { perror("write A"); return 2; }

    /* Headroom for A to reach flock() before B is released. Modern Linux
     * flock is FIFO-fair, so the first waiter wins; 50 ms is generous. */
    usleep(50 * 1000);

    /* Release B — B blocks on seekdb.startup until A finishes its spawn,
     * then spawns its own server which loses the seekdb.pid race. */
    if (write(start_b[1], "g", 1) != 1) { perror("write B"); return 2; }

    close(start_a[1]); close(start_b[1]);

    int status_a = 0, status_b = 0;
    waitpid(pid_a, &status_a, 0);
    waitpid(pid_b, &status_b, 0);

    int failed = 0;
    if (!WIFEXITED(status_a) || WEXITSTATUS(status_a) != 0) {
        fprintf(stderr, "client A failed: status=%d\n", status_a);
        failed++;
    }
    if (!WIFEXITED(status_b) || WEXITSTATUS(status_b) != 0) {
        fprintf(stderr, "client B failed: status=%d\n", status_b);
        failed++;
    }
    return failed > 0 ? 1 : 0;
}
