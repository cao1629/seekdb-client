// Two client processes race to spawn a seekdb-server.
//
// Release order is deterministic: client A is released first, reaches
// flock(EX) seekdb.startup before client B, and wins the spawn. Client B
// then wakes up, posix_spawns a second server that loses the flock(EX, NB)
// seekdb.pid race and _exit(0)s. Both clients end up connected to the
// winning server.
//
// Configure with env vars:
//   SEEKDB_BIN      path to the seekdb binary
//   SEEKDB_DB_DIR   path to the test db directory

#include <gtest/gtest.h>

#include "seekdb.h"

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sys/wait.h>
#include <unistd.h>

namespace {

int run_client(const char *bin_path, const char *db_dir, int id, int start_fd)
{
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

    // Hold the connection briefly so an external observer can confirm only
    // one seekdb-server process is alive.
    sleep(2);

    seekdb_disconnect(c);
    seekdb_close(h);
    fprintf(stderr, "client %d: OK\n", id);
    return 0;
}

TEST(SpawnRace, BothClientsSucceed)
{
    const char *bin_path = std::getenv("SEEKDB_BIN");
    const char *db_dir   = std::getenv("SEEKDB_DB_DIR");
    ASSERT_NE(bin_path, nullptr) << "set SEEKDB_BIN to the seekdb binary";
    ASSERT_NE(db_dir,   nullptr) << "set SEEKDB_DB_DIR to the test db dir";

    int start_a[2], start_b[2];
    ASSERT_EQ(pipe(start_a), 0) << std::strerror(errno);
    ASSERT_EQ(pipe(start_b), 0) << std::strerror(errno);

    pid_t pid_a = fork();
    ASSERT_GE(pid_a, 0) << std::strerror(errno);
    if (pid_a == 0) {
        close(start_a[1]); close(start_b[0]); close(start_b[1]);
        _exit(run_client(bin_path, db_dir, 1, start_a[0]));
    }

    pid_t pid_b = fork();
    ASSERT_GE(pid_b, 0) << std::strerror(errno);
    if (pid_b == 0) {
        close(start_b[1]); close(start_a[0]); close(start_a[1]);
        _exit(run_client(bin_path, db_dir, 2, start_b[0]));
    }

    close(start_a[0]);
    close(start_b[0]);

    // Release A first so it reaches flock(EX) seekdb.startup and wins the
    // spawn. Linux flock is FIFO-fair; 50 ms headroom is generous.
    ASSERT_EQ(write(start_a[1], "g", 1), 1);
    usleep(50 * 1000);
    ASSERT_EQ(write(start_b[1], "g", 1), 1);

    close(start_a[1]);
    close(start_b[1]);

    int status_a = 0, status_b = 0;
    ASSERT_EQ(waitpid(pid_a, &status_a, 0), pid_a);
    ASSERT_EQ(waitpid(pid_b, &status_b, 0), pid_b);

    EXPECT_TRUE(WIFEXITED(status_a));
    EXPECT_EQ(WEXITSTATUS(status_a), 0);
    EXPECT_TRUE(WIFEXITED(status_b));
    EXPECT_EQ(WEXITSTATUS(status_b), 0);
}

}  // namespace
