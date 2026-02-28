/*
 * List open file descriptors as a JSON array.
 *
 * Used by fd hygiene tests to verify that child processes see exactly
 * the expected set of fds (no leaks from the executor or pipelines).
 *
 * Reads /proc/self/fd and excludes the opendir() fd itself via dirfd().
 * Unlike Python or shell, a C binary has no interpreter-internal fds,
 * so the output reflects only what the executor actually passed.
 *
 * Exit codes: 0 = success, 1 = can't open /proc/self/fd, 2 = parse error
 * Output: JSON array on stdout, e.g. [0,1,2] or [0,1,2,3,5]
 */

#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

int main(void) {
    DIR *dir = opendir("/proc/self/fd");
    if (!dir) {
        return 1;
    }

    /* dirfd() gives us the fd that opendir itself allocated.
     * We must exclude it — it's our bookkeeping, not the executor's. */
    int dir_fd = dirfd(dir);
    int rc = 0;
    if (dir_fd < 0) {
        rc = 1;
        goto done;
    }
    bool first = true;
    struct dirent *entry;
    printf("[");
    for (;;) {
        errno = 0;
        entry = readdir(dir);
        if (!entry) {
            if (errno != 0) {
                rc = 1;  /* readdir failed */
            }
            break;
        }
        if (entry->d_name[0] == '.') {
            continue;  /* skip . and .. */
        }

        /* Parse entry name as integer fd number.
         * /proc/self/fd entries are kernel-generated and always numeric,
         * but we validate fully: non-numeric, overflow, or negative = error. */
        char *end;
        errno = 0;
        long val = strtol(entry->d_name, &end, 10);
        if (*end != '\0' || errno != 0 || val < 0 || val > INT_MAX) {
            rc = 2;
            break;
        }

        int fd = (int)val;
        if (fd == dir_fd) {
            continue;  /* exclude opendir's own fd */
        }

        /* Emit as JSON array element */
        if (!first) {
            printf(",");
        }
        printf("%d", fd);
        first = false;
    }
    if (rc != 0) {
        goto done;
    }
    printf("]\n");

done:
    closedir(dir);
    return rc;
}
