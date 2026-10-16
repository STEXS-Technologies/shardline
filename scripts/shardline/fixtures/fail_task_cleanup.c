/* Test-only fault: reject removal of a confirmed task's top-level directory. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int task_name(const char *name) {
    return strlen(name) == 64 && strspn(name, "0123456789abcdef") == 64;
}

static int task_path(const char *path, const char *root) {
    size_t length = strlen(root);
    return strncmp(path, root, length) == 0 && path[length] == '/'
        && task_name(path + length + 1);
}

static int fault(void) {
    const char *marker = getenv("SHARDLINE_TEST_CLEANUP_MARKER");
    if (marker) {
        int fd = open(marker, O_WRONLY | O_CREAT | O_APPEND, 0600);
        if (fd >= 0) {
            ssize_t written = write(fd, "cleanup rejected\n", 17);
            (void)written;
            close(fd);
        }
    }
    errno = EIO;
    return -1;
}

int unlinkat(int directory, const char *name, int flags) {
    const char *root = getenv("SHARDLINE_TEST_CLEANUP_ROOT");
    if (root && (flags & AT_REMOVEDIR) && task_path(name, root)) return fault();
    if (root && (flags & AT_REMOVEDIR) && task_name(name)) {
        char descriptor[64], parent[PATH_MAX];
        snprintf(descriptor, sizeof(descriptor), "/proc/self/fd/%d", directory);
        ssize_t length = readlink(descriptor, parent, sizeof(parent) - 1);
        if (length >= 0) {
            parent[length] = '\0';
            if (strcmp(parent, root) == 0) return fault();
        }
    }
    int (*original)(int, const char *, int) = dlsym(RTLD_NEXT, "unlinkat");
    if (!original) { errno = ENOSYS; return -1; }
    return original(directory, name, flags);
}

int rmdir(const char *path) {
    const char *root = getenv("SHARDLINE_TEST_CLEANUP_ROOT");
    if (root && task_path(path, root)) return fault();
    int (*original)(const char *) = dlsym(RTLD_NEXT, "rmdir");
    if (!original) { errno = ENOSYS; return -1; }
    return original(path);
}
