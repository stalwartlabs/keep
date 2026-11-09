/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdint.h>
#include <sys/socket.h>
#include <unistd.h>

#define ECHO_BUF 65536

static void *echo_conn(void *arg) {
    int fd = (int)(intptr_t)arg;
    char buf[ECHO_BUF];
    for (;;) {
        ssize_t n = read(fd, buf, sizeof buf);
        if (n == 0) {
            break;
        }
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        ssize_t off = 0;
        while (off < n) {
            ssize_t w = write(fd, buf + off, (size_t)(n - off));
            if (w < 0) {
                if (errno == EINTR) {
                    continue;
                }
                close(fd);
                return NULL;
            }
            off += w;
        }
    }
    close(fd);
    return NULL;
}

int netprobe_c_echo_serve(int listen_fd) {
    for (;;) {
        int fd = accept(listen_fd, NULL, NULL);
        if (fd < 0) {
            if (errno == EINTR || errno == ECONNABORTED) {
                continue;
            }
            return errno;
        }
        int one = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
        pthread_t thread;
        if (pthread_create(&thread, NULL, echo_conn, (void *)(intptr_t)fd) != 0) {
            close(fd);
            continue;
        }
        pthread_detach(thread);
    }
}
