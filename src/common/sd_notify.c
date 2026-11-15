#define _GNU_SOURCE  /* SOCK_CLOEXEC, MSG_NOSIGNAL */

#include "sd_notify.h"

#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

status_t sd_notify_state(const char *state)
{
    const char *addr = getenv("NOTIFY_SOCKET");
    if (!addr || !*addr) return OK;
    if (!state) return set_error(ERR_INVALID, "sd_notify: no state to send");

    /* systemd also defines "vsock:CID:PORT" for VMs. Nothing here runs that
     * way, and a misread address must not become a datagram to some
     * unrelated relative path. */
    if (addr[0] != '/' && addr[0] != '@')
        return set_error(ERR_INVALID, "NOTIFY_SOCKET=%s is not an AF_UNIX address", addr);

    struct sockaddr_un sa;
    memset(&sa, 0, sizeof(sa));
    sa.sun_family = AF_UNIX;
    size_t len = strlen(addr);
    if (len >= sizeof(sa.sun_path))
        return set_error(ERR_INVALID, "NOTIFY_SOCKET is too long (%zu bytes)", len);
    memcpy(sa.sun_path, addr, len);
    /* An abstract name is a leading NUL and then exactly its bytes: the
     * length passed to sendto delimits it, and a trailing NUL would be part
     * of the name. A path is NUL-terminated by the memset either way. */
    if (addr[0] == '@') sa.sun_path[0] = '\0';
    socklen_t salen = (socklen_t)(offsetof(struct sockaddr_un, sun_path) + len +
                                  (addr[0] == '@' ? 0 : 1));

    int fd = socket(AF_UNIX, SOCK_DGRAM | SOCK_CLOEXEC, 0);
    if (fd < 0) return set_error_errno(ERR_IO, "sd_notify: socket");

    size_t n = strlen(state);
    ssize_t w = sendto(fd, state, n, MSG_NOSIGNAL, (const struct sockaddr *)&sa, salen);
    if (w < 0 || (size_t)w != n) {
        status_t st = set_error_errno(ERR_IO, "sd_notify: send to %s", addr);
        close(fd);
        return st;
    }
    close(fd);
    return OK;
}
