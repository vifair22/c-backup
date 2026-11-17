/*
 * A stand-in qemu-guest-agent, run as /init of a tiny initramfs booted with
 * the host's kernel, for the libvirt suites (test_virt_offline). It lives
 * under fake_host/, not tests/, because every tests/*.c is a suite. It gives a
 * guest with no OS a real agent on the virtio port libvirt talks to, whose
 * freeze can be made to hang like a slow fsfreeze hook: qga_freeze_s=N on
 * the kernel command line sleeps N seconds inside the first
 * guest-fsfreeze-freeze; later ones are immediate.
 *
 * Every freeze and thaw is written to the console (the domain's serial
 * file), so a test can see what the guest went through:
 *   fake-qga: ready | freezing | frozen | thawed <n>
 *
 * Built static by the suite at run time; it never exits (it is PID 1).
 */
#define _GNU_SOURCE
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mount.h>
#include <time.h>
#include <unistd.h>

static void say(const char *text)
{
    (void)!write(1, text, strlen(text));
}

static void say_n(const char *what, int n)
{
    char line[96];
    int len = snprintf(line, sizeof(line), "fake-qga: %s %d\n", what, n);
    if (len > 0) (void)!write(1, line, (size_t)len);
}

static void nap_ms(long ms)
{
    struct timespec ts = { ms / 1000, (ms % 1000) * 1000000L };
    while (nanosleep(&ts, &ts) != 0 && errno == EINTR) {}
}

static int freeze_delay(void)
{
    char buf[1024] = "";
    int fd = open("/proc/cmdline", O_RDONLY);
    if (fd < 0) return 0;
    ssize_t n = read(fd, buf, sizeof(buf) - 1);
    close(fd);
    if (n <= 0) return 0;
    buf[n] = '\0';
    const char *p = strstr(buf, "qga_freeze_s=");
    return p ? atoi(p + 13) : 0;
}

/* The port named org.qemu.guest_agent.0, by its sysfs name. */
static int open_port(void)
{
    for (int tries = 0; tries < 600; tries++) {
        DIR *d = opendir("/sys/class/virtio-ports");
        struct dirent *e;
        while (d && (e = readdir(d))) {
            if (e->d_name[0] == '.') continue;
            char p[300], name[128] = "";
            snprintf(p, sizeof(p), "/sys/class/virtio-ports/%s/name", e->d_name);
            int fd = open(p, O_RDONLY);
            if (fd < 0) continue;
            ssize_t n = read(fd, name, sizeof(name) - 1);
            close(fd);
            if (n > 0 && strncmp(name, "org.qemu.guest_agent.0", 22) == 0) {
                snprintf(p, sizeof(p), "/dev/%s", e->d_name);
                int pfd = open(p, O_RDWR);
                if (pfd >= 0) { closedir(d); return pfd; }
            }
        }
        if (d) closedir(d);
        nap_ms(100);
    }
    return -1;
}

static void reply(int fd, const char *text)
{
    size_t len = strlen(text);
    while (len) {
        ssize_t w = write(fd, text, len);
        if (w < 0) { if (errno == EINTR || errno == EAGAIN) { nap_ms(10); continue; } return; }
        text += w;
        len -= (size_t)w;
    }
}

static long arg_id(const char *cmd)
{
    const char *p = strstr(cmd, "\"id\"");
    if (!p) return 0;
    p = strchr(p, ':');
    return p ? strtol(p + 1, NULL, 10) : 0;
}

int main(void)
{
    mount("devtmpfs", "/dev", "devtmpfs", 0, NULL);
    mount("proc", "/proc", "proc", 0, NULL);
    mount("sysfs", "/sys", "sysfs", 0, NULL);
    int con = open("/dev/console", O_WRONLY);
    if (con >= 0) { dup2(con, 1); dup2(con, 2); }
    int delay = freeze_delay();
    say("fake-qga: booted\n");
    int fd = open_port();
    if (fd < 0) { say("fake-qga: no agent port\n"); for (;;) pause(); }
    say_n("ready, first freeze takes (s)", delay);

    char buf[8192];
    size_t have = 0;
    int frozen = 0;
    for (;;) {
        ssize_t n = read(fd, buf + have, sizeof(buf) - 1 - have);
        if (n <= 0) { nap_ms(50); continue; }      /* no host side yet, or it went away */
        have += (size_t)n;
        buf[have] = '\0';
        /* One command is one balanced JSON object; 0xFF and blanks between. */
        for (;;) {
            size_t s = 0;
            while (s < have && buf[s] != '{') s++;
            int depth = 0;
            size_t e = s;
            for (; e < have; e++) {
                if (buf[e] == '{') depth++;
                else if (buf[e] == '}' && --depth == 0) break;
            }
            if (s >= have || e >= have) { if (s) { memmove(buf, buf + s, have - s); have -= s; } break; }
            char cmd[4096];
            size_t cl = e - s + 1 < sizeof(cmd) ? e - s + 1 : sizeof(cmd) - 1;
            memcpy(cmd, buf + s, cl);
            cmd[cl] = '\0';
            memmove(buf, buf + e + 1, have - e - 1);
            have -= e + 1;

            char out[256];
            if (strstr(cmd, "\"guest-sync-delimited\"")) {
                snprintf(out, sizeof(out), "\xff{\"return\": %ld}\n", arg_id(cmd));
            } else if (strstr(cmd, "\"guest-sync\"")) {
                snprintf(out, sizeof(out), "{\"return\": %ld}\n", arg_id(cmd));
            } else if (strstr(cmd, "\"guest-ping\"")) {
                snprintf(out, sizeof(out), "{\"return\": {}}\n");
            } else if (strstr(cmd, "\"guest-fsfreeze-freeze")) {
                say("fake-qga: freezing\n");
                for (int i = 0; i < delay * 10; i++) nap_ms(100);
                delay = 0;
                frozen = 1;
                say("fake-qga: frozen\n");
                snprintf(out, sizeof(out), "{\"return\": 1}\n");
            } else if (strstr(cmd, "\"guest-fsfreeze-thaw\"")) {
                int was = frozen;
                frozen = 0;
                say_n("thawed", was);
                snprintf(out, sizeof(out), "{\"return\": %d}\n", was);
            } else if (strstr(cmd, "\"guest-fsfreeze-status\"")) {
                snprintf(out, sizeof(out), "{\"return\": \"%s\"}\n", frozen ? "frozen" : "thawed");
            } else if (strstr(cmd, "\"guest-info\"")) {
                snprintf(out, sizeof(out), "{\"return\": {\"version\": \"fake\", \"supported_commands\": []}}\n");
            } else {
                snprintf(out, sizeof(out), "{\"error\": {\"class\": \"CommandNotFound\", \"desc\": \"fake\"}}\n");
            }
            reply(fd, out);
        }
    }
}
