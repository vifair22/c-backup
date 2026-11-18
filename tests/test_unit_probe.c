#define _GNU_SOURCE

/*
 * units.probe (db-detection.md 8.1): the wire codec's round trip, its
 * refusals and count bounds, the budget a helper answer is cut to; the
 * /proc reader against a synthetic tree (cgroup membership, comm and
 * args, LISTEN rows, fdinfo flags, mounts that are and are not the
 * unit's) and against this process's own /proc; and the fake provider's
 * spec probe and injected failure.
 */

#include <fcntl.h>
#include <limits.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cmocka.h>

#include "fake_provider.h"
#include "proc_probe.h"
#include "test_tmp.h"
#include "unit_probe.h"

#define D    TEST_TMP "/c_backup_test_unit_probe"
#define PROC D "/proc"

static void rmrf(const char *p)
{
    char cmd[PATH_MAX + 16];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static void mkdirs(const char *p)
{
    char cmd[PATH_MAX + 16];
    snprintf(cmd, sizeof(cmd), "mkdir -p '%s'", p);
    assert_int_equal(system(cmd), 0);
}

static void put(const char *path, const char *data, size_t len)
{
    FILE *f = fopen(path, "wb");
    assert_non_null(f);
    assert_int_equal(fwrite(data, 1, len, f), len);
    fclose(f);
}

static void puts_(const char *path, const char *text) { put(path, text, strlen(text)); }

static cJSON *parse(const char *t)
{
    cJSON *o = cJSON_Parse(t);
    assert_non_null(o);
    return o;
}

/* from_json of text: the status, and the probe on OK. */
static status_t from_text(const char *t, sp_unit_probe_t **out)
{
    cJSON *o = parse(t);
    status_t st = sp_unit_probe_from_json(o, out);
    cJSON_Delete(o);
    return st;
}

static int has_open(const sp_probe_container_t *c, const char *path)
{
    for (size_t i = 0; i < c->n_open; i++) if (strcmp(c->open[i].path, path) == 0) return 1;
    return 0;
}

static int has_proc(const sp_probe_container_t *c, const char *comm, const char *args)
{
    for (size_t i = 0; i < c->n_procs; i++)
        if (strcmp(c->procs[i].comm, comm) == 0 && (!args || strcmp(c->procs[i].args, args) == 0)) return 1;
    return 0;
}

/* ------------------------------------------------------------------ */
/* Codec                                                               */
/* ------------------------------------------------------------------ */

static const char *k_full =
    "{\"containers\":["
    "{\"name\":\"pg\",\"image\":\"postgres:16\",\"running\":true,\"aliases\":[\"pg\",\"db\",\"pg\"],"
    "\"processes\":[{\"comm\":\"postgres\",\"args\":\"postgres -c x=1\"},{\"comm\":\"sh\",\"args\":\"\"}],"
    "\"ports\":[5432,8080,5432],"
    "\"open_write\":[{\"path\":\"/mnt/cache/appdata/pg/base/1/2619\",\"comm\":\"postgres\"}]},"
    "{\"name\":\"web\",\"running\":false}"
    "],\"truncated\":false}";

static void test_round_trip(void **state)
{
    (void)state;
    sp_unit_probe_t *p = NULL;
    assert_int_equal(from_text(k_full, &p), OK);
    assert_int_equal(p->n, 2);
    assert_false(p->truncated);
    const sp_probe_container_t *c = &p->v[0];
    assert_string_equal(c->name, "pg");
    assert_string_equal(c->image, "postgres:16");
    assert_true(c->running);
    assert_int_equal(c->n_aliases, 2);          /* the repeat dropped */
    assert_string_equal(c->aliases[1], "db");
    assert_int_equal(c->n_procs, 2);
    assert_string_equal(c->procs[0].args, "postgres -c x=1");
    assert_int_equal(c->n_ports, 2);
    assert_int_equal(c->ports[1], 8080);
    assert_int_equal(c->n_open, 1);
    assert_string_equal(c->open[0].comm, "postgres");
    /* Optional keys absent: empty. */
    assert_string_equal(p->v[1].image, "");
    assert_false(p->v[1].running);
    assert_int_equal(p->v[1].n_procs + p->v[1].n_ports + p->v[1].n_open + p->v[1].n_aliases, 0);

    cJSON *o = sp_unit_probe_to_json(p, 65536);
    assert_non_null(o);
    char *t = cJSON_PrintUnformatted(o);
    cJSON_Delete(o);
    sp_unit_probe_t *q = NULL;
    assert_int_equal(from_text(t, &q), OK);
    char *t2 = NULL;
    o = sp_unit_probe_to_json(q, 65536);
    t2 = cJSON_PrintUnformatted(o);
    cJSON_Delete(o);
    assert_string_equal(t, t2);                 /* a fixed point */
    assert_non_null(strstr(t, "\"truncated\":false"));
    cJSON_free(t);
    cJSON_free(t2);
    sp_unit_probe_free(q);
    sp_unit_probe_free(p);

    /* An empty probe is a valid answer. */
    p = sp_unit_probe_new();
    o = sp_unit_probe_to_json(p, 65536);
    t = cJSON_PrintUnformatted(o);
    assert_string_equal(t, "{\"containers\":[],\"truncated\":false}");
    cJSON_free(t);
    cJSON_Delete(o);
    sp_unit_probe_free(p);
    sp_unit_probe_free(NULL);
}

static void test_refusals(void **state)
{
    (void)state;
    char long_name[200], long_image[300], long_comm[40];
    memset(long_name, 'a', 128); long_name[128] = '\0';
    memset(long_image, 'i', 256); long_image[256] = '\0';
    memset(long_comm, 'c', 16); long_comm[16] = '\0';
    char b1[512], b2[512], b3[512];
    snprintf(b1, sizeof(b1), "{\"containers\":[{\"name\":\"%s\",\"running\":true}]}", long_name);
    snprintf(b2, sizeof(b2), "{\"containers\":[{\"name\":\"x\",\"image\":\"%s\",\"running\":true}]}", long_image);
    snprintf(b3, sizeof(b3), "{\"containers\":[{\"name\":\"x\",\"running\":true,\"processes\":[{\"comm\":\"%s\"}]}]}", long_comm);
    const struct { const char *doc; const char *field; } k[] = {
        { "[]", "must be an object" },
        { "{}", "containers" },
        { "{\"containers\":{}}", "containers" },
        { "{\"containers\":[],\"truncated\":\"no\"}", "truncated" },
        { "{\"containers\":[7]}", "containers[0]" },
        { "{\"containers\":[{\"running\":true}]}", "containers[0].name" },
        { "{\"containers\":[{\"name\":\"\",\"running\":true}]}", "containers[0].name" },
        { "{\"containers\":[{\"name\":\"a\\u0001b\",\"running\":true}]}", "containers[0].name" },
        { "{\"containers\":[{\"name\":\"x\"}]}", "containers[0].running" },
        { "{\"containers\":[{\"name\":\"x\",\"running\":1}]}", "containers[0].running" },
        { "{\"containers\":[{\"name\":\"x\",\"running\":true,\"aliases\":\"x\"}]}", "containers[0].aliases" },
        { "{\"containers\":[{\"name\":\"x\",\"running\":true,\"aliases\":[\"a\",3]}]}", "containers[0].aliases[1]" },
        { "{\"containers\":[{\"name\":\"x\",\"running\":true,\"processes\":[1]}]}", "containers[0].processes[0]" },
        { "{\"containers\":[{\"name\":\"x\",\"running\":true,\"processes\":[{\"args\":\"a\"}]}]}", "processes[0].comm" },
        { "{\"containers\":[{\"name\":\"x\",\"running\":true,\"ports\":[0]}]}", "ports[0]" },
        { "{\"containers\":[{\"name\":\"x\",\"running\":true,\"ports\":[22,65536]}]}", "ports[1]" },
        { "{\"containers\":[{\"name\":\"x\",\"running\":true,\"ports\":[1.5]}]}", "ports[0]" },
        { "{\"containers\":[{\"name\":\"x\",\"running\":true,\"ports\":[\"22\"]}]}", "ports[0]" },
        { "{\"containers\":[{\"name\":\"x\",\"running\":true,\"open_write\":[{\"path\":\"rel/a\"}]}]}", "open_write[0].path" },
        { "{\"containers\":[{\"name\":\"x\",\"running\":true,\"open_write\":[{\"path\":\"/a/../b\"}]}]}", "open_write[0].path" },
        { "{\"containers\":[{\"name\":\"x\",\"running\":true,\"open_write\":[{\"path\":\"/a/\"}]}]}", "open_write[0].path" },
        { "{\"containers\":[{\"name\":\"x\",\"running\":true,\"open_write\":[\"/a\"]}]}", "open_write[0]" },
        { "{\"containers\":[{\"name\":\"x\",\"running\":true,\"open_write\":[{\"path\":\"/a\",\"comm\":7}]}]}", "open_write[0].comm" },
        { b1, "containers[0].name" },
        { b2, "containers[0].image" },
        { b3, "processes[0].comm" },
    };
    for (size_t i = 0; i < sizeof(k) / sizeof(k[0]); i++) {
        sp_unit_probe_t *p = (sp_unit_probe_t *)1;
        status_t st = from_text(k[i].doc, &p);
        if (st != ERR_INVALID || p != NULL || !strstr(err_msg(), k[i].field))
            fail_msg("%s: wanted ERR_INVALID naming %s, got %d: %s", k[i].doc, k[i].field, st, err_msg());
        err_clear();
    }
    /* At the bounds exactly: accepted. */
    long_name[127] = '\0';
    snprintf(b1, sizeof(b1), "{\"containers\":[{\"name\":\"%s\",\"running\":true,\"ports\":[1,65535]}]}", long_name);
    sp_unit_probe_t *p = NULL;
    assert_int_equal(from_text(b1, &p), OK);
    assert_int_equal(strlen(p->v[0].name), 127);
    sp_unit_probe_free(p);
    /* Not an object at all. */
    assert_int_equal(sp_unit_probe_from_json(NULL, &p), ERR_INVALID);
    err_clear();
}

/* Counts past the bounds are cut, never refused. */
static void test_counts(void **state)
{
    (void)state;
    size_t cap = 1 << 20, o = 0;
    char *t = malloc(cap);
    o += (size_t)snprintf(t + o, cap - o, "{\"containers\":[");
    for (int i = 0; i < 33; i++)
        o += (size_t)snprintf(t + o, cap - o, "%s{\"name\":\"c%d\",\"running\":true}", i ? "," : "", i);
    snprintf(t + o, cap - o, "]}");
    sp_unit_probe_t *p = NULL;
    assert_int_equal(from_text(t, &p), OK);
    assert_int_equal(p->n, SP_PROBE_CONTAINERS_MAX);
    assert_true(p->truncated);
    sp_unit_probe_free(p);

    o = (size_t)snprintf(t, cap, "{\"containers\":[{\"name\":\"x\",\"running\":true,\"aliases\":[");
    for (int i = 0; i < 9; i++) o += (size_t)snprintf(t + o, cap - o, "%s\"a%d\",\"a%d\"", i ? "," : "", i, i);
    o += (size_t)snprintf(t + o, cap - o, "],\"processes\":[");
    for (int i = 0; i < 33; i++) o += (size_t)snprintf(t + o, cap - o, "%s{\"comm\":\"p%d\",\"args\":\"\"}", i ? "," : "", i);
    o += (size_t)snprintf(t + o, cap - o, "],\"ports\":[");
    for (int i = 0; i < 33; i++) o += (size_t)snprintf(t + o, cap - o, "%s%d", i ? "," : "", 1000 + i);
    o += (size_t)snprintf(t + o, cap - o, "],\"open_write\":[");
    for (int i = 0; i < 129; i++) o += (size_t)snprintf(t + o, cap - o, "%s{\"path\":\"/f/%d\",\"comm\":\"p\"}", i ? "," : "", i);
    snprintf(t + o, cap - o, "]}]}");
    assert_int_equal(from_text(t, &p), OK);
    assert_true(p->truncated);
    assert_int_equal(p->v[0].n_aliases, SP_PROBE_ALIASES_MAX);
    assert_int_equal(p->v[0].n_procs, SP_PROBE_PROCS_MAX);
    assert_int_equal(p->v[0].n_ports, SP_PROBE_PORTS_MAX);
    assert_int_equal(p->v[0].n_open, SP_PROBE_OPEN_MAX);
    sp_unit_probe_free(p);

    /* A truncated flag the sender set survives. */
    assert_int_equal(from_text("{\"containers\":[],\"truncated\":true}", &p), OK);
    assert_true(p->truncated);
    sp_unit_probe_free(p);
    free(t);
}

/* A full probe of long paths, cut to a packet: open files go first, from
 * the end, then processes, and the result still parses. */
static void test_budget(void **state)
{
    (void)state;
    sp_unit_probe_t *p = sp_unit_probe_new();
    p->n = SP_PROBE_CONTAINERS_MAX;
    p->v = calloc(p->n, sizeof(*p->v));
    char path[300];
    for (size_t i = 0; i < p->n; i++) {
        sp_probe_container_t *c = &p->v[i];
        snprintf(c->name, sizeof(c->name), "container-%zu", i);
        c->running = 1;
        c->procs = calloc(SP_PROBE_PROCS_MAX, sizeof(*c->procs));
        c->n_procs = SP_PROBE_PROCS_MAX;
        for (size_t k = 0; k < c->n_procs; k++) {
            snprintf(c->procs[k].comm, sizeof(c->procs[k].comm), "p%zu", k);
            memset(c->procs[k].args, 'a', 200);
        }
        c->open = calloc(SP_PROBE_OPEN_MAX, sizeof(*c->open));
        c->n_open = SP_PROBE_OPEN_MAX;
        for (size_t k = 0; k < c->n_open; k++) {
            snprintf(path, sizeof(path), "/mnt/cache/appdata/container-%zu/a/fairly/long/path/to/file-%zu", i, k);
            c->open[k].path = strdup(path);
        }
    }
    size_t budget = 65536 - 64;
    cJSON *o = sp_unit_probe_to_json(p, budget);
    char *t = cJSON_PrintUnformatted(o);
    cJSON_Delete(o);
    assert_true(strlen(t) <= budget);
    sp_unit_probe_t *q = NULL;
    assert_int_equal(from_text(t, &q), OK);
    cJSON_free(t);
    assert_true(q->truncated);
    assert_int_equal(q->n, SP_PROBE_CONTAINERS_MAX);
    /* Open files went from the end first; processes, only after every
     * open file, so the first container's open files went last. */
    assert_int_equal(q->v[q->n - 1].n_open, 0);
    int procs_cut = 0;
    for (size_t i = 0; i < q->n; i++) procs_cut |= q->v[i].n_procs < SP_PROBE_PROCS_MAX;
    if (procs_cut) for (size_t i = 0; i < q->n; i++) assert_int_equal(q->v[i].n_open, 0);
    sp_unit_probe_free(q);

    /* A budget too small for any container still answers the bare shape. */
    o = sp_unit_probe_to_json(p, 40);
    t = cJSON_PrintUnformatted(o);
    assert_string_equal(t, "{\"containers\":[],\"truncated\":true}");
    cJSON_free(t);
    cJSON_Delete(o);
    /* And one that fits is not marked. */
    sp_unit_probe_t *one = NULL;
    assert_int_equal(from_text(k_full, &one), OK);
    o = sp_unit_probe_to_json(one, 65536);
    assert_false(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(o, "truncated")));
    cJSON_Delete(o);
    sp_unit_probe_free(one);
    sp_unit_probe_free(p);
}

/* ------------------------------------------------------------------ */
/* /proc, synthetic                                                    */
/* ------------------------------------------------------------------ */

static void proc_pid(int pid, const char *cgroup, const char *comm, const char *cmdline, size_t cmdlen)
{
    char p[PATH_MAX];
    snprintf(p, sizeof(p), PROC "/%d/fd", pid);
    mkdirs(p);
    snprintf(p, sizeof(p), PROC "/%d/fdinfo", pid);
    mkdirs(p);
    snprintf(p, sizeof(p), PROC "/%d/cgroup", pid);
    puts_(p, cgroup);
    snprintf(p, sizeof(p), PROC "/%d/comm", pid);
    puts_(p, comm);
    snprintf(p, sizeof(p), PROC "/%d/cmdline", pid);
    put(p, cmdline, cmdlen);
}

/* An fd of pid: its link text and, unless flags is NULL, its fdinfo. */
static void proc_fd(int pid, int fd, const char *link, const char *flags, int mnt)
{
    char p[PATH_MAX], info[256];
    snprintf(p, sizeof(p), PROC "/%d/fd/%d", pid, fd);
    assert_int_equal(symlink(link, p), 0);
    if (!flags) return;
    snprintf(p, sizeof(p), PROC "/%d/fdinfo/%d", pid, fd);
    snprintf(info, sizeof(info), "pos:\t0\nflags:\t%s\nmnt_id:\t%d\nino:\t42\n", flags, mnt);
    puts_(p, info);
}

#define DATA "/var/lib/postgresql/data"
static const char k_mountinfo[] =
    "20 1 0:50 / / rw - overlay overlay rw\n"
    "30 20 0:60 /appdata/pg " DATA " rw,noatime master:1 - zfs cache/appdata rw\n"
    "31 30 0:61 / " DATA "/excl rw - xfs /dev/sdb rw\n"
    "32 30 0:62 / " DATA "/shm rw - tmpfs tmpfs rw\n"
    "33 30 0:63 / " DATA "/child rw - zfs cache/appdata/child rw\n"
    "34 20 0:64 / /etc/hosts rw - ext4 /dev/sda1 rw\n"
    "35 30 0:65 / " DATA "/with\\040space rw - zfs cache/appdata/sp rw\n";

static const proc_mount_t k_mounts[] = {
    { DATA, "/mnt/cache/appdata/pg" },
    { DATA "/excl", NULL },
    { "/etc/hosts", NULL },
};

static void build_tree(void)
{
    rmrf(D);
    mkdirs(PROC);
    static const char pg_args[] = "postgres\0-c\0config_file=/etc/pg.conf\0";
    proc_pid(100, "0::/docker/abc\n", "postgres\n", pg_args, sizeof(pg_args) - 1);
    proc_pid(101, "1:name=systemd:/x\n0::/docker/abc/sub\n", "postgres\n", pg_args, sizeof(pg_args) - 1);
    proc_pid(102, "0::/docker/abcd\n", "intruder\n", "intruder\0", 9);
    proc_pid(103, "0::/other\n", "other\n", "other\0", 6);
    static const char odd[] = "worker\0caf\xc3\xa9\0line\nbreak\0\0";
    proc_pid(104, "0::/docker/abc\n", "pg\x01worker\n", odd, sizeof(odd) - 1);
    /* Exits mid-scan: listed, but its files are gone. */
    mkdirs(PROC "/105");
    puts_(PROC "/105/cgroup", "0::/docker/abc\n");
    /* A FIFO where a file should be is never read: it would block. */
    proc_pid(106, "0::/docker/abc\n", "x\n", "", 0);
    unlink(PROC "/106/comm");
    assert_int_equal(mkfifo(PROC "/106/comm", 0600), 0);
    mkdirs(PROC "/self");                         /* not a pid */

    mkdirs(PROC "/100/net");
    puts_(PROC "/100/net/tcp",
          "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"
          "   0: 00000000:1538 00000000:0000 0A 00000000:00000000 00:00000000 00000000   999        0 1 1\n"
          "   1: 0100007F:0050 0100007F:9C40 01 00000000:00000000 00:00000000 00000000   999        0 2 1\n"
          "   2: 00000000:0000 00000000:0000 0A 00000000:00000000 00:00000000 00000000   999        0 3 1\n");
    puts_(PROC "/100/net/tcp6",
          "  sl  local_address                         remote_address                        st tx_queue\n"
          "   0: 00000000000000000000000000000000:1F90 00000000000000000000000000000000:0000 0A 00000000:00000000\n"
          "   1: 00000000000000000000000000000000:1538 00000000000000000000000000000000:0000 0A 00000000:00000000\n"
          "   2: 00000000000000000000000001000000:01BB 00000000000000000000000001000000:D431 06 00000000:00000000\n");
    puts_(PROC "/100/mountinfo", k_mountinfo);
    puts_(PROC "/101/mountinfo", k_mountinfo);
    puts_(PROC "/102/mountinfo", k_mountinfo);

    proc_fd(100, 3, DATA "/postmaster.pid", "0100001", 30);
    proc_fd(101, 3, DATA "/base/1/2619", "0100002", 30);          /* O_RDWR */
    proc_fd(101, 4, DATA "/PG_VERSION", "0100000", 30);           /* O_RDONLY */
    proc_fd(101, 5, DATA "/pg_wal/000001", "02101", 30);          /* O_WRONLY|O_APPEND|O_CREAT */
    proc_fd(101, 6, "/etc/hosts", "02", 34);                      /* outside every included mount */
    proc_fd(101, 7, "socket:[12345]", "02", 0);
    proc_fd(101, 8, DATA "/excl/f", "02", 31);                    /* under an excluded mount */
    proc_fd(101, 9, DATA "/shm/seg", "02", 32);                   /* a tmpfs inside */
    proc_fd(101, 10, DATA "/child/c", "02", 33);                  /* a dataset inside: its data */
    proc_fd(101, 11, DATA "/base/1/9999", "02", 34);              /* the text is inside; its mount is not */
    proc_fd(101, 12, DATA "/gone (deleted)", "02", 30);
    proc_fd(101, 13, DATA "/base/1/2619", "02", 30);              /* the same file twice */
    proc_fd(101, 14, "pipe:[1]", "01", 0);
    proc_fd(101, 15, DATA "/nofdinfo", NULL, 0);
    proc_fd(101, 16, DATA "/x", "02", 99);                        /* a mount not in the table */
    proc_fd(101, 17, DATA "/with space/f", "02", 35);
    proc_fd(102, 3, DATA "/intruder", "02", 30);                  /* not the container's process */
}

static void test_proc_synthetic(void **state)
{
    (void)state;
    build_tree();
    sp_probe_container_t c;
    memset(&c, 0, sizeof(c));
    int truncated = 0;
    assert_int_equal(proc_probe_container(PROC, 100, k_mounts, 3, &c, &truncated), OK);
    assert_false(truncated);

    /* Processes: 100, 101 (deduplicated with it) and 104; not 102 (a
     * sibling whose name only starts the same), not 103, not 105 (gone),
     * not 106 (its comm cannot be read without blocking). */
    assert_int_equal(c.n_procs, 2);
    assert_true(has_proc(&c, "postgres", "postgres -c config_file=/etc/pg.conf"));
    assert_true(has_proc(&c, "pg?worker", "worker caf?? line?break"));
    assert_false(has_proc(&c, "intruder", NULL));
    assert_false(has_proc(&c, "other", NULL));

    /* Ports: LISTEN only, port 0 and established rows left out, tcp6
     * merged, ascending. */
    assert_int_equal(c.n_ports, 2);
    assert_int_equal(c.ports[0], 5432);
    assert_int_equal(c.ports[1], 8080);

    /* Open for writing, mapped to recorded paths. */
    assert_true(has_open(&c, "/mnt/cache/appdata/pg/postmaster.pid"));
    assert_true(has_open(&c, "/mnt/cache/appdata/pg/base/1/2619"));
    assert_true(has_open(&c, "/mnt/cache/appdata/pg/pg_wal/000001"));
    assert_true(has_open(&c, "/mnt/cache/appdata/pg/child/c"));
    assert_true(has_open(&c, "/mnt/cache/appdata/pg/with space/f"));
    assert_int_equal(c.n_open, 5);
    for (size_t i = 0; i < c.n_open; i++) {
        assert_int_equal(strncmp(c.open[i].path, "/mnt/cache/appdata/pg/", 22), 0);
        assert_string_equal(c.open[i].comm, "postgres");
    }

    /* A pid that is gone is the container having stopped. */
    sp_probe_container_t g;
    memset(&g, 0, sizeof(g));
    assert_int_equal(proc_probe_container(PROC, 4242, k_mounts, 3, &g, &truncated), ERR_NOT_FOUND);
    err_clear();
    assert_int_equal(proc_probe_container(PROC, 0, k_mounts, 3, &g, &truncated), ERR_INVALID);
    err_clear();

    /* What the helper answers from it parses on the daemon's side (the
     * backend names the container). */
    snprintf(c.name, sizeof(c.name), "pg");
    sp_unit_probe_t *pw = sp_unit_probe_new();
    pw->v = malloc(sizeof(c));
    memcpy(pw->v, &c, sizeof(c));
    pw->n = 1;
    cJSON *o = sp_unit_probe_to_json(pw, 65536);
    sp_unit_probe_t *back = NULL;
    assert_int_equal(sp_unit_probe_from_json(o, &back), OK);
    assert_int_equal(back->v[0].n_open, 5);
    cJSON_Delete(o);
    sp_unit_probe_free(back);
    sp_unit_probe_free(pw);
    free(g.procs); free(g.ports); free(g.open);
    rmrf(D);
}

/* The rootfs as an included mount at "/" (LXC): kernel mounts inside it
 * and a recorded root of "/" both map correctly. */
static void test_proc_rootfs(void **state)
{
    (void)state;
    rmrf(D);
    mkdirs(PROC);
    proc_pid(300, "0::/lxc.payload.box\n", "init\n", "/sbin/init\0", 11);
    puts_(PROC "/300/mountinfo",
          "40 1 0:70 /box/rootfs / rw - zfs cache/lxc/box rw\n"
          "41 40 0:5 / /proc rw - proc proc rw\n"
          "42 40 0:6 / /dev/shm rw - tmpfs tmpfs rw\n"
          "43 40 0:71 /dev /srv/dev rw - zfs cache/appdata rw\n");
    proc_fd(300, 3, "/var/log/syslog", "02", 40);
    proc_fd(300, 4, "/dev/shm/x", "02", 42);
    proc_fd(300, 5, "/srv/dev/work", "02", 43);
    proc_fd(300, 6, "/proc/1/attr/current", "02", 41);
    proc_fd(300, 7, "/", "02", 40);                               /* never a clean file path */
    static const proc_mount_t m[] = {
        { "/", "/mnt/cache/lxc/box/rootfs" },
        { "/proc", NULL },
        { "/srv/dev", "/mnt/cache/appdata/dev" },
    };
    sp_probe_container_t c;
    memset(&c, 0, sizeof(c));
    int truncated = 0;
    assert_int_equal(proc_probe_container(PROC, 300, m, 3, &c, &truncated), OK);
    assert_true(has_open(&c, "/mnt/cache/lxc/box/rootfs/var/log/syslog"));
    assert_true(has_open(&c, "/mnt/cache/appdata/dev/work"));
    assert_int_equal(c.n_open, 2);
    for (size_t i = 0; i < c.n_open; i++) free(c.open[i].path);
    free(c.open); free(c.procs); free(c.ports);

    /* recorded "/" (a rootfs recorded at the root) does not double the
     * slash. */
    static const proc_mount_t r[] = { { "/", "/" } };
    memset(&c, 0, sizeof(c));
    assert_int_equal(proc_probe_container(PROC, 300, r, 1, &c, &truncated), OK);
    assert_true(has_open(&c, "/var/log/syslog"));
    for (size_t i = 0; i < c.n_open; i++) free(c.open[i].path);
    free(c.open); free(c.procs); free(c.ports);
    rmrf(D);
}

/* A process in the root cgroup would make every process a member: then
 * it is the init process and its descendants. */
static void test_proc_root_cgroup(void **state)
{
    (void)state;
    rmrf(D);
    mkdirs(PROC);
    proc_pid(200, "0::/\n", "init\n", "init\0", 5);
    proc_pid(201, "0::/\n", "child\n", "child\0", 6);
    proc_pid(202, "0::/\n", "grandchild\n", "gc\0", 3);
    proc_pid(203, "0::/\n", "stranger\n", "s\0", 2);
    proc_pid(150, "0::/\n", "early\n", "e\0", 2);     /* a lower pid, parented by 202 */
    puts_(PROC "/201/stat", "201 (child) S 200 201 201 0 -1\n");
    puts_(PROC "/202/stat", "202 (a) b) S 201 202 202 0 -1\n");   /* a ')' in the name */
    puts_(PROC "/203/stat", "203 (stranger) S 1 203 203 0 -1\n");
    puts_(PROC "/150/stat", "150 (early) S 202 150 150 0 -1\n");
    puts_(PROC "/200/stat", "200 (init) S 1 200 200 0 -1\n");
    sp_probe_container_t c;
    memset(&c, 0, sizeof(c));
    int truncated = 0;
    assert_int_equal(proc_probe_container(PROC, 200, NULL, 0, &c, &truncated), OK);
    assert_true(has_proc(&c, "init", NULL));
    assert_true(has_proc(&c, "child", NULL));
    assert_true(has_proc(&c, "grandchild", NULL));
    assert_true(has_proc(&c, "early", NULL));
    assert_false(has_proc(&c, "stranger", NULL));
    free(c.open); free(c.procs); free(c.ports);
    rmrf(D);
}

/* Past the bounds: cut, and said. */
static void test_proc_bounds(void **state)
{
    (void)state;
    rmrf(D);
    mkdirs(PROC);
    char args[64], p[PATH_MAX];
    for (int i = 0; i < 40; i++) {
        int n = snprintf(args, sizeof(args), "worker%c%d", '\0', i);
        proc_pid(400 + i, "0::/docker/big\n", "worker\n", args, (size_t)n + 1);
    }
    puts_(PROC "/400/mountinfo", "50 1 0:80 / /data rw - ext4 /dev/sdc rw\n");
    for (int i = 0; i < 140; i++) {
        snprintf(p, sizeof(p), "/data/f%d", i);
        proc_fd(400, 3 + i, p, "02", 50);
    }
    mkdirs(PROC "/400/net");
    char tcp[8192];
    size_t o = (size_t)snprintf(tcp, sizeof(tcp), "  sl  local_address rem_address   st\n");
    for (int i = 0; i < 40; i++)
        o += (size_t)snprintf(tcp + o, sizeof(tcp) - o, "%4d: 00000000:%04X 00000000:0000 0A 0\n", i, 2000 + i);
    puts_(PROC "/400/net/tcp", tcp);
    static const proc_mount_t m[] = { { "/data", "/rec" } };
    sp_probe_container_t c;
    memset(&c, 0, sizeof(c));
    int truncated = 0;
    assert_int_equal(proc_probe_container(PROC, 400, m, 1, &c, &truncated), OK);
    assert_true(truncated);
    assert_int_equal(c.n_procs, SP_PROBE_PROCS_MAX);
    assert_int_equal(c.n_ports, SP_PROBE_PORTS_MAX);
    assert_int_equal(c.n_open, SP_PROBE_OPEN_MAX);
    for (size_t i = 0; i < c.n_open; i++) free(c.open[i].path);
    free(c.open); free(c.procs); free(c.ports);
    rmrf(D);
}

/* ------------------------------------------------------------------ */
/* /proc, real                                                         */
/* ------------------------------------------------------------------ */

/* The mount point holding path, from this process's own table. */
static void mount_of(const char *path, char *out, size_t sz)
{
    FILE *f = fopen("/proc/self/mountinfo", "r");
    assert_non_null(f);
    char line[8192];
    size_t best = 0;
    snprintf(out, sz, "/");
    while (fgets(line, sizeof(line), f)) {
        char mp[4096];
        if (sscanf(line, "%*s %*s %*s %*s %4095s", mp) != 1 || strchr(mp, '\\')) continue;
        size_t n = strlen(mp);
        int under = strcmp(mp, "/") == 0 || (strncmp(path, mp, n) == 0 && (path[n] == '/' || path[n] == '\0'));
        if (under && n >= best) { best = n; snprintf(out, sz, "%s", mp); }
    }
    fclose(f);
}

static void test_proc_real(void **state)
{
    (void)state;
    rmrf(D);
    mkdirs(D);
    char real[PATH_MAX], mp[PATH_MAX], want[2 * PATH_MAX];
    assert_non_null(realpath(D, real));
    char file[PATH_MAX + 16];
    snprintf(file, sizeof(file), "%s/held-for-writing", real);
    int fd = open(file, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
    assert_true(fd >= 0);
    char rofile[PATH_MAX + 16];
    snprintf(rofile, sizeof(rofile), "%s/held-for-reading", real);
    close(open(rofile, O_WRONLY | O_CREAT | O_CLOEXEC, 0600));
    int rfd = open(rofile, O_RDONLY | O_CLOEXEC);
    assert_true(rfd >= 0);
    mount_of(real, mp, sizeof(mp));
    proc_mount_t m[] = { { mp, "/recorded" } };
    snprintf(want, sizeof(want), "/recorded%s", strcmp(mp, "/") == 0 ? file : file + strlen(mp));
    char notwant[2 * PATH_MAX];
    snprintf(notwant, sizeof(notwant), "/recorded%s", strcmp(mp, "/") == 0 ? rofile : rofile + strlen(mp));

    sp_probe_container_t c;
    memset(&c, 0, sizeof(c));
    int truncated = 0;
    assert_int_equal(proc_probe_container("/proc", (int)getpid(), m, 1, &c, &truncated), OK);
    /* Other processes of this cgroup (make, a shell) count against the
     * bounds too; only a probe cut short may miss the file. */
    if (truncated) fprintf(stderr, "  note: the probe of this process's cgroup was truncated\n");
    if (!has_open(&c, want) && !truncated) {
        for (size_t i = 0; i < c.n_open; i++) fprintf(stderr, "  open: %s\n", c.open[i].path);
        fail_msg("%s not among the files open for writing (mount %s)", want, mp);
    }
    assert_false(has_open(&c, notwant));
    assert_true(c.n_procs >= 1);
    for (size_t i = 0; i < c.n_open; i++) free(c.open[i].path);
    free(c.open); free(c.procs); free(c.ports);
    close(fd);
    close(rfd);
    rmrf(D);
}

/* ------------------------------------------------------------------ */
/* The fake provider                                                   */
/* ------------------------------------------------------------------ */

static void test_fake_provider(void **state)
{
    (void)state;
    sp_ctx_t *fk = NULL;
    sp_config_t pc = { NULL, "0badf00d" };
    assert_int_equal(fake_provider_ops.init(&pc, &fk), OK);
    assert_non_null(fake_provider_ops.unit_probe);
    assert_int_equal(fake_unit_add(fk, "u1", "one", SP_STATE_RUNNING, 0), OK);
    sp_unit_probe_t *p = NULL;
    /* No spec probe: an empty answer. */
    assert_int_equal(fake_provider_ops.unit_probe(fk, "u1", &p), OK);
    assert_int_equal(p->n, 0);
    sp_unit_probe_free(p);
    assert_int_equal(fake_provider_ops.unit_probe(fk, "nope", &p), ERR_NOT_FOUND);
    err_clear();
    assert_int_equal(fake_unit_set_probe(fk, "nope", "{}", 0), ERR_NOT_FOUND);
    err_clear();

    assert_int_equal(fake_unit_set_probe(fk, "u1", k_full, 0), OK);
    assert_int_equal(fake_provider_ops.unit_probe(fk, "u1", &p), OK);
    assert_int_equal(p->n, 2);
    assert_string_equal(p->v[0].open[0].path, "/mnt/cache/appdata/pg/base/1/2619");
    sp_unit_probe_free(p);

    fake_fail_set(fk, "u1", FAKE_FAIL_PROBE, 0);
    assert_int_equal(fake_provider_ops.unit_probe(fk, "u1", &p), ERR_IO);
    assert_non_null(strstr(err_msg(), "injected"));
    err_clear();
    fake_fail_clear(fk);
    assert_int_equal(fake_unit_set_probe(fk, "u1", k_full, 1), OK);
    assert_int_equal(fake_provider_ops.unit_probe(fk, "u1", &p), ERR_IO);
    err_clear();

    /* A malformed one fails the probe's own validation. */
    assert_int_equal(fake_unit_set_probe(fk, "u1", "{\"containers\":[{\"name\":\"x\"}]}", 0), OK);
    assert_int_equal(fake_provider_ops.unit_probe(fk, "u1", &p), ERR_INVALID);
    assert_non_null(strstr(err_msg(), "running"));
    err_clear();
    assert_int_equal(fake_unit_set_probe(fk, "u1", "not json", 0), OK);
    assert_int_equal(fake_provider_ops.unit_probe(fk, "u1", &p), ERR_INVALID);
    err_clear();

    /* From a spec file, as the helper's fake backend loads it. */
    rmrf(D);
    mkdirs(D);
    puts_(D "/units.json",
          "{\"units\":[{\"id\":\"s1\",\"name\":\"cbk-s1\",\"probe\":{\"containers\":[{\"name\":\"redis\","
          "\"running\":true,\"ports\":[6379]}]}},"
          "{\"id\":\"s2\",\"name\":\"cbk-s2\",\"probe_fail\":true}]}");
    assert_int_equal(fake_provider_load(fk, D "/units.json"), OK);
    assert_int_equal(fake_provider_ops.unit_probe(fk, "s1", &p), OK);
    assert_int_equal(p->v[0].ports[0], 6379);
    sp_unit_probe_free(p);
    assert_int_equal(fake_provider_ops.unit_probe(fk, "s2", &p), ERR_IO);
    err_clear();
    fake_provider_ops.fini(fk);
    rmrf(D);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_round_trip),
        cmocka_unit_test(test_refusals),
        cmocka_unit_test(test_counts),
        cmocka_unit_test(test_budget),
        cmocka_unit_test(test_proc_synthetic),
        cmocka_unit_test(test_proc_rootfs),
        cmocka_unit_test(test_proc_root_cgroup),
        cmocka_unit_test(test_proc_bounds),
        cmocka_unit_test(test_proc_real),
        cmocka_unit_test(test_fake_provider),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
