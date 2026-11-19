#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

/*
 * Database detection (docs/db-detection.md 11.1): every engine's signature
 * from a synthetic layout with real headers, the false positives of section
 * 4, confidence, and the coverage judgements of section 5 against fake
 * captures. The layouts are built here into scratch rather than committed
 * under tests/fixtures/db/: half of them are binary headers, which read
 * better as the bytes they are than as blobs.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <limits.h>
#include <unistd.h>
#include <sys/stat.h>

#include <cmocka.h>

#include "db_detect.h"
#include "scan.h"
#include "test_tmp.h"

#define TEST_DIR TEST_TMP "/c_backup_test_db_detect"

static void rmrf(const char *p)
{
    char cmd[PATH_MAX + 16];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static void mkdirs(const char *path)
{
    char cmd[PATH_MAX + 16];
    snprintf(cmd, sizeof(cmd), "mkdir -p '%s'", path);
    assert_int_equal(system(cmd), 0);
}

/* path relative to TEST_DIR; parents made. size pads with zeros past the
 * bytes given. */
static void wbytes(const char *rel, const void *b, size_t n, size_t size)
{
    char p[PATH_MAX];
    snprintf(p, sizeof(p), "%s/%s", TEST_DIR, rel);
    char d[PATH_MAX];
    snprintf(d, sizeof(d), "%s", p);
    char *s = strrchr(d, '/');
    *s = '\0';
    mkdirs(d);
    int fd = open(p, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    assert_true(fd >= 0);
    if (n) assert_int_equal(write(fd, b, n), (ssize_t)n);
    if (size > n) assert_int_equal(ftruncate(fd, (off_t)size), 0);
    close(fd);
}

static void wtext(const char *rel, const char *t) { wbytes(rel, t, strlen(t), 0); }
static void wempty(const char *rel) { wbytes(rel, NULL, 0, 0); }

static void wdir(const char *rel)
{
    char p[PATH_MAX];
    snprintf(p, sizeof(p), "%s/%s", TEST_DIR, rel);
    mkdirs(p);
}

static void wlink(const char *rel, const char *target)
{
    char p[PATH_MAX];
    snprintf(p, sizeof(p), "%s/%s", TEST_DIR, rel);
    assert_int_equal(symlink(target, p), 0);
}

static void wsqlite(const char *rel, int wal, size_t size)
{
    uint8_t h[100];
    memset(h, 0, sizeof(h));
    memcpy(h, "SQLite format 3", 16);
    h[16] = 0x10;           /* page size 4096 */
    h[18] = wal ? 2 : 1;
    h[19] = wal ? 2 : 1;
    wbytes(rel, h, sizeof(h), size);
}

static void wmagic16(const char *rel, uint32_t magic)
{
    uint8_t h[64];
    memset(h, 0, sizeof(h));
    h[16] = (uint8_t)magic; h[17] = (uint8_t)(magic >> 8);
    h[18] = (uint8_t)(magic >> 16); h[19] = (uint8_t)(magic >> 24);
    wbytes(rel, h, sizeof(h), 4096);
}

static char *abs_of(const char *rel)
{
    static char buf[4][PATH_MAX];
    static int k;
    char *b = buf[k++ & 3];
    snprintf(b, PATH_MAX, "%s%s%s", TEST_DIR, rel[0] ? "/" : "", rel);
    return b;
}

/* Scan root as the backup would, feed every entry and every file's head. */
static void feed(db_detector_t *d, const char *root, const char *const *unsettled)
{
    scan_imap_t *im = scan_imap_new();
    scan_result_t *sr = NULL;
    assert_int_equal(scan_tree(root, im, NULL, &sr), OK);
    for (uint32_t i = 0; i < sr->count; i++) {
        const scan_entry_t *e = &sr->entries[i];
        db_detect_entry(d, e->path, e->node.type == NODE_TYPE_DIR,
                        e->node.type == NODE_TYPE_SYMLINK, e->node.size, e->symlink_target);
        if (e->node.type != NODE_TYPE_REG) continue;
        int fd = open(e->path, O_RDONLY);
        assert_true(fd >= 0);
        db_detect_head_fd(d, e->path, fd, e->node.size);
        close(fd);
    }
    for (size_t i = 0; unsettled && unsettled[i]; i++) db_detect_unsettled(d, unsettled[i]);
    scan_result_free(sr);
    scan_imap_free(im);
}

static db_findings_t *detect(const char *root, const db_capture_desc_t *cap,
                             const char *const *unsettled)
{
    db_detect_cfg_t cfg = { 1024 * 1024 };
    db_detector_t *d = NULL;
    assert_int_equal(db_detect_begin(&cfg, &d), OK);
    feed(d, root, unsettled);
    db_findings_t *f = NULL;
    assert_int_equal(db_detect_finish(d, cap, &f), OK);
    db_detect_free(d);
    assert_non_null(f);
    return f;
}

static const db_finding_t *find(const db_findings_t *f, db_engine_t e, const char *rel)
{
    const char *p = abs_of(rel);
    for (size_t i = 0; i < f->n; i++)
        if (f->v[i].engine == e && strcmp(f->v[i].path, p) == 0) return &f->v[i];
    return NULL;
}

static size_t count_engine(const db_findings_t *f, db_engine_t e)
{
    size_t n = 0;
    for (size_t i = 0; i < f->n; i++) n += f->v[i].engine == e;
    return n;
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    mkdirs(TEST_DIR);
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    return 0;
}

/* ------------------------------------------------------------------ */

static void test_classify(void **state)
{
    (void)state;
    uint8_t sq[32] = "SQLite format 3";
    sq[18] = 2; sq[19] = 2;
    assert_int_equal(db_detect_classify("x.db", sq, sizeof(sq)), DB_HEAD_CODE(DB_HEAD_SQLITE, 2));
    sq[18] = 1; sq[19] = 1;
    assert_int_equal(db_detect_classify("x", sq, sizeof(sq)), DB_HEAD_CODE(DB_HEAD_SQLITE, 1));
    /* Too short for the version bytes. */
    assert_int_equal(db_detect_classify("x", sq, 16), DB_HEAD_NONE);
    assert_int_equal(db_detect_classify("dump.rdb", "REDIS0011\xfa", 10), DB_HEAD_CODE(DB_HEAD_REDIS_RDB, 11));
    assert_int_equal(db_detect_classify("dump.rdb", "REDISX011", 9), DB_HEAD_NONE);
    uint8_t lm[24] = { 0 };
    lm[16] = 0xDE; lm[17] = 0xC0; lm[18] = 0xEF; lm[19] = 0xBE;
    assert_int_equal(DB_HEAD_KIND(db_detect_classify("data.mdb", lm, sizeof(lm))), DB_HEAD_LMDB);
    lm[16] = 0xED; lm[17] = 0xDA; lm[18] = 0x0C; lm[19] = 0xED;
    assert_int_equal(DB_HEAD_KIND(db_detect_classify("a.bolt", lm, sizeof(lm))), DB_HEAD_BBOLT);
    assert_int_equal(db_detect_classify("PG_VERSION", "16\n", 3), DB_HEAD_CODE(DB_HEAD_PG_VERSION, 1600));
    assert_int_equal(db_detect_classify("PG_VERSION", "9.6\n", 4), DB_HEAD_CODE(DB_HEAD_PG_VERSION, 906));
    assert_int_equal(db_detect_classify("PG_VERSION", "hello\n", 6), DB_HEAD_CODE(DB_HEAD_PG_VERSION, 0xffff));
    assert_int_equal(db_detect_classify("PG_VERSION", "16x", 3), DB_HEAD_CODE(DB_HEAD_PG_VERSION, 0xffff));
    /* Content decides, not the name: PG_VERSION's rules apply to that name only. */
    assert_int_equal(db_detect_classify("VERSION", "16\n", 3), DB_HEAD_NONE);
    assert_int_equal(db_detect_classify("meta.json", "{\n\t\"ulid\": \"01H\"", 16), DB_HEAD_CODE(DB_HEAD_PROM_META, 1));
    assert_int_equal(db_detect_classify("meta.json", "{\"version\":1}", 13), DB_HEAD_CODE(DB_HEAD_PROM_META, 0));
    assert_int_equal(db_detect_classify("CURRENT", "MANIFEST-000002\n", 16), DB_HEAD_CODE(DB_HEAD_LDB_CURRENT, 1));
    assert_int_equal(db_detect_classify("CURRENT", "nope", 4), DB_HEAD_CODE(DB_HEAD_LDB_CURRENT, 0));
    const char *nc = "<?php\n$CONFIG = array (\n  'dbtype' => 'mysql',\n  'dbhost' => 'db',\n);\n";
    assert_int_equal(db_detect_classify("config.php", nc, strlen(nc)), DB_HEAD_CODE(DB_HEAD_NEXTCLOUD, 2));
    assert_int_equal(db_detect_classify("config.php", "<?php echo 1;", 13), DB_HEAD_NONE);
    assert_int_equal(db_detect_classify("x.db", "not a database at all, honestly", 31), DB_HEAD_NONE);
    assert_int_equal(db_detect_classify(NULL, NULL, 0), DB_HEAD_NONE);
    assert_true(db_detect_wants_more("/a/config/config.php"));
    assert_false(db_detect_wants_more("/a/config.php"));
}

static void test_vocabulary(void **state)
{
    (void)state;
    for (int e = 1; e < DB_ENGINE_COUNT_; e++) {
        assert_int_equal(db_engine_parse(db_engine_name((db_engine_t)e)), e);
        assert_true(strlen(db_engine_title((db_engine_t)e)) > 0);
    }
    assert_int_equal(db_engine_parse("oracle"), DB_ENGINE_NONE);
    assert_string_equal(db_engine_name(DB_ENGINE_NONE), "unknown");
    for (int c = DB_COV_COVERED_CRASH; c <= DB_COV_UNCOVERED; c++) {
        db_coverage_t out;
        assert_int_equal(db_coverage_parse(db_coverage_name((db_coverage_t)c), &out), 0);
        assert_int_equal(out, c);
    }
    db_coverage_t out;
    assert_int_equal(db_coverage_parse("", &out), -1);
    assert_int_equal(db_coverage_parse("nope", &out), -1);
    assert_string_equal(db_confidence_name(DB_CONF_LIVE), "live");
    assert_string_equal(db_confidence_name(DB_CONF_DORMANT), "dormant");
    assert_string_equal(db_confidence_name(DB_CONF_HINT), "hint");
}

/* Every engine of section 3, in one tree, read from one atomic capture. */
static void build_all(void)
{
    wtext("pg/data/PG_VERSION", "16\n");
    wempty("pg/data/global/pg_control");
    wempty("pg/data/base/1/112");
    wtext("pg/data/postmaster.pid", "1\n");

    wempty("maria/ibdata1");
    wempty("maria/ib_logfile0");
    wdir("maria/mysql");
    wempty("maria/aria_log_control");
    wempty("maria/app/ledger.MYD");
    wempty("maria/app/ledger.MYI");
    wtext("maria/mysqld.pid", "7\n");

    wsqlite("app/live.db", 1, 2u << 20);
    wempty("app/live.db-wal");
    wempty("app/live.db-shm");
    wsqlite("app/small.db", 0, 8192);
    wsqlite("app/big.db", 0, 2u << 20);

    wbytes("redis/dump.rdb", "REDIS0011\xfa\x09redis-ver", 20, 0);
    wempty("valkey/appendonlydir/appendonly.aof.1.base.rdb");
    wtext("valkey/appendonlydir/appendonly.aof.manifest", "file appendonly.aof.1.base.rdb seq 1 type b\n");

    wempty("prom/wal/00000001");
    wempty("prom/chunks_head/000001");
    wtext("prom/01HZX3K4T9W8N5Q2R7B6C1D0EF/meta.json", "{\n\t\"ulid\": \"01HZX3K4T9W8N5Q2R7B6C1D0EF\"\n}");
    wempty("prom/01HZX3K4T9W8N5Q2R7B6C1D0EF/index");
    wempty("prom/lock");

    wempty("influx1/meta/meta.db");
    wempty("influx1/data/db1/x.tsm");
    wempty("influx1/wal/db1/x.wal");

    wmagic16("influx2/influxd.bolt", 0xED0CDAEDu);
    wempty("influx2/engine/data/x.tsm");
    wempty("influx2/engine/wal/x.wal");

    wempty("mongo/WiredTiger");
    wempty("mongo/WiredTiger.turtle");
    wempty("mongo/WiredTiger.wt");
    wempty("mongo/journal/WiredTigerLog.0000000001");
    wtext("mongo/mongod.lock", "1\n");

    wempty("es/_state/state-1.st");
    wempty("es/node.lock");
    wempty("es/indices/Zx9rT1QwS0uQhY8b/0/index/segments_3");

    wtext("ldb/CURRENT", "MANIFEST-000002\n");
    wempty("ldb/MANIFEST-000002");
    wempty("ldb/000003.log");
    wempty("ldb/LOCK");

    wtext("rocks/CURRENT", "MANIFEST-000005\n");
    wempty("rocks/MANIFEST-000005");
    wempty("rocks/OPTIONS-000007");
    wempty("rocks/000004.sst");
    wempty("rocks/000003.log");
    wempty("rocks/LOCK");

    wmagic16("lmdb/data.mdb", 0xBEEFC0DEu);
    wempty("lmdb/lock.mdb");

    wmagic16("bolt/app.bolt", 0xED0CDAEDu);

    wempty("ch/metadata/default.sql");
    wempty("ch/store/3f2/3f2a7c1e-0000-4000-8000-000000000001/all_1_1_0/columns.txt");
    wempty("ch/store/3f2/3f2a7c1e-0000-4000-8000-000000000001/all_1_1_0/checksums.txt");
    wempty("ch/status");

    wtext("nc/config/config.php", "<?php\n$CONFIG = array (\n  'dbtype' => 'pgsql',\n  'dbhost' => 'db',\n"
                                  "  'version' => '30.0.2.2',\n);\n");
}

static void test_signatures(void **state)
{
    (void)state;
    build_all();
    db_cap_root_t r = { TEST_DIR, DB_CAP_ATOMIC, 0 };
    db_capture_desc_t cap = { &r, 1, 0, 0, 0, 1 };
    db_findings_t *f = detect(TEST_DIR, &cap, NULL);

    const db_finding_t *x = find(f, DB_ENGINE_POSTGRES, "pg/data");
    assert_non_null(x);
    assert_string_equal(x->version, "16");
    assert_int_equal(x->confidence, DB_CONF_LIVE);
    assert_int_equal(x->coverage, DB_COV_COVERED_CRASH);
    assert_string_equal(x->method, "snapshot");
    assert_non_null(strstr(x->evidence, "postmaster.pid"));
    assert_non_null(strstr(x->evidence, "global/pg_control"));
    /* Covered, in a unit: the optional dump, for portability. */
    assert_null(x->suggestion);
    assert_string_equal(x->optional, "pg-dumpall");

    x = find(f, DB_ENGINE_MYSQL, "maria");
    assert_non_null(x);
    assert_int_equal(x->confidence, DB_CONF_LIVE);
    assert_int_equal(x->coverage, DB_COV_ENGINE_UNSAFE);
    assert_string_equal(x->suggestion, "mariadb-dump");
    assert_non_null(strstr(x->evidence, "\"flavour\":\"mariadb\""));
    assert_non_null(strstr(x->evidence, "MyISAM"));

    x = find(f, DB_ENGINE_SQLITE, "app/live.db");
    assert_non_null(x);
    assert_int_equal(x->confidence, DB_CONF_LIVE);
    assert_non_null(strstr(x->evidence, "\"journal_mode\":\"wal\""));
    assert_non_null(strstr(x->evidence, "-wal"));
    x = find(f, DB_ENGINE_SQLITE, "app/big.db");
    assert_non_null(x);
    assert_int_equal(x->confidence, DB_CONF_DORMANT);
    assert_int_equal(x->coverage, DB_COV_NONE);
    assert_null(find(f, DB_ENGINE_SQLITE, "app/small.db"));
    assert_int_equal(f->sqlite_small, 1);

    x = find(f, DB_ENGINE_REDIS, "redis");
    assert_non_null(x);
    assert_string_equal(x->version, "rdb 11");
    assert_int_equal(x->confidence, DB_CONF_DORMANT);
    assert_non_null(find(f, DB_ENGINE_REDIS, "valkey"));

    x = find(f, DB_ENGINE_PROMETHEUS, "prom");
    assert_non_null(x);
    assert_int_equal(x->confidence, DB_CONF_LIVE);
    assert_non_null(strstr(x->evidence, "block directories"));

    assert_non_null(find(f, DB_ENGINE_INFLUX1, "influx1"));
    x = find(f, DB_ENGINE_INFLUX2, "influx2");
    assert_non_null(x);
    /* Its bbolt file is its own, not a second finding (section 4). */
    assert_null(find(f, DB_ENGINE_BBOLT, "influx2/influxd.bolt"));
    assert_non_null(strstr(x->evidence, "\"also\""));

    x = find(f, DB_ENGINE_MONGO, "mongo");
    assert_non_null(x);
    assert_int_equal(x->confidence, DB_CONF_LIVE);
    assert_string_equal(x->optional, "mongodump");

    x = find(f, DB_ENGINE_ELASTIC, "es");
    assert_non_null(x);
    assert_int_equal(x->coverage, DB_COV_ENGINE_UNSAFE);
    assert_string_equal(x->suggestion, "exclude-rebuild");

    /* RocksDB over LevelDB in one directory, never both. */
    assert_non_null(find(f, DB_ENGINE_LEVELDB, "ldb"));
    assert_non_null(find(f, DB_ENGINE_ROCKSDB, "rocks"));
    assert_null(find(f, DB_ENGINE_LEVELDB, "rocks"));
    /* The loser's evidence is kept. */
    assert_non_null(strstr(find(f, DB_ENGINE_ROCKSDB, "rocks")->evidence, "\"also\":[\"leveldb"));

    assert_non_null(find(f, DB_ENGINE_LMDB, "lmdb"));
    assert_non_null(find(f, DB_ENGINE_BBOLT, "bolt/app.bolt"));
    x = find(f, DB_ENGINE_CLICKHOUSE, "ch");
    assert_non_null(x);
    assert_int_equal(x->confidence, DB_CONF_LIVE);
    x = find(f, DB_ENGINE_NEXTCLOUD, "nc");
    assert_non_null(x);
    /* Nextcloud's own version; the dbtype is the link's (B249). */
    assert_string_equal(x->version, "30.0.2.2");
    assert_non_null(strstr(x->evidence, "\"dbtype\":\"pgsql\""));
    assert_int_equal(x->coverage, DB_COV_NONE);

    assert_int_equal(count_engine(f, DB_ENGINE_SQLITE), 2);
    assert_int_equal(count_engine(f, DB_ENGINE_LEVELDB), 1);

    /* The lookup names the finding a path lies in. */
    const db_finding_t *in = db_findings_lookup(f, abs_of("pg/data/base/1/112"));
    assert_non_null(in);
    assert_int_equal(in->engine, DB_ENGINE_POSTGRES);
    assert_null(db_findings_lookup(f, abs_of("nowhere/at/all")));
    db_findings_free(f);
}

/* Section 4's traps. */
static void test_false_positives(void **state)
{
    (void)state;
    wtext("app/notes.db", "this is a text file named like a database, long enough");
    wtext("half/PG_VERSION", "16\n");             /* no global/pg_control */
    wempty("half/base/1/1");
    wtext("garbage/PG_VERSION", "not digits\n");  /* a PG layout whose version is not */
    wempty("garbage/global/pg_control");
    wdir("garbage/base");
    wtext("pgdata.bak/PG_VERSION", "14\n");       /* an old copy: no liveness */
    wempty("pgdata.bak/global/pg_control");
    wdir("pgdata.bak/base");
    wempty("sys/ibdata1");                         /* MyISAM only in system schemas */
    wdir("sys/#innodb_redo");
    wempty("sys/mysql/user.MYD");
    wempty("sys/performance_schema/x.MYI");
    wtext("dump/dump.rdb", "not redis");
    wtext("lm/data.mdb", "not lmdb, though the name says so....");
    wempty("lm/lock.mdb");
    wtext("lv/CURRENT", "garbage\n");
    wempty("lv/MANIFEST-000001");
    wempty("lv/000001.log");
    wempty("logs/app.log");                        /* *.log without CURRENT */
    wempty("prom2/wal/1");                         /* wal/ alone */
    wempty("prom2/meta.json");

    db_cap_root_t r = { TEST_DIR, DB_CAP_ATOMIC, 0 };
    db_capture_desc_t cap = { &r, 1, 0, 0, 0, 0 };
    db_findings_t *f = detect(TEST_DIR, &cap, NULL);
    assert_int_equal(count_engine(f, DB_ENGINE_SQLITE), 0);
    assert_null(find(f, DB_ENGINE_POSTGRES, "half"));
    assert_null(find(f, DB_ENGINE_POSTGRES, "garbage"));
    const db_finding_t *x = find(f, DB_ENGINE_POSTGRES, "pgdata.bak");
    assert_non_null(x);
    assert_int_equal(x->confidence, DB_CONF_DORMANT);
    assert_null(x->suggestion);
    assert_null(x->optional);
    x = find(f, DB_ENGINE_MYSQL, "sys");
    assert_non_null(x);
    assert_int_equal(x->confidence, DB_CONF_DORMANT);
    assert_null(find(f, DB_ENGINE_REDIS, "dump"));
    assert_null(find(f, DB_ENGINE_LMDB, "lm"));
    assert_null(find(f, DB_ENGINE_LEVELDB, "lv"));
    assert_int_equal(count_engine(f, DB_ENGINE_LEVELDB), 0);
    assert_int_equal(count_engine(f, DB_ENGINE_PROMETHEUS), 0);
    assert_int_equal(f->n, 2);
    db_findings_free(f);
}

/* A live read of a file source: uncovered, and what to do about it. */
static void test_coverage_live(void **state)
{
    (void)state;
    wtext("pg/PG_VERSION", "15\n");
    wempty("pg/global/pg_control");
    wdir("pg/base");
    wtext("pg/postmaster.pid", "1\n");
    wsqlite("app/a.db", 1, 4096);
    wempty("app/a.db-wal");
    db_cap_root_t r = { TEST_DIR, DB_CAP_LIVE, 0 };
    db_capture_desc_t cap = { &r, 1, 0, 0, 0, 0 };
    db_findings_t *f = detect(TEST_DIR, &cap, NULL);
    const db_finding_t *x = find(f, DB_ENGINE_POSTGRES, "pg");
    assert_non_null(x);
    assert_int_equal(x->coverage, DB_COV_UNCOVERED);
    assert_string_equal(x->method, "live-read");
    assert_string_equal(x->suggestion, "source-snapshot");
    assert_int_equal(x->root, 0);
    x = find(f, DB_ENGINE_SQLITE, "app/a.db");
    assert_non_null(x);
    assert_int_equal(x->coverage, DB_COV_UNCOVERED);
    assert_string_equal(x->suggestion, "sqlite-backup");
    db_findings_free(f);

    /* The same in a unit: the container's own dump template. */
    cap.unit = 1;
    f = detect(TEST_DIR, &cap, NULL);
    assert_string_equal(find(f, DB_ENGINE_POSTGRES, "pg")->suggestion, "pg-dumpall");
    assert_string_equal(find(f, DB_ENGINE_SQLITE, "app/a.db")->suggestion, "sqlite-backup");
    db_findings_free(f);

    /* With no capture described, nothing is judged. */
    f = detect(TEST_DIR, NULL, NULL);
    x = find(f, DB_ENGINE_POSTGRES, "pg");
    assert_non_null(x);
    assert_int_equal(x->coverage, DB_COV_NONE);
    assert_int_equal(x->root, -1);
    db_findings_free(f);
}

/* An unsettled file (OWN-45) under a data directory is liveness. */
static void test_unsettled_liveness(void **state)
{
    (void)state;
    wtext("pg/PG_VERSION", "16\n");
    wempty("pg/global/pg_control");
    wempty("pg/base/1/2619");
    wsqlite("app/b.db", 0, 4096);
    const char *uns[] = { abs_of("pg/base/1/2619"), abs_of("app/b.db"), NULL };
    db_cap_root_t r = { TEST_DIR, DB_CAP_LIVE, 0 };
    db_capture_desc_t cap = { &r, 1, 0, 0, 0, 0 };
    db_findings_t *f = detect(TEST_DIR, &cap, uns);
    const db_finding_t *x = find(f, DB_ENGINE_POSTGRES, "pg");
    assert_non_null(x);
    assert_int_equal(x->confidence, DB_CONF_LIVE);
    assert_int_equal(x->unsettled, 1);
    assert_non_null(strstr(x->evidence, "\"unsettled\":1"));
    /* A small SQLite that is live is listed, not counted. */
    x = find(f, DB_ENGINE_SQLITE, "app/b.db");
    assert_non_null(x);
    assert_int_equal(x->confidence, DB_CONF_LIVE);
    assert_int_equal(f->sqlite_small, 0);
    db_findings_free(f);
}

static void build_pg_with_ts(const char *target)
{
    wtext("a/pg/PG_VERSION", "16\n");
    wempty("a/pg/global/pg_control");
    wdir("a/pg/base");
    wtext("a/pg/postmaster.pid", "1\n");
    wdir("a/pg/pg_tblspc");
    wlink("a/pg/pg_tblspc/16400", target);
    wempty("b/ts/PG_16_202307071/16384/1");
}

/* Section 5: every one of the engine's files in the capture, at one instant. */
static void test_coverage_split(void **state)
{
    (void)state;
    build_pg_with_ts(abs_of("b/ts"));
    db_cap_root_t roots[2] = { { abs_of("a"), DB_CAP_ATOMIC, 0 }, { abs_of("b"), DB_CAP_ATOMIC, 0 } };
    char ra[PATH_MAX], rb[PATH_MAX];
    snprintf(ra, sizeof(ra), "%s", abs_of("a"));
    snprintf(rb, sizeof(rb), "%s", abs_of("b"));
    roots[0].root = ra;
    roots[1].root = rb;
    db_capture_desc_t cap = { roots, 2, 0, 0, 0, 1 };

    /* One instant: covered, the tablespace found. */
    db_findings_t *f = detect(TEST_DIR, &cap, NULL);
    const db_finding_t *x = find(f, DB_ENGINE_POSTGRES, "a/pg");
    assert_non_null(x);
    assert_int_equal(x->coverage, DB_COV_COVERED_CRASH);
    assert_non_null(strstr(x->evidence, "\"found\":\"in the capture\""));
    db_findings_free(f);

    /* Two instants (two pools, no pause): split. */
    roots[1].instant = 1;
    f = detect(TEST_DIR, &cap, NULL);
    x = find(f, DB_ENGINE_POSTGRES, "a/pg");
    assert_int_equal(x->coverage, DB_COV_SPLIT);
    assert_string_equal(x->suggestion, "pause-always");
    db_findings_free(f);

    /* The tablespace's root read live while the data was snapshotted. */
    roots[1].instant = 0;
    roots[1].kind = DB_CAP_LIVE;
    f = detect(TEST_DIR, &cap, NULL);
    assert_int_equal(find(f, DB_ENGINE_POSTGRES, "a/pg")->coverage, DB_COV_SPLIT);
    db_findings_free(f);

    /* Paused at the snapshot's instant: one point in time. */
    roots[1].kind = DB_CAP_PAUSED;
    f = detect(TEST_DIR, &cap, NULL);
    assert_int_equal(find(f, DB_ENGINE_POSTGRES, "a/pg")->coverage, DB_COV_COVERED_CRASH);
    db_findings_free(f);

    /* Only the data directory captured: the tablespace is outside. */
    db_capture_desc_t one = { roots, 1, 0, 0, 0, 0 };
    f = detect(ra, &one, NULL);
    x = find(f, DB_ENGINE_POSTGRES, "a/pg");
    assert_int_equal(x->coverage, DB_COV_SPLIT);
    assert_string_equal(x->suggestion, "source-snapshot");
    assert_non_null(strstr(x->evidence, "outside the capture"));
    db_findings_free(f);

    /* In a container the link is the container's path: noted, not judged. */
    one.container_paths = 1;
    one.unit = 1;
    f = detect(ra, &one, NULL);
    x = find(f, DB_ENGINE_POSTGRES, "a/pg");
    assert_int_equal(x->coverage, DB_COV_COVERED_CRASH);
    assert_non_null(strstr(x->evidence, "unchecked"));
    db_findings_free(f);

    /* A quiescing pre hook, a guest freeze. */
    cap.application = 1;
    roots[1].kind = DB_CAP_ATOMIC;
    f = detect(TEST_DIR, &cap, NULL);
    assert_int_equal(find(f, DB_ENGINE_POSTGRES, "a/pg")->coverage, DB_COV_COVERED_APPLICATION);
    db_findings_free(f);
    cap.application = 0;
    cap.quiesced = 1;
    f = detect(TEST_DIR, &cap, NULL);
    assert_int_equal(find(f, DB_ENGINE_POSTGRES, "a/pg")->coverage, DB_COV_COVERED_QUIESCED);
    db_findings_free(f);
}

/* A pg_wal relocated by symlink, found and not found. */
static void test_coverage_pg_wal(void **state)
{
    (void)state;
    wtext("a/pg/PG_VERSION", "16\n");
    wempty("a/pg/global/pg_control");
    wdir("a/pg/base");
    wtext("a/pg/postmaster.pid", "1\n");
    wdir("w/archive_status");
    char target[PATH_MAX];
    snprintf(target, sizeof(target), "%s/", abs_of("w"));   /* trailing slash */
    wlink("a/pg/pg_wal", target);
    char r0[PATH_MAX];
    snprintf(r0, sizeof(r0), "%s", TEST_DIR);
    db_cap_root_t r = { r0, DB_CAP_ATOMIC, 0 };
    db_capture_desc_t cap = { &r, 1, 0, 0, 0, 0 };
    db_findings_t *f = detect(TEST_DIR, &cap, NULL);
    const db_finding_t *x = find(f, DB_ENGINE_POSTGRES, "a/pg");
    assert_non_null(x);
    assert_int_equal(x->coverage, DB_COV_COVERED_CRASH);
    assert_non_null(strstr(x->evidence, "pg_wal"));
    db_findings_free(f);
    /* B245, a file source: the WAL is on disk, but outside every root
     * the backup reads. */
    char ra[PATH_MAX];
    snprintf(ra, sizeof(ra), "%s", abs_of("a"));
    db_cap_root_t ar = { ra, DB_CAP_ATOMIC, 0 };
    db_capture_desc_t only_a = { &ar, 1, 0, 0, 0, 0 };
    f = detect(TEST_DIR, &only_a, NULL);
    x = find(f, DB_ENGINE_POSTGRES, "a/pg");
    assert_int_equal(x->coverage, DB_COV_SPLIT);
    assert_string_equal(x->suggestion, "source-snapshot");
    assert_non_null(strstr(x->evidence, "\"kind\":\"pg_wal\""));
    assert_non_null(strstr(x->evidence, "\"found\":\"outside the capture\""));
    db_findings_free(f);
    rmrf(abs_of("w"));
    f = detect(TEST_DIR, &cap, NULL);
    assert_int_equal(find(f, DB_ENGINE_POSTGRES, "a/pg")->coverage, DB_COV_SPLIT);
    db_findings_free(f);
}

/* A file the cache vouched for keeps its classification. */
static void test_carry(void **state)
{
    (void)state;
    db_detect_cfg_t cfg = { 0 };
    db_detector_t *d = NULL;
    assert_int_equal(db_detect_begin(&cfg, &d), OK);
    db_detect_carry_add(d, "/x/app.db", DB_HEAD_CODE(DB_HEAD_SQLITE, 2));
    db_detect_carry_add(d, "/x/other", DB_HEAD_NONE);   /* ignored */
    db_detect_entry(d, "/x", 1, 0, 0, NULL);
    db_detect_entry(d, "/x/app.db", 0, 0, 5000, NULL);
    db_detect_entry(d, "/x/app.db-wal", 0, 0, 10, NULL);
    assert_int_equal(db_detect_cached(d, "/x/app.db", 5000), 1);
    assert_int_equal(db_detect_cached(d, "/x/nothing", 1), 0);
    db_findings_t *f = NULL;
    assert_int_equal(db_detect_finish(d, NULL, &f), OK);
    assert_int_equal(f->n, 1);
    assert_int_equal(f->v[0].engine, DB_ENGINE_SQLITE);
    assert_int_equal(f->v[0].confidence, DB_CONF_LIVE);
    db_findings_free(f);
    db_detect_free(d);
}

static void count_head(void *ctx, const char *path, uint32_t code)
{
    (void)path;
    (void)code;
    (*(int *)ctx)++;
}

static void test_heads_read(void **state)
{
    (void)state;
    db_detector_t *d = NULL;
    assert_int_equal(db_detect_begin(NULL, &d), OK);
    uint8_t sq[32] = "SQLite format 3";
    sq[18] = 1; sq[19] = 1;
    db_detect_head(d, "/a/x.db", sq, sizeof(sq), 100);
    db_detect_head(d, "/a/notes.txt", "hello", 5, 5);
    db_detect_carry_add(d, "/a/y.db", DB_HEAD_CODE(DB_HEAD_SQLITE, 1));
    assert_int_equal(db_detect_cached(d, "/a/y.db", 100), 1);
    int n = 0;
    db_detect_heads_read(d, count_head, &n);
    assert_int_equal(n, 1);
    /* NULL-safety of every entry point, which a backup with detection off
     * relies on. */
    db_detect_entry(NULL, "/a", 1, 0, 0, NULL);
    db_detect_head(NULL, "/a", sq, 1, 1);
    db_detect_head_fd(NULL, "/a", 0, 1);
    db_detect_unsettled(NULL, "/a");
    assert_int_equal(db_detect_cached(NULL, "/a", 0), 0);
    db_detect_heads_read(NULL, count_head, &n);
    db_findings_t *f = NULL;
    assert_int_not_equal(db_detect_finish(NULL, NULL, &f), OK);
    db_findings_free(NULL);
    db_detect_free(NULL);
    db_detect_free(d);
}

/* ------------------------------------------------------------------ */
/* Units (D1): units.probe liveness, hints and the Nextcloud link       */
/* ------------------------------------------------------------------ */

static void test_engine_maps(void **state)
{
    (void)state;
    const uint32_t PG = 1u << DB_ENGINE_POSTGRES, MY = 1u << DB_ENGINE_MYSQL,
                   RD = 1u << DB_ENGINE_REDIS, I1 = 1u << DB_ENGINE_INFLUX1,
                   I2 = 1u << DB_ENGINE_INFLUX2, ES = 1u << DB_ENGINE_ELASTIC,
                   CH = 1u << DB_ENGINE_CLICKHOUSE, MO = 1u << DB_ENGINE_MONGO,
                   PR = 1u << DB_ENGINE_PROMETHEUS, NC = 1u << DB_ENGINE_NEXTCLOUD;
    assert_int_equal(db_detect_process_engines("postgres", "postgres: checkpointer"), PG);
    assert_int_equal(db_detect_process_engines("postmaster", NULL), PG);
    assert_int_equal(db_detect_process_engines("mariadbd", ""), MY);
    assert_int_equal(db_detect_process_engines("mysqld", ""), MY);
    assert_int_equal(db_detect_process_engines("valkey-server", ""), RD);
    assert_int_equal(db_detect_process_engines("redis-server", ""), RD);
    assert_int_equal(db_detect_process_engines("influxd", ""), I1 | I2);
    assert_int_equal(db_detect_process_engines("mongod", ""), MO);
    assert_int_equal(db_detect_process_engines("prometheus", ""), PR);
    assert_int_equal(db_detect_process_engines("clickhouse-serv", ""), CH);
    assert_int_equal(db_detect_process_engines("java", "/usr/share/opensearch/jdk/bin/java -Xshare:auto"), ES);
    assert_int_equal(db_detect_process_engines("java", "java -jar app.jar"), 0);
    assert_int_equal(db_detect_process_engines("java", NULL), 0);
    assert_int_equal(db_detect_process_engines("nginx", ""), 0);
    assert_int_equal(db_detect_process_engines("", ""), 0);
    assert_int_equal(db_detect_process_engines(NULL, ""), 0);

    /* Registries, library/, tags and digests come off before matching. */
    assert_int_equal(db_detect_image_engines("postgres:16"), PG);
    assert_int_equal(db_detect_image_engines("docker.io/library/postgres:16-alpine"), PG);
    assert_int_equal(db_detect_image_engines("ghcr.io/immich-app/postgres:14-vectorchord0.4.3"), PG);
    assert_int_equal(db_detect_image_engines("tensorchord/pgvecto-rs:pg14-v0.2.0"), PG);
    assert_int_equal(db_detect_image_engines("timescale/timescaledb:latest-pg16"), PG);
    assert_int_equal(db_detect_image_engines("postgis/postgis"), PG);
    assert_int_equal(db_detect_image_engines("localhost:5000/mariadb:11@sha256:abcd"), MY);
    assert_int_equal(db_detect_image_engines("lscr.io/linuxserver/mariadb"), MY);
    assert_int_equal(db_detect_image_engines("MySQL:8.4"), MY);
    assert_int_equal(db_detect_image_engines("valkey/valkey:8"), RD);
    assert_int_equal(db_detect_image_engines("bitnami/redis"), RD);
    assert_int_equal(db_detect_image_engines("prom/prometheus"), PR);
    assert_int_equal(db_detect_image_engines("influxdb:1.8"), I1);
    assert_int_equal(db_detect_image_engines("influxdb:2.7"), I2);
    assert_int_equal(db_detect_image_engines("influxdb"), I2);
    assert_int_equal(db_detect_image_engines("mongo:7"), MO);
    assert_int_equal(db_detect_image_engines("mongodb/mongodb-community-server"), MO);
    assert_int_equal(db_detect_image_engines("opensearchproject/opensearch:2"), ES);
    assert_int_equal(db_detect_image_engines("elasticsearch:8.15.0"), ES);
    assert_int_equal(db_detect_image_engines("clickhouse/clickhouse-server"), CH);
    assert_int_equal(db_detect_image_engines("linuxserver/nextcloud"), NC);
    /* A name only containing an engine's name is not that engine. */
    assert_int_equal(db_detect_image_engines("redisinsight"), 0);
    assert_int_equal(db_detect_image_engines("adminer"), 0);
    assert_int_equal(db_detect_image_engines(""), 0);
    assert_int_equal(db_detect_image_engines(NULL), 0);

    assert_int_equal(db_detect_port_engines(5432), PG);
    assert_int_equal(db_detect_port_engines(3306), MY);
    assert_int_equal(db_detect_port_engines(6379), RD);
    assert_int_equal(db_detect_port_engines(9090), PR);
    assert_int_equal(db_detect_port_engines(8086), I1 | I2);
    assert_int_equal(db_detect_port_engines(27017), MO);
    assert_int_equal(db_detect_port_engines(9200), ES);
    assert_int_equal(db_detect_port_engines(8123), CH);
    assert_int_equal(db_detect_port_engines(9000), 0);
    assert_int_equal(db_detect_port_engines(80), 0);
}

/* A probe built by hand: one container per call to pc(). */
typedef struct { sp_unit_probe_t p; sp_probe_container_t c[4]; sp_probe_proc_t pr[4][4];
                 uint16_t po[4][4]; sp_probe_open_t op[4][8]; } probe_t;

static sp_probe_container_t *pc(probe_t *t, const char *name, const char *image)
{
    sp_probe_container_t *c = &t->c[t->p.n];
    memset(c, 0, sizeof(*c));
    snprintf(c->name, sizeof(c->name), "%s", name);
    snprintf(c->image, sizeof(c->image), "%s", image);
    c->running = 1;
    c->procs = t->pr[t->p.n];
    c->ports = t->po[t->p.n];
    c->open = t->op[t->p.n];
    t->p.v = t->c;
    t->p.n++;
    return c;
}

static void pproc(sp_probe_container_t *c, const char *comm, const char *args)
{
    snprintf(c->procs[c->n_procs].comm, SP_PROBE_COMM_MAX, "%s", comm);
    snprintf(c->procs[c->n_procs].args, SP_PROBE_ARGS_MAX, "%s", args);
    c->n_procs++;
}

static void popen_(sp_probe_container_t *c, const char *path, const char *comm)
{
    c->open[c->n_open].path = (char *)(uintptr_t)path;
    snprintf(c->open[c->n_open].comm, SP_PROBE_COMM_MAX, "%s", comm);
    c->n_open++;
}

static db_findings_t *detect_unit(const sp_unit_probe_t *p, const db_capture_desc_t *cap)
{
    db_detect_cfg_t cfg = { 1024 * 1024 };
    db_detector_t *d = NULL;
    assert_int_equal(db_detect_begin(&cfg, &d), OK);
    db_detect_unit(d, p);
    feed(d, TEST_DIR, NULL);
    db_findings_t *f = NULL;
    assert_int_equal(db_detect_finish(d, cap, &f), OK);
    db_detect_free(d);
    return f;
}

/* B245, a unit: pg_wal links to /wal, a container path. The probe's
 * mounts say where /wal is recorded, if anywhere, and the finding is split
 * when it is not in the capture, with the dump suggested: a pause cannot
 * bring in files the unit does not read. */
static void test_coverage_pg_wal_unit(void **state)
{
    (void)state;
    wtext("a/pg/PG_VERSION", "16\n");
    wempty("a/pg/global/pg_control");
    wdir("a/pg/base");
    wtext("a/pg/postmaster.pid", "1\n");
    wdir("w/archive_status");
    wlink("a/pg/pg_wal", "/wal");
    char ra[PATH_MAX], rw[PATH_MAX], data[PATH_MAX];
    snprintf(ra, sizeof(ra), "%s", abs_of("a"));
    snprintf(rw, sizeof(rw), "%s", abs_of("w"));
    snprintf(data, sizeof(data), "%s", abs_of("a/pg"));
    db_cap_root_t roots[2] = { { ra, DB_CAP_ATOMIC, 0 }, { rw, DB_CAP_ATOMIC, 0 } };
    db_capture_desc_t cap = { roots, 1, 0, 0, 1, 1 };

    probe_t t;
    memset(&t, 0, sizeof(t));
    sp_probe_container_t *c = pc(&t, "pg", "postgres:16");
    sp_probe_mount_t mounts[2] = {
        { (char *)(uintptr_t)"/var/lib/postgresql/data", data },
        { (char *)(uintptr_t)"/wal", NULL },    /* an excluded bind */
    };
    c->mounts = mounts;
    c->n_mounts = 2;

    /* The bind holding /wal is not backed up: split, the dump suggested. */
    db_findings_t *f = detect_unit(&t.p, &cap);
    const db_finding_t *x = find(f, DB_ENGINE_POSTGRES, "a/pg");
    assert_non_null(x);
    assert_int_equal(x->coverage, DB_COV_SPLIT);
    assert_string_equal(x->suggestion, "pg-dumpall");
    assert_non_null(strstr(x->evidence, "\"target\":\"/wal\""));
    assert_non_null(strstr(x->evidence, "\"found\":\"outside the capture\""));
    assert_non_null(strstr(x->evidence, "does not back up"));
    assert_null(strstr(x->evidence, "unchecked"));
    db_findings_free(f);

    /* Backed up, recorded at w, but w is no root this capture read. */
    mounts[1].recorded = rw;
    f = detect_unit(&t.p, &cap);
    x = find(f, DB_ENGINE_POSTGRES, "a/pg");
    assert_int_equal(x->coverage, DB_COV_SPLIT);
    assert_non_null(strstr(x->evidence, "outside the capture"));
    db_findings_free(f);

    /* Read with the data, at the same instant: covered, and the recorded
     * path named. */
    cap.n_roots = 2;
    f = detect_unit(&t.p, &cap);
    x = find(f, DB_ENGINE_POSTGRES, "a/pg");
    assert_int_equal(x->coverage, DB_COV_COVERED_CRASH);
    assert_non_null(strstr(x->evidence, "\"found\":\"in the capture\""));
    assert_non_null(strstr(x->evidence, "\"recorded\":\""));
    db_findings_free(f);
    /* At another instant: split, and a pause is what joins them. */
    roots[1].instant = 1;
    f = detect_unit(&t.p, &cap);
    x = find(f, DB_ENGINE_POSTGRES, "a/pg");
    assert_int_equal(x->coverage, DB_COV_SPLIT);
    assert_string_equal(x->suggestion, "pause-always");
    db_findings_free(f);
    roots[1].instant = 0;
    cap.n_roots = 1;

    /* No mount at /wal: the container's own filesystem, never backed up. */
    c->n_mounts = 1;
    f = detect_unit(&t.p, &cap);
    x = find(f, DB_ENGINE_POSTGRES, "a/pg");
    assert_int_equal(x->coverage, DB_COV_SPLIT);
    assert_non_null(strstr(x->evidence, "own filesystem"));
    db_findings_free(f);

    /* No mounts at all (an older helper): unchecked, as before. */
    c->n_mounts = 0;
    f = detect_unit(&t.p, &cap);
    x = find(f, DB_ENGINE_POSTGRES, "a/pg");
    assert_int_equal(x->coverage, DB_COV_COVERED_CRASH);
    assert_non_null(strstr(x->evidence, "unchecked"));
    db_findings_free(f);
}

static const db_finding_t *find_hint(const db_findings_t *f, db_engine_t e)
{
    for (size_t i = 0; i < f->n; i++)
        if (f->v[i].engine == e && f->v[i].confidence == DB_CONF_HINT) return &f->v[i];
    return NULL;
}

/* A running engine process is liveness; once one of its files is seen
 * held open for writing, only the data directories it holds are live, so
 * an old copy beside them stays dormant (section 4). */
static void test_probe_liveness(void **state)
{
    (void)state;
    wtext("pg/PG_VERSION", "16\n");
    wempty("pg/global/pg_control");
    wdir("pg/base/1");
    wbytes("pg/base/1/2619", "x", 1, 8192);
    wtext("pgdata.bak/PG_VERSION", "16\n");
    wempty("pgdata.bak/global/pg_control");
    wdir("pgdata.bak/base");
    wtext("rd/dump.rdb", "REDIS0011");
    wsqlite("app/state.db", 2, 4096);
    wsqlite("app/idle.db", 1, 2 * 1024 * 1024);

    db_cap_root_t r = { TEST_DIR, DB_CAP_ATOMIC, 0 };
    db_capture_desc_t cap = { &r, 1, 0, 0, 1, 1 };

    /* No probe: the file signals alone, all dormant. */
    db_findings_t *f = detect_unit(NULL, &cap);
    assert_int_equal(find(f, DB_ENGINE_POSTGRES, "pg")->confidence, DB_CONF_DORMANT);
    assert_int_equal(find(f, DB_ENGINE_REDIS, "rd")->confidence, DB_CONF_DORMANT);
    db_findings_free(f);

    /* Processes only: every finding of their engine is live. */
    probe_t t;
    memset(&t, 0, sizeof(t));
    sp_probe_container_t *c = pc(&t, "immich_postgres", "postgres:16");
    pproc(c, "postgres", "postgres -c shared_buffers=512MB");
    sp_probe_container_t *c2 = pc(&t, "immich_redis", "valkey/valkey:8");
    pproc(c2, "valkey-server", "valkey-server *:6379");
    f = detect_unit(&t.p, &cap);
    const db_finding_t *x = find(f, DB_ENGINE_POSTGRES, "pg");
    assert_int_equal(x->confidence, DB_CONF_LIVE);
    assert_int_equal(x->coverage, DB_COV_COVERED_CRASH);
    assert_non_null(strstr(x->evidence, "\"processes\":[{\"container\":\"immich_postgres\",\"comm\":\"postgres\"}]"));
    assert_int_equal(find(f, DB_ENGINE_POSTGRES, "pgdata.bak")->confidence, DB_CONF_LIVE);
    assert_int_equal(find(f, DB_ENGINE_REDIS, "rd")->confidence, DB_CONF_LIVE);
    /* SQLite has no process of its own: only an open file ties it. */
    assert_int_equal(find(f, DB_ENGINE_SQLITE, "app/idle.db")->confidence, DB_CONF_DORMANT);
    /* The images and processes matched a signature: no hints. */
    assert_null(find_hint(f, DB_ENGINE_POSTGRES));
    assert_null(find_hint(f, DB_ENGINE_REDIS));
    /* The unit's names go with a database a Nextcloud may name. */
    assert_non_null(strstr(x->evidence, "\"unit_names\":[\"immich_postgres\",\"immich_redis\"]"));
    assert_null(strstr(find(f, DB_ENGINE_REDIS, "rd")->evidence, "unit_names"));
    db_findings_free(f);

    /* With an open file: the held directory is live, the copy dormant, and
     * a SQLite held through its -wal is live. */
    popen_(c, abs_of("pg/base/1/2619"), "postgres");
    popen_(c, abs_of("app/idle.db-wal"), "node");
    popen_(c, abs_of("elsewhere/file"), "node");
    f = detect_unit(&t.p, &cap);
    x = find(f, DB_ENGINE_POSTGRES, "pg");
    assert_int_equal(x->confidence, DB_CONF_LIVE);
    assert_non_null(strstr(x->evidence, "\"open_write\":[{\"path\":"));
    assert_int_equal(find(f, DB_ENGINE_POSTGRES, "pgdata.bak")->confidence, DB_CONF_DORMANT);
    assert_int_equal(find(f, DB_ENGINE_SQLITE, "app/idle.db")->confidence, DB_CONF_LIVE);
    assert_int_equal(find(f, DB_ENGINE_SQLITE, "app/state.db") ? 1 : 0, 0);  /* small, dormant: counted */
    assert_int_equal(f->sqlite_small, 1);
    db_findings_free(f);
}

/* B252: listening ports are hint evidence on the findings the engine's
 * processes are on (section 3), never liveness, and a container with no
 * network, which listens on nothing, loses nothing by it. A java process
 * says which word made it Elasticsearch's or OpenSearch's. */
static void test_probe_ports(void **state)
{
    (void)state;
    wtext("pg/PG_VERSION", "16\n");
    wempty("pg/global/pg_control");
    wdir("pg/base/1");
    wtext("pgdata.bak/PG_VERSION", "16\n");
    wempty("pgdata.bak/global/pg_control");
    wdir("pgdata.bak/base");
    wbytes("pg/base/1/2619", "x", 1, 8192);
    wempty("influx1/meta/meta.db");
    wempty("influx1/data/db1/x.tsm");
    wempty("influx1/wal/db1/x.wal");
    wempty("es/_state/state-1.st");
    wempty("es/indices/Zx9rT1QwS0uQhY8b/0/index/segments_3");
    db_cap_root_t r = { TEST_DIR, DB_CAP_ATOMIC, 0 };
    db_capture_desc_t cap = { &r, 1, 0, 0, 1, 1 };

    /* --network none: processes, no ports. Live all the same. */
    probe_t t;
    memset(&t, 0, sizeof(t));
    sp_probe_container_t *pg = pc(&t, "pg", "postgres:16");
    pproc(pg, "postgres", "postgres");
    sp_probe_container_t *ix = pc(&t, "influx", "influxdb:1.8");
    pproc(ix, "influxd", "influxd");
    sp_probe_container_t *os = pc(&t, "search", "opensearchproject/opensearch:2");
    pproc(os, "java", "/usr/share/opensearch/jdk/bin/java -Xshare:auto -Dopensearch.path.home=x");
    db_findings_t *f = detect_unit(&t.p, &cap);
    const db_finding_t *x = find(f, DB_ENGINE_POSTGRES, "pg");
    assert_int_equal(x->confidence, DB_CONF_LIVE);
    assert_null(strstr(x->evidence, "\"ports\""));
    x = find(f, DB_ENGINE_ELASTIC, "es");
    assert_non_null(x);
    assert_int_equal(x->confidence, DB_CONF_LIVE);
    assert_non_null(strstr(x->evidence, "\"processes\":[{\"container\":\"search\",\"comm\":\"java\","
                                        "\"args_match\":\"opensearch\"}]"));
    db_findings_free(f);

    /* Listening: each engine's port goes on its findings. */
    pg->ports[pg->n_ports++] = 5432;
    ix->ports[ix->n_ports++] = 8086;
    ix->ports[ix->n_ports++] = 8088;
    os->ports[os->n_ports++] = 9200;
    f = detect_unit(&t.p, &cap);
    x = find(f, DB_ENGINE_POSTGRES, "pg");
    assert_non_null(strstr(x->evidence, "\"ports\":[{\"container\":\"pg\",\"port\":\"5432\"}]"));
    x = find(f, DB_ENGINE_INFLUX1, "influx1");
    assert_int_equal(x->confidence, DB_CONF_LIVE);
    assert_non_null(strstr(x->evidence, "\"ports\":[{\"container\":\"influx\",\"port\":\"8086\"}]"));
    assert_non_null(strstr(find(f, DB_ENGINE_ELASTIC, "es")->evidence, "\"port\":\"9200\""));
    /* No hint for what a signature already found. */
    assert_null(find_hint(f, DB_ENGINE_POSTGRES));
    db_findings_free(f);

    /* Once the process is seen holding one directory, the port goes with
     * the process, not on the old copy beside it. */
    popen_(pg, abs_of("pg/base/1/2619"), "postgres");
    f = detect_unit(&t.p, &cap);
    assert_non_null(strstr(find(f, DB_ENGINE_POSTGRES, "pg")->evidence, "\"port\":\"5432\""));
    x = find(f, DB_ENGINE_POSTGRES, "pgdata.bak");
    assert_int_equal(x->confidence, DB_CONF_DORMANT);
    assert_null(strstr(x->evidence, "\"ports\""));
    db_findings_free(f);

    /* A port with no process of its engine is evidence, never liveness. */
    pg->n_procs = 0;
    pg->n_open = 0;
    f = detect_unit(&t.p, &cap);
    x = find(f, DB_ENGINE_POSTGRES, "pg");
    assert_int_equal(x->confidence, DB_CONF_DORMANT);
    assert_non_null(strstr(x->evidence, "\"port\":\"5432\""));
    db_findings_free(f);
}

/* An image, port or process with no signature in the included mounts is
 * a hint, one per engine, with path "" and nothing judged. */
static void test_probe_hints(void **state)
{
    (void)state;
    wtext("pg/PG_VERSION", "16\n");
    wempty("pg/global/pg_control");
    wdir("pg/base");
    probe_t t;
    memset(&t, 0, sizeof(t));
    pc(&t, "pg", "postgres:16");                       /* matched by pg/ */
    sp_probe_container_t *c = pc(&t, "client", "mariadb:11");   /* a client only */
    c->ports[c->n_ports++] = 3306;
    c->ports[c->n_ports++] = 8086;
    c->ports[c->n_ports++] = 9000;
    sp_probe_container_t *c3 = pc(&t, "search", "");
    pproc(c3, "java", "/usr/share/elasticsearch/jdk/bin/java -Xms1g");
    db_cap_root_t r = { TEST_DIR, DB_CAP_ATOMIC, 0 };
    db_capture_desc_t cap = { &r, 1, 0, 0, 1, 1 };
    db_findings_t *f = detect_unit(&t.p, &cap);
    assert_null(find_hint(f, DB_ENGINE_POSTGRES));
    const db_finding_t *h = find_hint(f, DB_ENGINE_MYSQL);
    assert_non_null(h);
    assert_string_equal(h->path, "");
    assert_int_equal(h->coverage, DB_COV_NONE);
    assert_null(h->method);
    assert_null(h->suggestion);
    assert_int_equal(h->root, -1);
    assert_non_null(strstr(h->evidence, "\"images\":[{\"container\":\"client\",\"image\":\"mariadb:11\"}]"));
    assert_non_null(strstr(h->evidence, "\"ports\":[{\"container\":\"client\",\"port\":\"3306\"}]"));
    assert_non_null(strstr(h->evidence, "the image of client suggests MySQL/MariaDB"));
    /* 8086 fits both InfluxDBs; the hint names the current one. */
    assert_non_null(find_hint(f, DB_ENGINE_INFLUX2));
    assert_null(find_hint(f, DB_ENGINE_INFLUX1));
    /* 9000 is too common to mean ClickHouse. */
    assert_null(find_hint(f, DB_ENGINE_CLICKHOUSE));
    h = find_hint(f, DB_ENGINE_ELASTIC);
    assert_non_null(h);
    assert_non_null(strstr(h->evidence, "a running process of search suggests"));
    db_findings_free(f);

    /* An InfluxDB 1 data directory answers an InfluxDB 2 hint as well. */
    wempty("ix/meta/meta.db");
    wdir("ix/data");
    wdir("ix/wal");
    f = detect_unit(&t.p, &cap);
    assert_non_null(find(f, DB_ENGINE_INFLUX1, "ix"));
    assert_null(find_hint(f, DB_ENGINE_INFLUX2));
    db_findings_free(f);
}

static void wnextcloud(const char *dbtype, const char *dbhost)
{
    char buf[1024];
    snprintf(buf, sizeof(buf), "<?php\n$CONFIG = array (\n  'dbtype' => '%s',\n%s%s%s  'dbname' => 'nc',\n"
             "  'version' => '30.0.2.2',\n);\n",
             dbtype, dbhost ? "  'dbhost' => '" : "", dbhost ? dbhost : "", dbhost ? "',\n" : "");
    wtext("nc/config/config.php", buf);
}

static const db_finding_t *nc_of(const db_findings_t *f)
{
    const db_finding_t *x = find(f, DB_ENGINE_NEXTCLOUD, "nc");
    assert_non_null(x);
    return x;
}

static const char *nc_evidence(const db_findings_t *f)
{
    return nc_of(f)->evidence;
}

/* MariaDB's data directory, by its names. */
static void wmariadb(const char *rel)
{
    char p[256];
    snprintf(p, sizeof(p), "%s/ibdata1", rel);
    wempty(p);
    snprintf(p, sizeof(p), "%s/ib_logfile0", rel);
    wempty(p);
    snprintf(p, sizeof(p), "%s/mysql", rel);
    wdir(p);
}

/* The link (section 3): a dbhost in the same container or another
 * container of the unit is in this capture, and the database's data
 * directory found in the unit is judged with Nextcloud's files; anything
 * else is left for the host's other units (db_findings_link_nextcloud).
 * B249: the finding is live while Nextcloud runs, judged for coverage,
 * suggested, and its version is Nextcloud's, not its dbtype. */
static void test_nextcloud_link(void **state)
{
    (void)state;
    probe_t t;
    memset(&t, 0, sizeof(t));
    sp_probe_container_t *c = pc(&t, "nextcloud-app-1", "nextcloud:29");
    snprintf(c->aliases[c->n_aliases++], SP_PROBE_NAME_MAX, "app");
    pproc(c, "apache2", "apache2 -DFOREGROUND");
    sp_probe_container_t *db = pc(&t, "nextcloud-db-1", "mariadb:11");
    snprintf(db->aliases[db->n_aliases++], SP_PROBE_NAME_MAX, "db");
    sp_probe_container_t *px = pc(&t, "proxy", "caddy:2");
    pproc(px, "caddy", "caddy run");
    db_cap_root_t r = { TEST_DIR, DB_CAP_ATOMIC, 0 };
    db_capture_desc_t cap = { &r, 1, 0, 0, 1, 1 };

    /* The database container's data directory is not in the mounts. */
    wnextcloud("mysql", "db:3306");
    db_findings_t *f = detect_unit(&t.p, &cap);
    const db_finding_t *x = nc_of(f);
    assert_string_equal(x->version, "30.0.2.2");
    assert_int_equal(x->confidence, DB_CONF_LIVE);
    /* Its web server is the liveness; the proxy beside it is not. */
    assert_non_null(strstr(x->evidence, "\"processes\":[{\"container\":\"nextcloud-app-1\",\"comm\":\"apache2\"}]"));
    assert_non_null(strstr(x->evidence, "\"link\":{\"dbtype\":\"mysql\",\"dbhost\":\"db:3306\","
                                        "\"resolved\":\"this unit\",\"consistent\":false"));
    assert_int_equal(x->coverage, DB_COV_SPLIT);
    assert_non_null(strstr(x->coverage_why, "no data directory of it is in the captured mounts"));
    assert_null(x->suggestion);
    db_findings_free(f);

    /* With it, and its server running: one capture of both. */
    wmariadb("ncdb");
    pproc(db, "mariadbd", "mariadbd");
    f = detect_unit(&t.p, &cap);
    x = nc_of(f);
    assert_int_equal(x->confidence, DB_CONF_LIVE);
    assert_non_null(strstr(x->evidence, "\"resolved\":\"this unit\",\"databases\":[\""));
    assert_non_null(strstr(x->evidence, "\"consistent\":true"));
    assert_int_equal(x->coverage, DB_COV_COVERED_CRASH);
    assert_null(x->suggestion);
    assert_string_equal(x->optional, "nextcloud-maintenance");
    assert_string_equal(x->method, "snapshot");
    db_findings_free(f);

    /* Nextcloud stopped, its database running: the pair is still live. */
    c->n_procs = 0;
    f = detect_unit(&t.p, &cap);
    assert_int_equal(nc_of(f)->confidence, DB_CONF_LIVE);
    assert_int_equal(nc_of(f)->coverage, DB_COV_COVERED_CRASH);
    db_findings_free(f);
    /* Both stopped: at rest, nothing judged or suggested. */
    db->n_procs = 0;
    f = detect_unit(&t.p, &cap);
    assert_int_equal(nc_of(f)->confidence, DB_CONF_DORMANT);
    assert_int_equal(nc_of(f)->coverage, DB_COV_NONE);
    assert_null(nc_of(f)->optional);
    db_findings_free(f);
    /* Without a Nextcloud image in the unit, any web server or PHP serves
     * it. */
    pproc(px, "php-fpm83", "php-fpm: master process");
    snprintf(c->image, sizeof(c->image), "custom/cloud:1");
    f = detect_unit(&t.p, &cap);
    assert_int_equal(nc_of(f)->confidence, DB_CONF_LIVE);
    assert_non_null(strstr(nc_evidence(f), "\"comm\":\"php-fpm83\""));
    db_findings_free(f);
    snprintf(c->image, sizeof(c->image), "nextcloud:29");
    px->n_procs = 0;
    pproc(c, "apache2", "apache2 -DFOREGROUND");
    pproc(db, "mariadbd", "mariadbd");

    /* Its files and its database at two instants: split, pause the unit. */
    char ncroot[PATH_MAX], dbroot[PATH_MAX];
    snprintf(ncroot, sizeof(ncroot), "%s", abs_of("nc"));
    snprintf(dbroot, sizeof(dbroot), "%s", abs_of("ncdb"));
    db_cap_root_t two[2] = { { ncroot, DB_CAP_ATOMIC, 0 }, { dbroot, DB_CAP_ATOMIC, 1 } };
    db_capture_desc_t cap2 = { two, 2, 0, 0, 1, 1 };
    f = detect_unit(&t.p, &cap2);
    assert_int_equal(nc_of(f)->coverage, DB_COV_SPLIT);
    assert_string_equal(nc_of(f)->suggestion, "pause-always");
    db_findings_free(f);
    /* Read live: uncovered, and maintenance mode is what helps. */
    db_cap_root_t lr = { TEST_DIR, DB_CAP_LIVE, 0 };
    db_capture_desc_t capl = { &lr, 1, 0, 0, 1, 1 };
    f = detect_unit(&t.p, &capl);
    assert_int_equal(nc_of(f)->coverage, DB_COV_UNCOVERED);
    assert_string_equal(nc_of(f)->suggestion, "nextcloud-maintenance");
    db_findings_free(f);
    /* After a pre hook: application-consistent, nothing more to offer. */
    db_capture_desc_t capa = { &r, 1, 1, 0, 1, 1 };
    f = detect_unit(&t.p, &capa);
    assert_int_equal(nc_of(f)->coverage, DB_COV_COVERED_APPLICATION);
    assert_null(nc_of(f)->optional);
    db_findings_free(f);

    wnextcloud("pgsql", "localhost:/run/postgresql");
    f = detect_unit(&t.p, &cap);
    assert_non_null(strstr(nc_evidence(f), "\"resolved\":\"this container\""));
    /* pgsql names PostgreSQL, and there is none here. */
    assert_int_equal(nc_of(f)->coverage, DB_COV_SPLIT);
    db_findings_free(f);

    wnextcloud("mysql", "[::1]:3306");
    f = detect_unit(&t.p, &cap);
    assert_non_null(strstr(nc_evidence(f), "\"resolved\":\"this container\""));
    assert_int_equal(nc_of(f)->coverage, DB_COV_COVERED_CRASH);
    db_findings_free(f);

    /* Another unit's: judged on its files here, linked by the caller. */
    wnextcloud("mysql", "mariadb-shared");
    f = detect_unit(&t.p, &cap);
    assert_non_null(strstr(nc_evidence(f), "\"resolved\":null,\"host\":\"mariadb-shared\""));
    assert_int_equal(nc_of(f)->coverage, DB_COV_COVERED_CRASH);
    db_findings_free(f);

    /* Without the probe, a name cannot be found in the unit, and nothing
     * says Nextcloud runs. */
    wnextcloud("mysql", "db:3306");
    f = detect_unit(NULL, &cap);
    assert_non_null(strstr(nc_evidence(f), "\"resolved\":null,\"host\":\"db\""));
    assert_int_equal(nc_of(f)->confidence, DB_CONF_DORMANT);
    db_findings_free(f);

    /* A dbhost with anything odd in it is not read, and the database
     * cannot be placed. */
    wnextcloud("mysql", "db$(id)");
    f = detect_unit(&t.p, &cap);
    assert_non_null(strstr(nc_evidence(f), "\"note\":\"dbhost not read\""));
    assert_int_equal(nc_of(f)->coverage, DB_COV_SPLIT);
    db_findings_free(f);

    /* SQLite needs no link: its database is among its files. */
    wnextcloud("sqlite3", NULL);
    f = detect_unit(&t.p, &cap);
    assert_null(strstr(nc_evidence(f), "\"link\""));
    assert_int_equal(nc_of(f)->coverage, DB_COV_COVERED_CRASH);
    assert_string_equal(nc_of(f)->optional, "nextcloud-maintenance");
    db_findings_free(f);

    /* A config.php without a version has none, never the dbtype. */
    wtext("nc/config/config.php", "<?php\n$CONFIG = array (\n  'dbtype' => 'mysql',\n);\n");
    f = detect_unit(&t.p, &cap);
    assert_string_equal(nc_of(f)->version, "");
    db_findings_free(f);
}

static void reread_cb(void *ctx, const char *path)
{
    db_detector_t *d = ctx;
    int fd = open(path, O_RDONLY);
    assert_true(fd >= 0);
    struct stat st;
    assert_int_equal(fstat(fd, &st), 0);
    db_detect_head_fd(d, path, fd, (uint64_t)st.st_size);
    close(fd);
}

static void count_cb(void *ctx, const char *path)
{
    (void)path;
    (*(int *)ctx)++;
}

/* A config.php the cache vouched for has its code but not its dbhost: the
 * caller is asked to read it once, and then the link resolves. */
static void test_nextcloud_unread(void **state)
{
    (void)state;
    wnextcloud("mysql", "db");
    const char *p = abs_of("nc/config/config.php");
    db_detector_t *d = NULL;
    assert_int_equal(db_detect_begin(NULL, &d), OK);
    db_detect_carry_add(d, p, DB_HEAD_CODE(DB_HEAD_NEXTCLOUD, 2));
    assert_int_equal(db_detect_cached(d, p, 100), 1);
    probe_t t;
    memset(&t, 0, sizeof(t));
    sp_probe_container_t *c = pc(&t, "nc", "nextcloud");
    snprintf(c->aliases[c->n_aliases++], SP_PROBE_NAME_MAX, "db");
    db_detect_unit(d, &t.p);
    db_detect_nextcloud_unread(d, reread_cb, d);
    int calls = 0;
    db_detect_nextcloud_unread(d, count_cb, &calls);
    assert_int_equal(calls, 0);              /* read once, never again */
    db_findings_t *f = NULL;
    assert_int_equal(db_detect_finish(d, NULL, &f), OK);
    assert_non_null(strstr(nc_evidence(f), "\"resolved\":\"this unit\""));
    db_findings_free(f);
    db_detect_free(d);
    /* Inert without a detector or a reader. */
    db_detect_nextcloud_unread(NULL, count_cb, &calls);
    db_detect_unit(NULL, &t.p);
    assert_int_equal(calls, 0);
}

/* Entries arriving out of order and far apart merge into one directory. */
static void test_merge_order(void **state)
{
    (void)state;
    db_detector_t *d = NULL;
    assert_int_equal(db_detect_begin(NULL, &d), OK);
    db_detect_entry(d, "/d/PG_VERSION", 0, 0, 3, NULL);
    db_detect_entry(d, "/e/unrelated.log", 0, 0, 3, NULL);
    db_detect_entry(d, "/d/base", 1, 0, 0, NULL);
    db_detect_entry(d, "/f/x.sst", 0, 0, 3, NULL);
    db_detect_entry(d, "/d/global/pg_control", 0, 0, 3, NULL);
    db_detect_entry(d, "/PG_VERSION", 0, 0, 3, NULL);     /* a cluster at / */
    db_detect_entry(d, "/base", 1, 0, 0, NULL);
    db_detect_entry(d, "/global/pg_control", 0, 0, 3, NULL);
    db_findings_t *f = NULL;
    assert_int_equal(db_detect_finish(d, NULL, &f), OK);
    assert_int_equal(f->n, 2);
    assert_string_equal(f->v[0].path, "/");
    assert_string_equal(f->v[1].path, "/d");
    db_findings_free(f);
    db_detect_free(d);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_classify),
        cmocka_unit_test(test_vocabulary),
        cmocka_unit_test_setup_teardown(test_signatures, setup, teardown),
        cmocka_unit_test_setup_teardown(test_false_positives, setup, teardown),
        cmocka_unit_test_setup_teardown(test_coverage_live, setup, teardown),
        cmocka_unit_test_setup_teardown(test_unsettled_liveness, setup, teardown),
        cmocka_unit_test_setup_teardown(test_coverage_split, setup, teardown),
        cmocka_unit_test_setup_teardown(test_coverage_pg_wal, setup, teardown),
        cmocka_unit_test_setup_teardown(test_coverage_pg_wal_unit, setup, teardown),
        cmocka_unit_test(test_carry),
        cmocka_unit_test(test_heads_read),
        cmocka_unit_test(test_merge_order),
        cmocka_unit_test(test_engine_maps),
        cmocka_unit_test_setup_teardown(test_probe_liveness, setup, teardown),
        cmocka_unit_test_setup_teardown(test_probe_hints, setup, teardown),
        cmocka_unit_test_setup_teardown(test_probe_ports, setup, teardown),
        cmocka_unit_test_setup_teardown(test_nextcloud_link, setup, teardown),
        cmocka_unit_test_setup_teardown(test_nextcloud_unread, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
