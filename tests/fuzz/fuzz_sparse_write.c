/*
 * A sparse file, stored and reconstructed.
 *
 * Holes are metadata, and metadata is where this program's failures hide.
 * A sparse object is stored as a region table followed by only the data the
 * file actually holds, so everything about where those bytes belong lives in
 * a table that no hash covers until the whole object has been read. Get the
 * table wrong and the restored file has the right length, the right name and
 * the right byte count, with the contents at the wrong offsets.
 *
 * Neither half of that path was reachable from any other harness here:
 * write_sparse_object_stream because a fuzzer input is not a file with
 * holes, and restore_sparse_streaming because it is the only reader that
 * does not go through object_load. This drives both and compares the
 * reconstruction against the original file byte for byte, holes included.
 *
 * The source file has to live on a filesystem that reports holes. tmpfs on
 * this kernel does not - SEEK_HOLE returns EOF for a file that is almost
 * entirely empty - so a fixture written there is stored by the ordinary
 * dense path and every assertion below passes without the sparse writer
 * having run at all. Probe for somewhere that does and refuse to pretend
 * otherwise.
 *
 * Input layout: island count(1) + logical size(2, LE) + per-island offset
 * fraction(1 each) + payload seed.
 */
#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include "object.h"
#include "parity.h"
#include "repo_key.h"
#include "sparse_restore.h"
#include "store.h"
#include "types.h"
#include "fake_index.h"

#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define FUZZ_ROOT    "/tmp/c_backup_fuzz_sparse_write"
#define MAX_ISLANDS  8
#define ISLAND_BYTES 4096u

static store_t   *g_store;
static repo_key_t g_key;
static int        g_keyed = -1;
static char       g_srcdir[256];

/* Somewhere that actually produces holes, or nothing. */
static const char *hole_dir(void)
{
    static int probed;
    if (probed) return g_srcdir[0] ? g_srcdir : NULL;
    probed = 1;

    const char *cands[] = { getenv("CBACKUP_FUZZ_SPARSE_DIR"), "build", "." };
    for (size_t i = 0; i < sizeof(cands) / sizeof(cands[0]); i++) {
        if (!cands[i]) continue;
        char probe[300];
        snprintf(probe, sizeof(probe), "%s/.fuzz_sparse_probe", cands[i]);
        int fd = open(probe, O_CREAT | O_TRUNC | O_RDWR, 0644);
        if (fd < 0) continue;
        int holes = 0;
        if (ftruncate(fd, 1 << 20) == 0 && lseek(fd, 0, SEEK_SET) == 0 &&
            write(fd, "data", 4) == 4) {
            off_t h = lseek(fd, 0, SEEK_HOLE);
            holes = (h > 0 && h < (1 << 20));
        }
        close(fd);
        unlink(probe);
        if (holes) {
            snprintf(g_srcdir, sizeof(g_srcdir), "%s", cands[i]);
            return g_srcdir;
        }
    }
    return NULL;
}

static void reset_repo(int keyed)
{
    if (g_store) { store_close(g_store); g_store = NULL; }
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", FUZZ_ROOT);
    if (system(cmd) != 0) { /* first run */ }
    if (store_init_dirs(FUZZ_ROOT) != OK) abort();
    if (store_open(FUZZ_ROOT, NULL, &g_store) != OK) abort();
    fake_index_reset();
    fake_index_attach(g_store);
    if (keyed) {
        store_set_encrypted(g_store, 1);
        store_set_key(g_store, &g_key);
    }
    g_keyed = keyed;
}

static void setup_once(void)
{
    static int done;
    if (done) return;
    done = 1;
    rs_init();
    memset(&g_key, 0x5a, sizeof(g_key));
    repo_key_compute_id(&g_key);
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size < 8 || size > (16u << 10)) return 0;
    setup_once();
    const char *dir = hole_dir();
    if (!dir) return 0;   /* nothing here reports holes; do not pretend */

    unsigned nisl = (unsigned)(data[0] % MAX_ISLANDS) + 1u;
    int      keyed = (data[0] & 0x80u) != 0;
    /* Logical size in pages, so a hole is always at least one page and the
     * filesystem has something it can actually leave unallocated. */
    uint64_t fsize = ((uint64_t)((data[1] << 8) | data[2]) % 4096u + 16u) * 4096u;

    if (size < 3 + nisl + 1) return 0;
    const uint8_t *fracs = data + 3;
    const uint8_t *seed  = data + 3 + nisl;
    size_t         slen  = size - 3 - nisl;

    if (g_keyed != keyed) reset_repo(keyed);

    /* Build what the file should contain, then write only the islands so
     * everything between them is a hole. Comparing against this buffer is
     * what makes a misplaced region a failure rather than a curiosity. */
    uint8_t *want = calloc(1, fsize);
    if (!want) return 0;

    char path[512];
    snprintf(path, sizeof(path), "%s/fuzz_sparse_src.bin", dir);
    int fd = open(path, O_CREAT | O_TRUNC | O_RDWR, 0644);
    if (fd < 0) { free(want); return 0; }
    if (ftruncate(fd, (off_t)fsize) != 0) { close(fd); unlink(path); free(want); return 0; }

    for (unsigned i = 0; i < nisl; i++) {
        uint64_t span = fsize > ISLAND_BYTES ? fsize - ISLAND_BYTES : 0;
        uint64_t at   = span ? (span * fracs[i]) / 255u : 0;
        at &= ~(uint64_t)4095;                       /* page aligned */
        size_t n = ISLAND_BYTES < fsize - at ? ISLAND_BYTES : (size_t)(fsize - at);
        for (size_t j = 0; j < n; j++)
            want[at + j] = seed[(j + i * 7u) % slen];
        if (pwrite(fd, want + at, n, (off_t)at) != (ssize_t)n) {
            close(fd); unlink(path); free(want); return 0;
        }
    }

    uint8_t hash[OBJECT_HASH_SIZE];
    status_t st = object_store_file(g_store, fd, fsize, hash);
    close(fd);
    unlink(path);
    if (st != OK) { err_clear(); free(want); return 0; }

    /* Reconstruct through the path a restore uses, and require the result
     * to be the file that went in. */
    char outp[512];
    snprintf(outp, sizeof(outp), "%s/fuzz_sparse_out.bin", dir);
    int ofd = open(outp, O_CREAT | O_TRUNC | O_RDWR, 0644);
    if (ofd < 0) { free(want); return 0; }

    uint8_t otype = 0;
    uint64_t osz = 0;
    if (object_get_info(g_store, hash, &osz, &otype) != OK) abort();

    if (otype == OBJECT_TYPE_SPARSE) {
        if (restore_sparse_streaming(g_store, hash, ofd, fsize, outp) != OK) abort();
    } else {
        /* The prober decided it was dense - a legitimate outcome for a file
         * whose islands cover it - and then the ordinary reader owns it. */
        uint64_t sz = 0;
        if (object_load_stream(g_store, hash, ofd, &sz, NULL) != OK) abort();
        if (sz != fsize) abort();
    }

    struct stat sb;
    if (fstat(ofd, &sb) != 0) abort();
    if ((uint64_t)sb.st_size != fsize) abort();

    uint8_t *got = malloc(fsize);
    if (got) {
        if (pread(ofd, got, fsize, 0) != (ssize_t)fsize) abort();
        if (memcmp(got, want, fsize) != 0) abort();
        free(got);
    }

    close(ofd);
    unlink(outp);
    free(want);
    err_clear();
    return 0;
}
