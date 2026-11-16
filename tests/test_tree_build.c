#define _POSIX_C_SOURCE 200809L

/*
 * Tree building from a scan: byte-identical output, and linear cost.
 *
 * tree_build replaced a per-directory rescan of the whole scan (OWN-43): each
 * directory looked at every entry to find its children, so the phase was
 * O(directories x entries). It now buckets entries under their parent once.
 *
 * Two things can go wrong with a change like that and neither fails loudly.
 * A different tree for the same directory is a dedup regression: every
 * backup rewrites every tree and succeeds. And a quadratic path does not
 * fail either, it just never finishes on a large tree, which is what a real
 * 223,000-file backup did before the ordering pass was fixed. So this suite
 * pins both: the names of every tree in a synthetic scan against values
 * captured from the code before the change, and the number of entries the
 * build looks at.
 *
 * Synthetic rather than a real directory, because a tree carries uid, gid,
 * mtimes, inode identity and the absolute path's ancestors, none of which a
 * fixture on disk can hold constant across machines.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <cmocka.h>

#include "cdc.h"
#include "chunklist.h"
#include "manifest.h"
#include "object.h"
#include "scan.h"
#include "tree_build.h"

/* ---- synthetic scan fixture ---- */
static void fx_push(scan_result_t *s, const char *path, uint64_t id,
                    uint64_t parent, uint8_t type, uint32_t seed)
{
    if (s->count == s->capacity) {
        s->capacity = s->capacity ? s->capacity * 2 : 64;
        s->entries = realloc(s->entries, s->capacity * sizeof(*s->entries));
        assert_non_null(s->entries);
    }
    scan_entry_t *e = &s->entries[s->count++];
    memset(e, 0, sizeof(*e));
    e->path = strdup(path);
    assert_non_null(e->path);
    e->parent_node_id  = parent;
    e->node.node_id    = id;
    e->node.type       = type;
    e->node.mode       = (type == NODE_TYPE_DIR ? 040755u : 0100644u) ^ (seed & 7u);
    e->node.uid        = 1000u + (seed % 3u);
    e->node.gid        = 100u + (seed % 5u);
    e->node.size       = type == NODE_TYPE_REG ? (uint64_t)seed * 37u : 0;
    e->node.mtime_sec  = (seed % 11u == 0) ? -(int64_t)seed : 1700000000 + (int64_t)seed;
    e->node.mtime_nsec = (uint64_t)seed * 1000u % 1000000000u;
    e->node.inode_identity = 0x9E3779B97F4A7C15ULL * (seed + 1u);
    if (type == NODE_TYPE_REG) {
        for (int b = 0; b < OBJECT_HASH_SIZE; b++)
            e->node.content_hash[b] = (uint8_t)(seed * 31u + (uint32_t)b);
    }
    if (seed % 4u == 1u)
        for (int b = 0; b < OBJECT_HASH_SIZE; b++)
            e->node.xattr_hash[b] = (uint8_t)(seed + (uint32_t)b * 7u);
    if (seed % 6u == 2u)
        for (int b = 0; b < OBJECT_HASH_SIZE; b++)
            e->node.acl_hash[b] = (uint8_t)(seed ^ (uint32_t)b);
    if (type == NODE_TYPE_SYMLINK) {
        char t[64];
        snprintf(t, sizeof(t), "../target-%u", seed);
        e->symlink_target = strdup(t);
        assert_non_null(e->symlink_target);
        e->node.symlink.target_len = (uint32_t)strlen(t);
    }
    if (type == NODE_TYPE_CHR || type == NODE_TYPE_BLK) {
        e->node.device.major = seed % 256u;
        e->node.device.minor = seed % 17u;
    }
}

/*
 * Two source roots, a 40-deep chain, a 300-wide directory, empty
 * directories, every node type, a child listed before its parent, an orphan
 * whose parent is not in the scan and an entry hanging off a regular file.
 * The last two belong to no tree, under either implementation.
 */
static void fx_build(scan_result_t *s)
{
    memset(s, 0, sizeof(*s));
    char path[4096];
    uint64_t id = 1;
    uint32_t seed = 1;

    /* Listed first, parent (id 900000) appended at the end. */
    fx_push(s, "/srv/late/early-child.txt", 900001, 900000, NODE_TYPE_REG, seed++);

    fx_push(s, "/srv", id, 0, NODE_TYPE_DIR, seed++);
    uint64_t srv = id++;
    fx_push(s, "/home", id, 0, NODE_TYPE_DIR, seed++);
    uint64_t home = id++;

    /* Deep chain under /srv. */
    uint64_t parent = srv;
    int n = snprintf(path, sizeof(path), "/srv");
    for (int d = 0; d < 40; d++) {
        n += snprintf(path + n, sizeof(path) - (size_t)n, "/d%02d", d);
        fx_push(s, path, id, parent, NODE_TYPE_DIR, seed++);
        parent = id++;
        char f[4200];
        snprintf(f, sizeof(f), "%s/file", path);
        fx_push(s, f, id++, parent, NODE_TYPE_REG, seed++);
    }

    /* Wide directory under /home, names out of byte order. */
    fx_push(s, "/home/wide", id, home, NODE_TYPE_DIR, seed++);
    uint64_t wide = id++;
    for (int i = 0; i < 300; i++) {
        snprintf(path, sizeof(path), "/home/wide/%c%03d",
                 "zAa_9~"[i % 6], 299 - i);
        uint8_t t = (i % 10 == 3) ? NODE_TYPE_SYMLINK
                  : (i % 10 == 7) ? NODE_TYPE_DIR
                  : (i % 50 == 11) ? NODE_TYPE_CHR
                  : (i % 50 == 21) ? NODE_TYPE_BLK
                  : NODE_TYPE_REG;
        fx_push(s, path, id, wide, t, seed++);
        uint64_t me = id++;
        if (t == NODE_TYPE_DIR && i % 20 == 7) {
            char f[4200];
            snprintf(f, sizeof(f), "%s/inner", path);
            fx_push(s, f, id++, me, NODE_TYPE_REG, seed++);
        }
    }

    /* Empty directories at both roots. */
    fx_push(s, "/srv/empty", id++, srv, NODE_TYPE_DIR, seed++);
    fx_push(s, "/home/empty", id++, home, NODE_TYPE_DIR, seed++);

    /* An orphan, and an entry whose parent is a regular file. */
    fx_push(s, "/lost/orphan", id++, 777777, NODE_TYPE_REG, seed++);
    fx_push(s, "/home/afile", id, home, NODE_TYPE_REG, seed++);
    uint64_t afile = id++;
    fx_push(s, "/home/afile/impossible", id++, afile, NODE_TYPE_REG, seed++);

    /* The parent of the first entry, last. */
    fx_push(s, "/srv/late", 900000, srv, NODE_TYPE_DIR, seed++);
}

static void fx_free(scan_result_t *s)
{
    for (uint32_t i = 0; i < s->count; i++) {
        free(s->entries[i].path);
        free(s->entries[i].symlink_target);
    }
    free(s->entries);
    memset(s, 0, sizeof(*s));
}

/* ---- golden names: tree entry version 3 (OWN-45-LIVE flags byte) ---- */

static const struct { uint32_t index; const char *name; } golden[] = {
    {   1, "c887a898da43e3c9271be57f5bd7519fea04ea6d5e912d6c5012f3bbd1307e83" }, /* /srv */
    {   2, "75fddfa95598b7e14a8952d8db34eb5d9fb3bd89bd2c4742dc7a9c54ebc97f85" }, /* /home */
    {   3, "5076adfb1de135437438b69975ea8acd8ecab4473297036677474a99a1cc6343" }, /* /srv/d00 */
    {   5, "e844b30d64de572b503a43bdae50ac7f66a5cb4a779e741a386bdf6f2afa072d" }, /* /srv/d00/d01 */
    {   7, "cb17146527aec2a2d3810db82c884c4f1ec42a6471b3fca5c7b492fd2ffcf05b" }, /* /srv/d00/d01/d02 */
    {   9, "b6772cfb93a1d8d6cf952d7204f844107168ae914f9bda6fd19f0d47d3b4a751" }, /* /srv/d00/d01/d02/d03 */
    {  11, "8a12f8a4797e85ef07a280172cee475034227a135cbaba137d4ce8838c13482a" }, /* /srv/d00/d01/d02/d03/d04 */
    {  13, "d88af729b21c403a806077856dc227df3fb6ddd4e3bf0791ece91e5e3efaebcb" }, /* /srv/d00/d01/d02/d03/d04/d05 */
    {  15, "aa96181fc7e0c82a48ae2016a4ae7cae535a14fbf4e3fdf6adea654dbb13f65d" }, /* ...d00/d01/d02/d03/d04/d05/d06 */
    {  17, "5c403f0bd315ce1209b7663d36e9a4f63a32765bf1e606c24fd1ee8b16db8266" }, /* ...d01/d02/d03/d04/d05/d06/d07 */
    {  19, "c854d1c9beece303215e7f0d24ffe70247350eb40ced0939937ceb7bc81cc9f1" }, /* ...d02/d03/d04/d05/d06/d07/d08 */
    {  21, "d2e4328cdb1f069f0d40100c78031a700a9521c7de9bf4737719bf1eeaced80d" }, /* ...d03/d04/d05/d06/d07/d08/d09 */
    {  23, "bf33564de4e96fd9b47c40ecfc48f6053ced1f6a03937b52354a19391d00aecd" }, /* ...d04/d05/d06/d07/d08/d09/d10 */
    {  25, "153206b07f614b6221b3382c9dea643fcb7902b86e4afefd465be73613a6c641" }, /* ...d05/d06/d07/d08/d09/d10/d11 */
    {  27, "5c639b8e6dc438c13463941fbb73cf4011253f483bcbb790e4a61605370f3e70" }, /* ...d06/d07/d08/d09/d10/d11/d12 */
    {  29, "62f9a75024641c0f177e34f86a61ff7e3570e82b53cf84f7d461ad7f968dba95" }, /* ...d07/d08/d09/d10/d11/d12/d13 */
    {  31, "cd210bfdab644e9f25cb68269f9c239847d99582d3028859777ec58a81182e23" }, /* ...d08/d09/d10/d11/d12/d13/d14 */
    {  33, "b1767b24cb1c4f3dd9897cf14273efdf02c3150c6d4e61c71cb79ac4e325edcf" }, /* ...d09/d10/d11/d12/d13/d14/d15 */
    {  35, "22cd55569759e53095efd66a87dc0ed78bda2e6f8c87febfcc840685d908b823" }, /* ...d10/d11/d12/d13/d14/d15/d16 */
    {  37, "82edc5b72e6e0faa386aead20c7cbc4f2c894872263b76f18515a523b8f76428" }, /* ...d11/d12/d13/d14/d15/d16/d17 */
    {  39, "7c92708f7b8682ed9b70c992af3cb78c97ba5ca903fc07776a9cbb344aba9ee3" }, /* ...d12/d13/d14/d15/d16/d17/d18 */
    {  41, "7e6549aa58f6acd488a0a419276ade821ec20234a8e4f3040f1df8a27fb06a45" }, /* ...d13/d14/d15/d16/d17/d18/d19 */
    {  43, "5b8adf0de63e403bb84aa041d4e6265491a636ccba4eab916ee6abd0c5ebd874" }, /* ...d14/d15/d16/d17/d18/d19/d20 */
    {  45, "00c99c63d202eec983669ec0fd6c69609c7359a2b3aa0b67e2b471dcdb00f2df" }, /* ...d15/d16/d17/d18/d19/d20/d21 */
    {  47, "25e38a44bd7b985c1b8b75619b57fae2da29037e215e44650553f8970c26e073" }, /* ...d16/d17/d18/d19/d20/d21/d22 */
    {  49, "6d291f7028d24fff70f812dfdafc6a1bd7a746b4728c8fd206842af61df98b6b" }, /* ...d17/d18/d19/d20/d21/d22/d23 */
    {  51, "7930fc33c4ccaec98aea16aebcd61e1b06ad62846ee5c898c72ab47b7c94239a" }, /* ...d18/d19/d20/d21/d22/d23/d24 */
    {  53, "94d6cdafaddbee3dcd1ae96d9c85f069e9244431dfbee591c6271555d4ff1a67" }, /* ...d19/d20/d21/d22/d23/d24/d25 */
    {  55, "7a10f3e6da1ec8c49659a0bf903a4a92b263d7e8e2c64da231520c506bcb60e0" }, /* ...d20/d21/d22/d23/d24/d25/d26 */
    {  57, "42495e9c3a997d91db6796e0a4a1292127baa29c024cf5e2de4215f4ba09f181" }, /* ...d21/d22/d23/d24/d25/d26/d27 */
    {  59, "d20856ba3d414830fb03d74516a5936cb3c91a04112d3bc28b04a340fe809c6d" }, /* ...d22/d23/d24/d25/d26/d27/d28 */
    {  61, "aa8041cd20a6881e1b5e904802e593284acb0e670bb9446b8a0ed68343b08e39" }, /* ...d23/d24/d25/d26/d27/d28/d29 */
    {  63, "6f220679e7982e68bf7cf801c15b5a6b5815bdfd17234df4a083a1a938db52ab" }, /* ...d24/d25/d26/d27/d28/d29/d30 */
    {  65, "e731ee29baaf4d9911e434f02036901a9f58ebd7919d34199c99f4db25ec4b90" }, /* ...d25/d26/d27/d28/d29/d30/d31 */
    {  67, "34403df54e8e2deffa1f5ad13244ace4e79f65fb12b7204bf692ad1fa01623d4" }, /* ...d26/d27/d28/d29/d30/d31/d32 */
    {  69, "08b3f5a4f78964145ab47ab43f29059e84c0957c106963fcca59a198a3a5b980" }, /* ...d27/d28/d29/d30/d31/d32/d33 */
    {  71, "09b640ad3415154311c7a29c1ab072d39a9dc1edee6a8ee2e95ea726bc8688b1" }, /* ...d28/d29/d30/d31/d32/d33/d34 */
    {  73, "abf6c697de24bbfa2deeae0a175a20adf2d125d1a6c349f7bf54b7eec5bd9c07" }, /* ...d29/d30/d31/d32/d33/d34/d35 */
    {  75, "70f57380d8c1ef2d63bf4e52abb76672b2eb08cc0d181a81cdb76ef357cc16f8" }, /* ...d30/d31/d32/d33/d34/d35/d36 */
    {  77, "d714489492d897c95af7838d22ba60137caa54ab483ee91e5079f3f4937e0df7" }, /* ...d31/d32/d33/d34/d35/d36/d37 */
    {  79, "b339ccfc369c600396fa8597f69b3ec6b3055dff5e16388b914a1367eb9925e7" }, /* ...d32/d33/d34/d35/d36/d37/d38 */
    {  81, "921847f3a85f71dd128f84a1ae794e322191821f6037c05e901d673a4d6190ae" }, /* ...d33/d34/d35/d36/d37/d38/d39 */
    {  83, "c54949a67d4f42391a61bc202fcec5dce923c0bbf808d327345172092661a6e9" }, /* /home/wide */
    {  91, "68b6bd542f8a68f21c4f3e7b40912a0b320177b3c9c46209fa649222e9f3e102" }, /* /home/wide/A292 */
    { 102, "ea12f45712914a772acf09134a15ef762861391cf06459617c6879f01c869c87" }, /* /home/wide/~282 */
    { 112, "47324b5185d49068c81110a80dce17cd4fe31d4fdff75bce1632968296f0a54a" }, /* /home/wide/_272 */
    { 123, "ea12f45712914a772acf09134a15ef762861391cf06459617c6879f01c869c87" }, /* /home/wide/A262 */
    { 133, "7c1e169b4f651f9aee4d0d9f0e87e283a246c8648b84e9c6462e44bb07c216db" }, /* /home/wide/~252 */
    { 144, "ea12f45712914a772acf09134a15ef762861391cf06459617c6879f01c869c87" }, /* /home/wide/_242 */
    { 154, "1e7ff493388492cc4b4fc94f6e6aa4a70ec26261352519861331c765f0b60659" }, /* /home/wide/A232 */
    { 165, "ea12f45712914a772acf09134a15ef762861391cf06459617c6879f01c869c87" }, /* /home/wide/~222 */
    { 175, "7dd93d4e3c91817ae7082928cf11d9628cd1f6b5eeab316411a7671d5f95fdb8" }, /* /home/wide/_212 */
    { 186, "ea12f45712914a772acf09134a15ef762861391cf06459617c6879f01c869c87" }, /* /home/wide/A202 */
    { 196, "a98ce394c6f4cbe59a5d0adc86c73dab8b16a602469d85d3c3a063d8ab2560ab" }, /* /home/wide/~192 */
    { 207, "ea12f45712914a772acf09134a15ef762861391cf06459617c6879f01c869c87" }, /* /home/wide/_182 */
    { 217, "decda99e00db91a3ea18956b486b659516ab606eda63424dc0ce8fa76b818b67" }, /* /home/wide/A172 */
    { 228, "ea12f45712914a772acf09134a15ef762861391cf06459617c6879f01c869c87" }, /* /home/wide/~162 */
    { 238, "120ffeb4775bb247d8d8e0c3d58134ce9a7f2d2e5089e91e99b98676265cf8d3" }, /* /home/wide/_152 */
    { 249, "ea12f45712914a772acf09134a15ef762861391cf06459617c6879f01c869c87" }, /* /home/wide/A142 */
    { 259, "a862d0b1692a14fcd2bd512cf0f634b416bd511630866eaa3841e449bc115e66" }, /* /home/wide/~132 */
    { 270, "ea12f45712914a772acf09134a15ef762861391cf06459617c6879f01c869c87" }, /* /home/wide/_122 */
    { 280, "490469030a19f6a5db3cb8551ced89b2745eef293675e35311152fa6f802c61f" }, /* /home/wide/A112 */
    { 291, "ea12f45712914a772acf09134a15ef762861391cf06459617c6879f01c869c87" }, /* /home/wide/~102 */
    { 301, "9fc48f18adad414008c7dbe1eb1fcb8f2232a3c45e9661b2d6a0471cfd92e99d" }, /* /home/wide/_092 */
    { 312, "ea12f45712914a772acf09134a15ef762861391cf06459617c6879f01c869c87" }, /* /home/wide/A082 */
    { 322, "ac05c1dcaed4e7964dd255ccc5b9f3efb5e94db13585d945347847c9c01b04b2" }, /* /home/wide/~072 */
    { 333, "ea12f45712914a772acf09134a15ef762861391cf06459617c6879f01c869c87" }, /* /home/wide/_062 */
    { 343, "138d6f82fd4bdec41308c1761161a5eec819353dc8d5e6ea722c72e3ef22f4d6" }, /* /home/wide/A052 */
    { 354, "ea12f45712914a772acf09134a15ef762861391cf06459617c6879f01c869c87" }, /* /home/wide/~042 */
    { 364, "da05007a0fd705baa35c5e23377eb54f3be51c47736820bff6d474ed9bee4da8" }, /* /home/wide/_032 */
    { 375, "ea12f45712914a772acf09134a15ef762861391cf06459617c6879f01c869c87" }, /* /home/wide/A022 */
    { 385, "06b1743b27cc4f300dc9dc6e43a1c1be4b166e8cc9210a4c7eccf2c46cfcf8d9" }, /* /home/wide/~012 */
    { 396, "ea12f45712914a772acf09134a15ef762861391cf06459617c6879f01c869c87" }, /* /home/wide/_002 */
    { 399, "ea12f45712914a772acf09134a15ef762861391cf06459617c6879f01c869c87" }, /* /srv/empty */
    { 400, "ea12f45712914a772acf09134a15ef762861391cf06459617c6879f01c869c87" }, /* /home/empty */
    { 404, "d903fd2198434b0f6f385295ecb9c2f296c1a1fce4650cf3693a01a497fc1a40" }, /* /srv/late */
};

#define GOLDEN_ROOT \
    "c7406135ede1f4819723441863d4303e967c5d4a72c71f025da83e4f4c00ddb0"

/* ---- harness ---- */

/*
 * Stores a tree the way a backup does - manifest_store_tree, fragmenting a
 * directory past 256 KiB serialised - into a sink that only names objects,
 * so the names are the store's without a store.
 */
typedef struct {
    uint32_t puts;
    uint32_t fail_at_step;     /* 0: never */
    uint32_t steps;
    uint32_t objects;          /* objects the sink was handed */
    uint32_t lists;            /* of which chunk lists: fragmented directories */
} harness_t;

static status_t name_only_put(void *ctx, uint8_t type, const void *data,
                              size_t len, obj_put_t *out)
{
    harness_t *h = ctx;
    h->objects++;
    if (type == OBJECT_TYPE_CHUNKLIST) h->lists++;
    object_name(type, data, len, out->hash);
    out->is_new = 1;
    out->phys_bytes = len;
    return OK;
}

static status_t h_step(void *ctx, uint32_t done, uint32_t total)
{
    harness_t *h = ctx;
    (void)total;
    h->steps++;
    if (h->fail_at_step && done + 1 == h->fail_at_step)
        return set_error(ERR_BUSY, "stopped by the test");
    return OK;
}

static status_t h_put(void *ctx, uint32_t index, manifest_entry_t *entries,
                      uint32_t count, uint8_t out_hash[OBJECT_HASH_SIZE])
{
    harness_t *h = ctx;
    (void)index;
    h->puts++;
    cdc_params_t cp;
    cdc_params_default(&cp);
    obj_sink_t sink = { .put = name_only_put, .ctx = h };
    return manifest_store_tree(entries, count, &cp, &sink, out_hash, NULL);
}

typedef struct {
    uint8_t (*hashes)[OBJECT_HASH_SIZE];
    uint8_t  root[OBJECT_HASH_SIZE];
    tree_build_stats_t stats;
    harness_t h;
} run_t;

static status_t run(const scan_result_t *s, run_t *r)
{
    memset(r, 0, sizeof(*r));
    r->hashes = calloc(s->count ? s->count : 1, sizeof(*r->hashes));
    assert_non_null(r->hashes);
    tree_build_ops_t ops = {
        .step = h_step, .put = h_put, .ctx = &r->h,
        .hashes = r->hashes[0], .stride = OBJECT_HASH_SIZE,
    };
    return tree_build(s, &ops, r->root, &r->stats);
}

static void run_free(run_t *r)
{
    free(r->hashes);
    r->hashes = NULL;
}

static void hex(const uint8_t h[OBJECT_HASH_SIZE], char out[OBJECT_HASH_SIZE * 2 + 1])
{
    object_hash_to_hex(h, out);
}

/* ---- tests ---- */

/*
 * Every tree in the fixture names to a fixed table. A mismatch means a
 * directory that has not changed would get a new tree on the next backup:
 * a silent dedup regression, not a cosmetic one.
 *
 * The table was first captured from the pre-OWN-43 build_trees (backup.c
 * at 015fee8) and matched it exactly. It was recaptured once, deliberately,
 * when tree entries gained a flags byte and the tree version went to 3
 * (OWN-45-LIVE): that changes every tree's bytes, and format 3 was not yet
 * frozen. Only a format change may move these names.
 */
static void test_names_match_the_previous_build(void **state)
{
    (void)state;
    scan_result_t s;
    fx_build(&s);
    run_t r;
    assert_int_equal(run(&s, &r), OK);

    char got[OBJECT_HASH_SIZE * 2 + 1];
    uint32_t ndirs = 0;
    for (uint32_t i = 0; i < s.count; i++)
        if (s.entries[i].node.type == NODE_TYPE_DIR) ndirs++;
    assert_int_equal(ndirs, sizeof(golden) / sizeof(golden[0]));

    for (size_t g = 0; g < sizeof(golden) / sizeof(golden[0]); g++) {
        assert_true(golden[g].index < s.count);
        hex(r.hashes[golden[g].index], got);
        assert_string_equal(got, golden[g].name);
    }
    hex(r.root, got);
    assert_string_equal(got, GOLDEN_ROOT);

    run_free(&r);
    fx_free(&s);
}

/*
 * The order entries arrive in does not change any tree: reversing the scan
 * yields the same names. The build orders by depth and the serialiser by
 * name, so nothing about scan position may leak into the bytes.
 */
static void test_scan_order_does_not_change_names(void **state)
{
    (void)state;
    scan_result_t s;
    fx_build(&s);
    run_t fwd;
    assert_int_equal(run(&s, &fwd), OK);

    scan_result_t rev = s;
    rev.entries = malloc(s.count * sizeof(*rev.entries));
    assert_non_null(rev.entries);
    for (uint32_t i = 0; i < s.count; i++) rev.entries[i] = s.entries[s.count - 1 - i];
    run_t bwd;
    assert_int_equal(run(&rev, &bwd), OK);

    assert_memory_equal(fwd.root, bwd.root, OBJECT_HASH_SIZE);
    for (uint32_t i = 0; i < s.count; i++)
        if (s.entries[i].node.type == NODE_TYPE_DIR)
            assert_memory_equal(fwd.hashes[i], bwd.hashes[s.count - 1 - i], OBJECT_HASH_SIZE);

    run_free(&fwd);
    run_free(&bwd);
    free(rev.entries);
    fx_free(&s);
}

/*
 * Each entry is looked at once. The orphan and the entry under a regular
 * file belong to no tree, so they are the two the build never copies;
 * everything else is copied into exactly one tree, and every directory plus
 * the root is serialised once.
 */
static void test_each_entry_is_visited_once(void **state)
{
    (void)state;
    scan_result_t s;
    fx_build(&s);
    run_t r;
    assert_int_equal(run(&s, &r), OK);

    uint32_t ndirs = 0;
    for (uint32_t i = 0; i < s.count; i++)
        if (s.entries[i].node.type == NODE_TYPE_DIR) ndirs++;
    assert_int_equal(r.stats.entries_visited, s.count - 2);
    assert_int_equal(r.stats.trees, ndirs + 1);
    assert_int_equal(r.h.puts, ndirs + 1);
    assert_int_equal(r.h.steps, ndirs + 1);

    run_free(&r);
    fx_free(&s);
}

/* A step that fails stops the build there, with its status. */
static void test_a_failed_step_stops_the_build(void **state)
{
    (void)state;
    scan_result_t s;
    fx_build(&s);
    run_t r;
    memset(&r, 0, sizeof(r));
    r.hashes = calloc(s.count, sizeof(*r.hashes));
    assert_non_null(r.hashes);
    r.h.fail_at_step = 6;
    tree_build_ops_t ops = {
        .step = h_step, .put = h_put, .ctx = &r.h,
        .hashes = r.hashes[0], .stride = OBJECT_HASH_SIZE,
    };
    assert_int_equal(tree_build(&s, &ops, r.root, NULL), ERR_BUSY);
    assert_int_equal(r.h.puts, 5);
    run_free(&r);
    fx_free(&s);
}

/* No entries: one empty root tree, and no directory callbacks. */
static void test_empty_scan_builds_an_empty_root(void **state)
{
    (void)state;
    scan_result_t s;
    memset(&s, 0, sizeof(s));
    run_t r;
    assert_int_equal(run(&s, &r), OK);
    assert_int_equal(r.stats.trees, 1);
    assert_int_equal(r.stats.entries_visited, 0);
    assert_int_equal(r.h.steps, 1);
    run_free(&r);
}

static void test_bad_arguments_are_refused(void **state)
{
    (void)state;
    scan_result_t s;
    fx_build(&s);
    uint8_t root[OBJECT_HASH_SIZE];
    uint8_t one[OBJECT_HASH_SIZE];
    harness_t h;
    memset(&h, 0, sizeof(h));
    tree_build_ops_t ops = { .step = h_step, .put = h_put, .ctx = &h,
                             .hashes = one, .stride = OBJECT_HASH_SIZE - 1 };
    assert_int_equal(tree_build(&s, &ops, root, NULL), ERR_INVALID);
    ops.stride = OBJECT_HASH_SIZE;
    ops.put = NULL;
    assert_int_equal(tree_build(&s, &ops, root, NULL), ERR_INVALID);
    assert_int_equal(tree_build(NULL, &ops, root, NULL), ERR_INVALID);
    assert_int_equal(h.puts, 0);
    fx_free(&s);
}

/* ---- scale ---- */

/* dirs directories of files_per regular files each, under one source root. */
static void scale_build(scan_result_t *s, uint32_t dirs, uint32_t files_per)
{
    memset(s, 0, sizeof(*s));
    char path[128];
    uint64_t id = 1;
    uint32_t seed = 1;
    fx_push(s, "/data", id, 0, NODE_TYPE_DIR, seed++);
    uint64_t root = id++;
    for (uint32_t d = 0; d < dirs; d++) {
        snprintf(path, sizeof(path), "/data/d%06u", d);
        fx_push(s, path, id, root, NODE_TYPE_DIR, seed++);
        uint64_t dir = id++;
        for (uint32_t f = 0; f < files_per; f++) {
            snprintf(path, sizeof(path), "/data/d%06u/f%06u", d, f);
            fx_push(s, path, id++, dir, NODE_TYPE_REG, seed++);
        }
    }
}

static double now_s(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (double)t.tv_sec + (double)t.tv_nsec / 1e9;
}

/* Best of three: the noise on a shared runner only ever adds time. */
static double time_build(const scan_result_t *s, run_t *r)
{
    double best = 1e30;
    for (int k = 0; k < 3; k++) {
        double t0 = now_s();
        assert_int_equal(run(s, r), OK);
        double t = now_s() - t0;
        if (t < best) best = t;
        if (k < 2) run_free(r);
    }
    return best;
}

/*
 * The shape that made the rescan quadratic: many directories of few files.
 * At 25,000 directories by 10 files the old build compared about 6.9 billion
 * parent ids; this one looks at each of the 275,001 entries once.
 *
 * The visit count is the proof and is exact. The timing is a backstop for a
 * cost the counter does not see (a sort, a lookup gone linear): four times
 * the entries should take about four times as long, a quadratic path
 * sixteen. The bound of 10 sits between them with room for a noisy runner.
 */
static void test_many_small_directories_are_linear(void **state)
{
    (void)state;
    scan_result_t small, big;
    scale_build(&small, 6250, 10);
    scale_build(&big, 25000, 10);

    run_t r;
    double ts = time_build(&small, &r);
    assert_int_equal(r.stats.entries_visited, small.count);
    run_free(&r);
    double tb = time_build(&big, &r);
    assert_int_equal(r.stats.entries_visited, big.count);
    assert_int_equal(r.stats.trees, 25000 + 2);
    run_free(&r);

    printf("    trees: %u entries %.3f s, %u entries %.3f s, ratio %.2f\n",
           small.count, ts, big.count, tb, tb / ts);
    assert_true(tb / ts < 10.0);

    fx_free(&small);
    fx_free(&big);
}

/* The other extreme: one directory of a quarter of a million entries. */
static void test_one_huge_directory_is_linear(void **state)
{
    (void)state;
    scan_result_t s;
    scale_build(&s, 1, 250000);
    run_t r;
    double t = time_build(&s, &r);
    assert_int_equal(r.stats.entries_visited, s.count);
    assert_int_equal(r.stats.trees, 3);
    printf("    trees: 1 directory of 250000 entries %.3f s\n", t);
    run_free(&r);
    fx_free(&s);
}

/*
 * A directory past 256 KiB serialised is fragments and a chunk list
 * (OWN-34), and its name is the list's - not the name one TREE object of
 * the same entries would have had, by design. Its name does not depend on
 * scan order, and the small directory beside it keeps the name a single
 * TREE gives it.
 */
static void test_large_directory_fragments_deterministically(void **state)
{
    (void)state;
    scan_result_t a, b;
    memset(&a, 0, sizeof(a));
    memset(&b, 0, sizeof(b));
    /* 6,000 entries with long names: well past 256 KiB serialised. */
    char path[256];
    for (int pass = 0; pass < 2; pass++) {
        scan_result_t *s = pass ? &b : &a;
        fx_push(s, "/big", 1, 0, NODE_TYPE_DIR, 1);
        fx_push(s, "/small", 2, 0, NODE_TYPE_DIR, 2);
        fx_push(s, "/small/one.txt", 3, 2, NODE_TYPE_REG, 3);
        for (int k = 0; k < 6000; k++) {
            int i = pass ? 5999 - k : k;
            snprintf(path, sizeof(path),
                     "/big/a-rather-long-file-name-to-fill-the-tree-%06d.dat", i);
            fx_push(s, path, (uint64_t)(100 + i), 1, NODE_TYPE_REG, (uint32_t)(100 + i));
        }
    }

    run_t ra, rb;
    assert_int_equal(run(&a, &ra), OK);
    assert_int_equal(run(&b, &rb), OK);
    assert_true(ra.h.lists >= 1);
    assert_memory_equal(ra.root, rb.root, OBJECT_HASH_SIZE);
    assert_memory_equal(ra.hashes[0], rb.hashes[0], OBJECT_HASH_SIZE);

    /* /small is index 1 in both scans: one TREE object, named as such. */
    manifest_entry_t e;
    memset(&e, 0, sizeof(e));
    e.name = "one.txt";
    e.name_len = 7;
    e.type = NODE_TYPE_REG;
    const scan_entry_t *se = &a.entries[2];
    e.mode = se->node.mode; e.uid = se->node.uid; e.gid = se->node.gid;
    e.size = se->node.size; e.mtime_sec = se->node.mtime_sec;
    e.mtime_nsec = se->node.mtime_nsec; e.inode_identity = se->node.inode_identity;
    memcpy(e.target_hash, se->node.content_hash, OBJECT_HASH_SIZE);
    memcpy(e.xattr_hash, se->node.xattr_hash, OBJECT_HASH_SIZE);
    memcpy(e.acl_hash, se->node.acl_hash, OBJECT_HASH_SIZE);
    uint8_t *buf = NULL;
    size_t len = 0;
    assert_int_equal(manifest_serialise(&e, 1, &buf, &len), OK);
    uint8_t want[OBJECT_HASH_SIZE];
    object_name(OBJECT_TYPE_TREE, buf, len, want);
    free(buf);
    assert_memory_equal(ra.hashes[1], want, OBJECT_HASH_SIZE);

    /* The big one is not that: its name is its chunk list's. */
    run_free(&ra);
    run_free(&rb);
    fx_free(&a);
    fx_free(&b);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_names_match_the_previous_build),
        cmocka_unit_test(test_large_directory_fragments_deterministically),
        cmocka_unit_test(test_scan_order_does_not_change_names),
        cmocka_unit_test(test_each_entry_is_visited_once),
        cmocka_unit_test(test_a_failed_step_stops_the_build),
        cmocka_unit_test(test_empty_scan_builds_an_empty_root),
        cmocka_unit_test(test_bad_arguments_are_refused),
        cmocka_unit_test(test_many_small_directories_are_linear),
        cmocka_unit_test(test_one_huge_directory_is_linear),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
