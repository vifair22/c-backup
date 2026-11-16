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

/* ---- golden names, captured from backup.c at 015fee8 before OWN-43 ---- */

static const struct { uint32_t index; const char *name; } golden[] = {
    {   1, "d1f209b17a01e0db1c30169a8163ab26deec67bc295fb88514b5191c51209bc8" }, /* /srv */
    {   2, "d6d9d9e00eff70faaf9e516a0e7510678798296c6d46226d6c95bc30bc7e90ab" }, /* /home */
    {   3, "c4ac1a8acdeb7255953873609f8c3a85c5d2b5e3d73096682532c92c34026aba" }, /* /srv/d00 */
    {   5, "cdc6e9bdfcd4120b48925a221d7010cbdf34b54047c80d99057c5482e8c5d604" }, /* /srv/d00/d01 */
    {   7, "18a783354f521fcc5b0bf2e1958feffded57d39c9c43ea8e474191ae3a6b7efe" }, /* /srv/d00/d01/d02 */
    {   9, "bfde7fa64ba8fb3a55d90e296f9203efeac6ca0da8c48ee04210e4d7a396b519" }, /* /srv/d00/d01/d02/d03 */
    {  11, "dcd8cdf7417ba9c9927eb47b2f028a9913c0c1008d6044094b7102d605ddafe6" }, /* /srv/d00/d01/d02/d03/d04 */
    {  13, "41bae9412485ccde3160a2c78b6fc86a6385835d520ad9db5e2c0b1b7fcb6275" }, /* /srv/d00/d01/d02/d03/d04/d05 */
    {  15, "891c18893bbdcaee241e030cb233630cad1b2dc4c20c9f686d458f461ace1b07" }, /* ...d00/d01/d02/d03/d04/d05/d06 */
    {  17, "e9aacf520b3c51ba3aaa1acda4392cdae79bd51ff68c7cafd7b43dbec1efde50" }, /* ...d01/d02/d03/d04/d05/d06/d07 */
    {  19, "593721114d11838e3f6f085c972ade2730547679d29568c8b1addd2f8fcb3394" }, /* ...d02/d03/d04/d05/d06/d07/d08 */
    {  21, "ab677fd4af706b833781d5d0a22a9caf4678a8f9618c7dbe0e7c0714fa21e88e" }, /* ...d03/d04/d05/d06/d07/d08/d09 */
    {  23, "d3af7a50014e677fd6d0683fabb0a488846bef192a01b948ec3efe7ef60ab7fc" }, /* ...d04/d05/d06/d07/d08/d09/d10 */
    {  25, "ca46ca02b46d45c0548215f1ccfa45c1b6fb66182d953cbe534343b2e44e581d" }, /* ...d05/d06/d07/d08/d09/d10/d11 */
    {  27, "d5fbd132772e2787e40b677977a0d33f9988ace80e433a7f15c32634563160a6" }, /* ...d06/d07/d08/d09/d10/d11/d12 */
    {  29, "ce1bfeba3e5ff1b4aa63126c954f2e2781c513d7661755764aa2a5d6ff3c138d" }, /* ...d07/d08/d09/d10/d11/d12/d13 */
    {  31, "f4dd018a8ae7ae135b9bed770be99d2f6225bf0da0bf4615699b83d8d5e9260a" }, /* ...d08/d09/d10/d11/d12/d13/d14 */
    {  33, "bc46898c771b090af8a306518af3918c2512ea0dbaef34f3808099206847ce85" }, /* ...d09/d10/d11/d12/d13/d14/d15 */
    {  35, "47409d20ffd605a239a8f2965128d56751a32513d922baedd451147f71cac45f" }, /* ...d10/d11/d12/d13/d14/d15/d16 */
    {  37, "0aa965522ee373e8d6ed31d83a90f8edccdfaef808afc89a114dd555de6ba09e" }, /* ...d11/d12/d13/d14/d15/d16/d17 */
    {  39, "6c17ce677ec0337734355553befc4cea24525af4b4081fded4ca95c03a72a69a" }, /* ...d12/d13/d14/d15/d16/d17/d18 */
    {  41, "bc1db252e97c0508c289e9154ccd8edc7ec48874bf4f26e5c1372aba63de277a" }, /* ...d13/d14/d15/d16/d17/d18/d19 */
    {  43, "6172884b7e06b14acfa8cfafc9aeef298749ea57838806cc085d52f600eec595" }, /* ...d14/d15/d16/d17/d18/d19/d20 */
    {  45, "b6fac0e2a3a9e70e8ccaae4c0a09846022eca4b38ce4c809a2c8e228de662a9c" }, /* ...d15/d16/d17/d18/d19/d20/d21 */
    {  47, "57830e9e1d9d4b45e6a23f7f8789093d2ace842dd40498f3b9ad626a88240a20" }, /* ...d16/d17/d18/d19/d20/d21/d22 */
    {  49, "17fb32d6ccf13807fc6fff19241ddc7cd86993e7adf91ac4269e36d0be323c40" }, /* ...d17/d18/d19/d20/d21/d22/d23 */
    {  51, "84d2c422747551af0b50452d3b594e8738e655035574f497795928242440fa5b" }, /* ...d18/d19/d20/d21/d22/d23/d24 */
    {  53, "8fac3f2a684c911c3efeb225cdcdffe6fbe4b3ad8ab7f7f6109e295453903c53" }, /* ...d19/d20/d21/d22/d23/d24/d25 */
    {  55, "872f5f0194c90967cc099f307d12140d6e7bbae54d95659e43726b7c554f2804" }, /* ...d20/d21/d22/d23/d24/d25/d26 */
    {  57, "e61b1050ed41d34ceb8b39008feb3df92d01d5ac0978e2e40e7d4479dbf42723" }, /* ...d21/d22/d23/d24/d25/d26/d27 */
    {  59, "64c83049e84ebae369969402917ec3267ab6f71de03cf46b0e6086f7b2c47e03" }, /* ...d22/d23/d24/d25/d26/d27/d28 */
    {  61, "3d9d69b6dd317ae76edcf684ad8639dbdf4c707dc3325a42fdb6c694883f4034" }, /* ...d23/d24/d25/d26/d27/d28/d29 */
    {  63, "0cd1955e297b2f1460fb0cbbaf14a9113e2960018f4622523f93cc70859f2e5a" }, /* ...d24/d25/d26/d27/d28/d29/d30 */
    {  65, "b074b4e87a000a7ce0efa281bc1245ce59bde9542aec23c9625e8f32daf09bc5" }, /* ...d25/d26/d27/d28/d29/d30/d31 */
    {  67, "e9fe02c8f19a5ca76e17879fdbf265e1b9f6e1eb736e999d812588ad59aa5edd" }, /* ...d26/d27/d28/d29/d30/d31/d32 */
    {  69, "fca9b4bf513c717f33b55bb1338d53d0df40639a41b06193c75d06c2765c5d6c" }, /* ...d27/d28/d29/d30/d31/d32/d33 */
    {  71, "825e383dbda39b65fda265d3db3c7ffa631b80f007222cbf69a44455c3055fc5" }, /* ...d28/d29/d30/d31/d32/d33/d34 */
    {  73, "cb5f9b05876b6cd3058e18d95f39aed467710053f738fee4c25110eee69eec1b" }, /* ...d29/d30/d31/d32/d33/d34/d35 */
    {  75, "3b7cf07aa498a77971d3d1d7a39bd66c8f142dafe2c6b4dcbe4ed196cb332616" }, /* ...d30/d31/d32/d33/d34/d35/d36 */
    {  77, "51fba79338a7b4c493d852d874fa0d38953b2ff10904a80d1b286634ded45705" }, /* ...d31/d32/d33/d34/d35/d36/d37 */
    {  79, "29606d65a047ea0850356d5ae53e55f918c7af11c59f70e6181a00936fd1380d" }, /* ...d32/d33/d34/d35/d36/d37/d38 */
    {  81, "16c17be0c9289662d118886f1eecdce3181fe803fa5bf29cae09d3d189c42482" }, /* ...d33/d34/d35/d36/d37/d38/d39 */
    {  83, "078d56eeaa2ebc8a5a183aaa0310b32f2ef35b9cb44d40f63ed087ff6aeadee2" }, /* /home/wide */
    {  91, "a4a930b3cabd3e830bad1981612dcfa40d92592d96731eabc74e63684c6fa7b7" }, /* /home/wide/A292 */
    { 102, "c285608bbda53b2ad2d263a7a836a45672b6219018456549a9b8e64483683377" }, /* /home/wide/~282 */
    { 112, "b8d1505097a03e628f4dac5d639ef149a6f97b10742105fb17ec12a3e85750d6" }, /* /home/wide/_272 */
    { 123, "c285608bbda53b2ad2d263a7a836a45672b6219018456549a9b8e64483683377" }, /* /home/wide/A262 */
    { 133, "0ed570b0670d606787488b5687efa7c2be31d9ff651d975a603e1ed18f88305f" }, /* /home/wide/~252 */
    { 144, "c285608bbda53b2ad2d263a7a836a45672b6219018456549a9b8e64483683377" }, /* /home/wide/_242 */
    { 154, "0abdffaaec7728f85e6e4f356286b612fcad190cb4aee857627a3df409a1442e" }, /* /home/wide/A232 */
    { 165, "c285608bbda53b2ad2d263a7a836a45672b6219018456549a9b8e64483683377" }, /* /home/wide/~222 */
    { 175, "358de9ba02c1b5fc7bca8cfea59f24dad3af95eb2e35c521f24bfdb67377b6de" }, /* /home/wide/_212 */
    { 186, "c285608bbda53b2ad2d263a7a836a45672b6219018456549a9b8e64483683377" }, /* /home/wide/A202 */
    { 196, "9f74649c6eaa56f8c49e439ef0f761a952031f96fc09caf0023302d2aa9aa901" }, /* /home/wide/~192 */
    { 207, "c285608bbda53b2ad2d263a7a836a45672b6219018456549a9b8e64483683377" }, /* /home/wide/_182 */
    { 217, "53bea6355d2d028181dfcd3fe9393a84a5cae11e9f95bc89b2695d93dec4d596" }, /* /home/wide/A172 */
    { 228, "c285608bbda53b2ad2d263a7a836a45672b6219018456549a9b8e64483683377" }, /* /home/wide/~162 */
    { 238, "431dffcbf559ba690a8e4619a5ec4ec13f9cfcfc9a9ab0fa753ad12060963f55" }, /* /home/wide/_152 */
    { 249, "c285608bbda53b2ad2d263a7a836a45672b6219018456549a9b8e64483683377" }, /* /home/wide/A142 */
    { 259, "80a26ba2a595d75c0a16e6244af0fa5b0717da045498901c2bddd31155b19e20" }, /* /home/wide/~132 */
    { 270, "c285608bbda53b2ad2d263a7a836a45672b6219018456549a9b8e64483683377" }, /* /home/wide/_122 */
    { 280, "6b1143738b5a30fa938be5f9cf63933250f50bb7fe6ff9d2a0c8c72a973ad3d3" }, /* /home/wide/A112 */
    { 291, "c285608bbda53b2ad2d263a7a836a45672b6219018456549a9b8e64483683377" }, /* /home/wide/~102 */
    { 301, "66235b12ba8af7fa6189aaaf77b5f3581a884e173632e918bd58833633f49b59" }, /* /home/wide/_092 */
    { 312, "c285608bbda53b2ad2d263a7a836a45672b6219018456549a9b8e64483683377" }, /* /home/wide/A082 */
    { 322, "bfd345e5cb42870fb2d2be3d33e92b52399c2a8fd0ea3f0818fbecc107075d2d" }, /* /home/wide/~072 */
    { 333, "c285608bbda53b2ad2d263a7a836a45672b6219018456549a9b8e64483683377" }, /* /home/wide/_062 */
    { 343, "2165edd3acf6e1b15137a5f686c4803c0122d3e0127cdc3c9632e0171257e46a" }, /* /home/wide/A052 */
    { 354, "c285608bbda53b2ad2d263a7a836a45672b6219018456549a9b8e64483683377" }, /* /home/wide/~042 */
    { 364, "5b3eaaf6a6c1633db16e18efeadc37329dfc41b4a72ed3ebf0d442717150f1db" }, /* /home/wide/_032 */
    { 375, "c285608bbda53b2ad2d263a7a836a45672b6219018456549a9b8e64483683377" }, /* /home/wide/A022 */
    { 385, "bc8d22dddfa9e7fe3d33b6165609086ca01460273313988772659e691f73c33b" }, /* /home/wide/~012 */
    { 396, "c285608bbda53b2ad2d263a7a836a45672b6219018456549a9b8e64483683377" }, /* /home/wide/_002 */
    { 399, "c285608bbda53b2ad2d263a7a836a45672b6219018456549a9b8e64483683377" }, /* /srv/empty */
    { 400, "c285608bbda53b2ad2d263a7a836a45672b6219018456549a9b8e64483683377" }, /* /home/empty */
    { 404, "a5f03711c99d220ee29063770d074b36bf229dcb6cd914c695705cb06bf327c8" }, /* /srv/late */
};

#define GOLDEN_ROOT \
    "3831172033db87bfb4da96ef24c52da05990ead6093b6df97ba7e829e9c2a446"

/* ---- harness ---- */

/* Names a tree the way the store would, without a store. */
typedef struct {
    uint32_t puts;
    uint32_t fail_at_step;     /* 0: never */
    uint32_t steps;
} harness_t;

static status_t h_step(void *ctx, uint32_t done, uint32_t total)
{
    harness_t *h = ctx;
    (void)total;
    h->steps++;
    if (h->fail_at_step && done + 1 == h->fail_at_step)
        return set_error(ERR_BUSY, "stopped by the test");
    return OK;
}

static status_t h_put(void *ctx, uint32_t index, const uint8_t *buf, size_t len,
                      uint8_t out_hash[OBJECT_HASH_SIZE])
{
    harness_t *h = ctx;
    (void)index;
    h->puts++;
    object_name(OBJECT_TYPE_TREE, buf, len, out_hash);
    return OK;
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
 * Every tree in the fixture names to what the pre-OWN-43 build produced.
 * The table was captured by driving the old build_trees (backup.c at
 * 015fee8) over this exact fixture, before any of the change was written.
 * A mismatch here means a directory that has not changed would get a new
 * tree on the next backup: a silent dedup regression, not a cosmetic one.
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

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_names_match_the_previous_build),
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
