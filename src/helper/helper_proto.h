#pragma once

/*
 * The host helper's wire protocol (docs/source-providers.md 11.2 to 11.4),
 * shared by c-backup-helper and the daemon's client.
 *
 * AF_UNIX SOCK_SEQPACKET: one request, one response, each one packet of at
 * most HELPER_PACKET_MAX bytes holding one UTF-8 JSON object. Message
 * boundaries are the socket's, so there is no framing to get wrong.
 *
 *   {"v":1,"id":17,"op":"capture.begin","args":{...}}
 *   {"id":17,"ok":true,"result":{...}}
 *   {"id":17,"ok":false,"error":{"code":"denied","msg":"..."}}
 *
 * Validation is the helper's job and is strict: exactly the keys each
 * operation lists, every value of its type and inside its bounds, strings
 * valid UTF-8 with no NUL or control character. helper_request_parse does
 * all of it before an operation ever sees its arguments, which is what
 * makes it the fuzz target.
 */

#include "error.h"

#include <stddef.h>
#include <stdint.h>

#define HELPER_PROTO_VERSION 1
#define HELPER_PACKET_MAX    ((size_t)64 * 1024)

/* String bounds (11.4). */
#define HELPER_UNIT_KEY_MAX  160
#define HELPER_NAME_MAX      64
#define HELPER_PATH_MAX      4096
#define HELPER_TOKEN_HEX     32          /* 128-bit tokens, lowercase hex */

/* The closed set of error codes. */
typedef enum {
    HELPER_OK = 0,
    HELPER_E_DENIED,
    HELPER_E_INVALID,
    HELPER_E_BUSY,
    HELPER_E_TIMEOUT,
    HELPER_E_FAILED,
    HELPER_E_UNSUPPORTED,
} helper_err_t;

const char *helper_err_name(helper_err_t e);
/* -1 for a name outside the set. */
int         helper_err_parse(const char *name, helper_err_t *out);

/* The complete operation list for 1.0 (11.3). Anything else is denied. */
typedef enum {
    HOP_HELLO = 0,
    HOP_UNITS_LIST,
    HOP_UNITS_DESCRIBE,
    HOP_CAPTURE_BEGIN,
    HOP_CAPTURE_BEGIN_PATHS,
    HOP_CAPTURE_END,
    HOP_HOOK_RUN,
    HOP_HOOK_WAIT,
    HOP_VM_BACKUP_BEGIN,
    HOP_VM_BACKUP_END,
    HOP_VM_CHECKPOINT_REDEFINE,
    HOP_VM_INSTALL_STATE,
    HOP_RESTORE_STAGE,
    HOP_RESTORE_SWAP,
    HOP_UNIT_STOP,
    HOP_UNIT_START,
    HOP_UNIT_DEFINE,
    HOP_LEASE_RELEASE,
    HOP_SWEEP,
    HOP_COUNT
} helper_op_t;

const char *helper_op_name(helper_op_t op);

/* Providers the protocol knows (the enum of units.list). */
#define HELPER_PROVIDERS "libvirt, docker, lxc, fake"
int helper_provider_known(const char *p);

/* ---- parsed arguments: one struct, the fields an operation uses ---- */

#define HELPER_UNITS_MAX  32
#define HELPER_PATHS_MAX  64
#define HELPER_DISKS_MAX  32

typedef struct {
    char target[HELPER_NAME_MAX + 1];
    int  incremental;
} helper_disk_arg_t;

typedef struct {
    helper_op_t op;
    int64_t     id;
    /* hello */
    int64_t     client_version;
    /* units.list */
    char        provider[16];
    /* unit, units */
    char        unit[HELPER_UNIT_KEY_MAX + 1];
    char        units[HELPER_UNITS_MAX][HELPER_UNIT_KEY_MAX + 1];
    size_t      n_units;
    int         pause;
    int         hold;        /* capture.begin: keep a pause for the daemon's second pass */
    /* capture.begin_paths */
    char       *paths[HELPER_PATHS_MAX];
    size_t      n_paths;
    /* tokens: capture, run, session, staging */
    char        token[HELPER_TOKEN_HEX + 1];
    /* hook.run */
    char        hook[HELPER_NAME_MAX + 1];
    /* vm.backup_begin */
    helper_disk_arg_t disks[HELPER_DISKS_MAX];
    size_t      n_disks;
    char        base[HELPER_NAME_MAX + 1];     /* "" for null */
    char        freeze[16];                    /* auto, off, required */
    /* vm.backup_end */
    char        outcome[16];                   /* committed, aborted */
    /* vm.checkpoint_redefine */
    char        checkpoint[HELPER_NAME_MAX + 1];
    /* restore.stage */
    char        mount[HELPER_PATH_MAX + 1];
    int64_t     job;
    /* unit.define: the definition, still JSON text, parsed by the backend */
    char       *definition;
} helper_req_t;

/*
 * Parse and validate one request packet. On failure *err is INVALID (or
 * DENIED for an operation outside the list) with a message in why, and
 * *req holds whatever id could be read (0 otherwise) so the response can
 * still carry it. Always call helper_req_free.
 */
helper_err_t helper_request_parse(const void *buf, size_t len, helper_req_t *req,
                                  char *why, size_t wsz);
void         helper_req_free(helper_req_t *req);

/* Unit key grammar per provider (11.4); 1 when well formed. Splits into
 * provider and id when the outputs are given. */
int helper_unit_key_ok(const char *key, char *provider, size_t psz,
                       char *id, size_t isz);

/* A clean absolute path: no "//", ".", "..", trailing "/", control bytes. */
int helper_path_clean(const char *p);

/* ---- transport ---- */

/* The largest configuration blob units.describe passes in its sealed memfd
 * (items carry data_off and data_len into it). */
#define HELPER_BLOB_MAX ((size_t)16 * 1024 * 1024)

/* One packet, and optionally one fd as SCM_RIGHTS. ERR_IO on failure. */
status_t helper_send(int fd, const void *buf, size_t len, int pass_fd);
/* One packet into buf (cap bytes, NUL appended, so cap - 1 data); *got is
 * the length; *recv_fd (may be NULL) gets a passed fd or -1. A packet that
 * does not fit is ERR_TOO_LARGE; EOF is ERR_NOT_FOUND. */
status_t helper_recv(int fd, char *buf, size_t cap, size_t *got, int *recv_fd);

/* Lowercase hex of n random bytes into out (2n + 1). */
status_t helper_random_hex(char *out, size_t nbytes);
/* Hex encode / decode for small binary values (blob data). decode returns
 * the length, or -1. */
void     helper_hex_encode(const uint8_t *in, size_t n, char *out);
long     helper_hex_decode(const char *in, uint8_t *out, size_t cap);
