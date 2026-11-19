#pragma once

/*
 * The helper server's state, shared by helper_server.c (the protocol, the
 * captures, the watchdog) and helper_journal.c (the hold journal, its
 * markers and the recovery from a lost journal, B285). Not an interface:
 * nothing outside src/helper includes it.
 */

#include "helper_server.h"
#include "helper_backend.h"
#include "helper_proto.h"
#include "hooklog.h"
#include "snap.h"

#include <limits.h>
#include <pthread.h>
#include <stdint.h>
#include <sys/types.h>
#include <time.h>

#define HOLDS_MAX     256
#define CAPTURES_MAX  64
#define RUNS_MAX      32
#define STAGINGS_MAX  32
#define CONNS_MAX     16

/* ------------------------------------------------------------------ */
/* State                                                               */
/* ------------------------------------------------------------------ */

typedef struct {
    int64_t id;
    char    unit[HELPER_UNIT_KEY_MAX + 1];  /* provider:id */
    char    kind[16];
    char    detail[192];
    int64_t created_ms;                     /* wall clock; 0 after a restart */
    int64_t capture;                        /* the capture's sequence number */
    int     busy;                           /* a release in flight; not persisted */
    /* Releases that failed: retried with a backoff, never dropped, and
     * reported to the daemon by the sweep (B175). Not persisted. */
    int     tries;
    int64_t next_ms, fail_since_ms;
    char    last_err[SP_RELEASE_ERR_MAX];
} hold_t;

struct jctx;

/* Wall-clock milliseconds: a hold's age against max_pause_s. In whole
 * seconds the watchdog fired up to a second early. */
static inline int64_t now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

typedef struct {
    char    unit[HELPER_UNIT_KEY_MAX + 1];
    const helper_backend_t *b;
    sp_session_t *sess;
    struct jctx  *j;          /* the session's journal context, freed after end */
} member_t;

typedef struct {
    int      used;
    char     token[HELPER_TOKEN_HEX + 1];
    int64_t  seq;
    time_t   deadline;
    member_t members[HELPER_UNITS_MAX];
    size_t   n_members;
    /* The daemon session that began it, and whether that session held an
     * anchor connection then: only such a capture ends with its session
     * (B169); one begun with no anchor (a client that keeps none) lives
     * to its lease as before. */
    char     session[HELPER_TOKEN_HEX + 1];
    int      anchored;
    /* capture.begin_paths: the snapshots it took (NULL: none), released
     * at its end through pj's journal, with pb's snapshot context. */
    snap_set_t             *pset;
    struct jctx            *pj;
    const helper_backend_t *pb;
} capture_t;

/* A daemon session: the daemon process, by the token its hellos carry,
 * and how many anchor connections it has open. */
typedef struct {
    char     token[HELPER_TOKEN_HEX + 1];
    int      anchors;
} dsession_t;

typedef struct {
    int      used;
    char     token[HELPER_TOKEN_HEX + 1];
    pid_t    pid;             /* a local process, or */
    const helper_backend_t *b;/* a run inside the unit, through its backend */
    helper_run_t *hr;
    int      err_fd;          /* the hook's stderr, for the log */
    time_t   deadline;
    int      done, status, timed_out;
    int      cancelled;       /* hook.wait with kill: the daemon's job was cancelled (B248) */
    int64_t  term_ms;         /* when SIGTERM went to its group, monotonic; 0 not yet */
    int      killed;          /* and SIGKILL, HOOK_KILL_GRACE_S after */
    hooklog_t log;            /* stderr, head and tail kept (6.6) */
} run_t;

typedef struct {
    int      used;
    char     token[HELPER_TOKEN_HEX + 1];
    char     unit[HELPER_UNIT_KEY_MAX + 1];
    char     mount[HELPER_PATH_MAX + 1];  /* as recorded */
    char     real[HELPER_PATH_MAX + 1];   /* where it is on the host: what is swapped (B178) */
    char     path[HELPER_PATH_MAX + 1];
    int64_t  job;
    int      swapped;         /* swapped in: kept for an undo until the slot is wanted */
    int      busy;            /* a swap or undo is in progress on it */
    int      had;             /* 1 the mount existed and was moved to old; -1 the
                                 old data stayed in the staging (no undo) */
    char     old[HELPER_PATH_MAX + 1];
    int      file;            /* the mount is a disk image: <path>/image replaces it */
    /* Pinned at stage (B171): the swap renames through these fds and
     * checks the path still names them, so nothing done to the path in
     * between can redirect it. Open from stage to swap, -1 after. */
    int      base_fd, job_fd;
    dev_t    base_dev, job_dev;
    ino_t    base_ino, job_ino;
    char     dom[HELPER_PATH_MAX + 1];   /* the domain the base is opened from */
    char     base[HELPER_PATH_MAX + 1];
    char     job_name[64];
} staging_t;

struct helper_server {
    const helper_policy_t *pol;
    helper_backend_t       backends[HELPER_BACKENDS_MAX];
    size_t                 nb;
    pthread_mutex_t        mu;          /* everything below but the journal */
    capture_t              caps[CAPTURES_MAX];
    run_t                  runs[RUNS_MAX];
    staging_t              stages[STAGINGS_MAX];
    dsession_t             sessions[CONNS_MAX];
    time_t                 recent[3600];  /* capture start times, a ring */
    size_t                 n_recent;
    int64_t                seq;

    pthread_mutex_t        jmu;         /* the hold journal */
    hold_t                 holds[HOLDS_MAX];
    size_t                 n_holds;
    int64_t                next_hold;

    /* A start whose journal could not be used (B285): set by holds_load
     * before the boot thread exists, the report by boot_main before
     * boot_done, and read-only after. */
    int                    recovering;
    char                   recovery_why[PATH_MAX + 1024]; /* what was wrong with it */
    char                   recovery_aside[PATH_MAX + 128]; /* where it was kept; "" none */
    char                   recovery_id[96];
    char                  *recovery_msg;             /* the report, for hello and the alert */

    int                    lfd;
    pthread_t              accept_th, wd_th, boot_th;
    int                    accept_on, wd_on, boot_on, stop;
    int                    boot_done;   /* the start-up sweep finished; under cmu */
    pthread_mutex_t        cmu;
    pthread_cond_t         wcv;
    int                    conns[CONNS_MAX];
    pthread_t              conn_th[CONNS_MAX];
    int                    conn_live[CONNS_MAX];
    unsigned               wd_ms;
};

typedef struct jctx {
    helper_server_t *s;
    char             unit[HELPER_UNIT_KEY_MAX + 1];
    int64_t          capture;
} jctx_t;
