#ifndef CBACKUP_JSON_ROW_H
#define CBACKUP_JSON_ROW_H

/*
 * Database rows to typed JSON.
 *
 * c-utils hands every column back as text, and the routes used to pass that
 * text straight to json_*_add_str, so every id, count, size and timestamp
 * reached a client as a string ("id":"3", "size":"1024") and every client
 * had to know which strings were really numbers. Each route now describes
 * its row once - which column goes under which key, and what kind of value
 * it is - and this turns the text into the right JSON type.
 *
 * A NULL column reads back as "" from c-utils, never as a NULL pointer, so
 * the empty string is what "no value" looks like here. For a numeric, flag
 * or nullable text column it becomes JSON null.
 *
 * Numbers are doubles inside the builder, and cJSON prints them with 15
 * significant digits, so an integer comes out digit for digit below 10^15.
 * Every id, count, byte size (10^15 bytes is a petabyte) and epoch-seconds
 * value in the schema stays below that. Past 2^53 the c-utils integer adder
 * refuses the value outright, so it is handed over as the nearest double
 * rather than failing the response; the builder has no way to emit raw
 * number text. Nanosecond timestamps are the one thing that would cross
 * these limits, which is why no route serves one as a single number.
 */

#include <cutils/json.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef enum {
    JCOL_STR,       /* text, as is; "" stays "" */
    JCOL_STR_NULL,  /* text; "" (a NULL column) becomes null */
    JCOL_INT,       /* integer; "" becomes null */
    JCOL_BOOL       /* 0/1 (or "true"/"false"); "" becomes null */
} jcol_kind_t;

typedef struct {
    const char  *key;   /* JSON member name */
    int          col;   /* index into the row */
    jcol_kind_t  kind;
} jcol_t;

/* One value of the given kind under key. 0 on success, non-zero only on
 * allocation failure inside the builder. */
int jrow_elem_add(cutils_json_elem_t *e, const char *key, jcol_kind_t kind,
                  const char *text);
int jrow_resp_add(cutils_json_resp_t *r, const char *key, jcol_kind_t kind,
                  const char *text);

/* Every column in cols, from row. Stops at the first failure. */
int jrow_elem_add_row(cutils_json_elem_t *e, const jcol_t *cols, size_t n,
                      char *const *row);
int jrow_resp_add_row(cutils_json_resp_t *r, const jcol_t *cols, size_t n,
                      char *const *row);

#define JROW_N(cols) (sizeof(cols) / sizeof((cols)[0]))

/*
 * A request flag, as a JSON boolean or the 0/1 an earlier client sent.
 *
 * Flags were read with the u32 getter, so the only spelling accepted was a
 * number, and a grep schedule stored "ignore_case":1 in its parameters
 * where every other flag in the API is a boolean. Writers now send and store
 * true/false; 0 and 1 are still read, because schedules created before the
 * change hold them. Returns 0 with *out set when the key is present and
 * valid, 0 with *out untouched when it is absent, and -1 for any other
 * value.
 */
int jreq_flag(const cutils_json_req_t *req, const char *key, int *out);

/*
 * Typed request fields.
 *
 * The c-utils getters fail the same way for a field that is absent and a
 * field of the wrong type, and the routes read a failure as absence: an
 * array `label` registered a repository with no label and answered 201,
 * and `"snap_id":1e999` was told "snap_id is required" about a field it had
 * plainly sent. These tell the three apart, so a route can say "is
 * required" only when it is true and name the expected type otherwise.
 *
 * Each returns JREQ_OK with *out set, JREQ_ABSENT with *out untouched when
 * the key is not in the body, or JREQ_BAD with err->msg naming the field
 * and what it must be. A JSON null is a value of the wrong type, not
 * absence: every client omits a field it means to leave out, and treating
 * null as absent would let a mistyped client keep half-working. jreq_flag
 * above already refused it.
 *
 * key may be a dot path ("trigger.seconds"); the message names it as given.
 */
enum { JREQ_BAD = -1, JREQ_OK = 0, JREQ_ABSENT = 1 };

typedef struct {
    char msg[192];
} jreq_err_t;

/* What a field holds. JREQ_T_HUGE is a number cJSON read as infinite, such
 * as 1e999: a number the client sent, too large for any field. */
typedef enum {
    JREQ_T_ABSENT,
    JREQ_T_NULL,
    JREQ_T_BOOL,
    JREQ_T_NUMBER,
    JREQ_T_HUGE,
    JREQ_T_STRING,
    JREQ_T_ARRAY,
    JREQ_T_OBJECT
} jreq_type_t;

jreq_type_t jreq_type(const cutils_json_req_t *req, const char *key);

/* A string; *out is malloc'd and the caller frees it. */
int jreq_str(const cutils_json_req_t *req, const char *key, char **out,
             jreq_err_t *err);

/* A JSON boolean, and only that. */
int jreq_bool(const cutils_json_req_t *req, const char *key, bool *out,
              jreq_err_t *err);

/*
 * A whole number in [min, max]. A fraction is "must be a whole number"; a
 * number outside the range, a negative one for an unsigned field, or one
 * too large to represent (1e999) is "is out of range", with the range.
 * Bounds wider than 2^53 are narrowed to it, the largest integer a double
 * holds exactly: past it the value read would not be the value sent.
 */
int jreq_u32(const cutils_json_req_t *req, const char *key, uint32_t *out,
             uint32_t min, uint32_t max, jreq_err_t *err);
int jreq_u64(const cutils_json_req_t *req, const char *key, uint64_t *out,
             uint64_t min, uint64_t max, jreq_err_t *err);
int jreq_i64(const cutils_json_req_t *req, const char *key, int64_t *out,
             int64_t min, int64_t max, jreq_err_t *err);

/*
 * An array of strings, every element checked: one that is not a string
 * refuses the whole field rather than being skipped, which is how a backup
 * body with a number among its paths used to back up the rest and say
 * nothing. More than max elements is refused too. On JREQ_OK *out holds
 * *n malloc'd strings (NULL when the array is empty); free it with
 * jreq_strv_free.
 */
int jreq_strv(const cutils_json_req_t *req, const char *key, size_t max,
              char ***out, size_t *n, jreq_err_t *err);
void jreq_strv_free(char **v, size_t n);

#endif
