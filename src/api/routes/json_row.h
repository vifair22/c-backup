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
#include <stddef.h>

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

#endif
