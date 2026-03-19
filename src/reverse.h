#pragma once

#include "error.h"
#include "repo.h"
#include "types.h"
#include <stdint.h>

typedef struct {
    uint8_t  op_type;
    char    *path;
    node_t   prev_node;
} rev_entry_t;

typedef struct {
    uint32_t    snap_id;   /* the snapshot this record reverts FROM */
    uint32_t    entry_count;
    rev_entry_t *entries;
} rev_record_t;

status_t reverse_write(repo_t *repo, const rev_record_t *rec);
status_t reverse_load(repo_t *repo, uint32_t snap_id, rev_record_t **out);
void     reverse_free(rev_record_t *rec);
