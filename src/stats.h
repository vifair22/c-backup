#pragma once

#include "error.h"
#include "repo.h"
#include <stdint.h>

typedef struct {
    uint32_t snap_count;       /* .snap files present */
    uint32_t snap_total;       /* highest snapshot ID (HEAD) */
    uint32_t loose_objects;
    uint64_t loose_bytes;
    uint32_t pack_files;       /* number of .dat pack files */
    uint64_t pack_bytes;       /* .dat + .idx combined */
    uint32_t reverse_records;
    uint64_t reverse_bytes;
    uint64_t total_bytes;
} repo_stat_t;

status_t repo_stats(repo_t *repo, repo_stat_t *out);
void     repo_stats_print(const repo_stat_t *s);
