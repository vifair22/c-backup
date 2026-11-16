#include "reserved.h"

#include <stdio.h>
#include <string.h>

int reserved_top_name(const char *name)
{
    return name && (strcmp(name, RESERVED_UNIT_DIR) == 0 ||
                    strcmp(name, RESERVED_CAPTURES_DIR) == 0);
}

/*
 * The first component that survives lexical normalisation: "." skipped,
 * ".." pops. Only the first component decides, so the walk keeps a depth
 * and the first real component seen at depth 0 after every pop.
 */
int reserved_source_refused(const char *path, char *why, size_t whysz)
{
    if (!path) return 0;
    /* Components at depth 0 are candidates for the root name; a ".." back
     * to depth 0 makes the next component a candidate again. */
    int depth = 0;
    const char *p = path;
    while (*p) {
        while (*p == '/') p++;
        if (!*p) break;
        const char *s = p;
        while (*p && *p != '/') p++;
        size_t n = (size_t)(p - s);
        if (n == 1 && s[0] == '.') continue;
        if (n == 2 && s[0] == '.' && s[1] == '.') {
            if (depth > 0) depth--;
            continue;
        }
        if (depth == 0) {
            char comp[64];
            if (n < sizeof(comp)) {
                memcpy(comp, s, n);
                comp[n] = '\0';
                if (reserved_top_name(comp)) {
                    if (why && whysz)
                        snprintf(why, whysz, "source %s is at or under /%s, a name "
                                 "reserved for the daemon in every snapshot; it "
                                 "cannot be backed up as files", path, comp);
                    return -1;
                }
            }
        }
        depth++;
    }
    return 0;
}
