#define _POSIX_C_SOURCE 200809L

#include "tzinfo.h"

#include <ctype.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

const char *tz_zonedir(void)
{
    const char *d = getenv("TZDIR");
    return d && *d ? d : "/usr/share/zoneinfo";
}

static int is_regular(const char *path)
{
    struct stat st;
    return stat(path, &st) == 0 && S_ISREG(st.st_mode);
}

/* A zone name joined to its directory, refusing ".." so a TZ value cannot
 * point the comparison anywhere but the zoneinfo tree. */
static int zone_path(const char *zonedir, const char *name, char *out, size_t n)
{
    if (!name[0] || strstr(name, "..")) return -1;
    if (name[0] == '/') {
        if (strlen(name) >= n) return -1;
        memcpy(out, name, strlen(name) + 1);
        return 0;
    }
    int w = snprintf(out, n, "%s/%s", zonedir, name);
    return w < 0 || (size_t)w >= n ? -1 : 0;
}

/* POSIX TZ: a std name of three or more letters, or <...>, then an offset.
 * Only the start is checked; glibc resolves the rest or not, and this is
 * for telling a zone name from a rule, not for validating the rule. */
static int looks_posix(const char *s)
{
    size_t i = 0;
    if (s[0] == '<') {
        const char *e = strchr(s, '>');
        if (!e || e - s < 4) return 0;
        i = (size_t)(e - s) + 1;
    } else {
        while (isalpha((unsigned char)s[i])) i++;
        if (i < 3) return 0;
    }
    if (s[i] == '+' || s[i] == '-') i++;
    return isdigit((unsigned char)s[i]) != 0;
}

int tz_known(const char *tz, const char *zonedir)
{
    if (!tz || !*tz) return 1;
    const char *name = tz[0] == ':' ? tz + 1 : tz;
    if (strcmp(name, "UTC") == 0 || strcmp(name, "GMT") == 0) return 1;
    char path[PATH_MAX];
    if (zone_path(zonedir, name, path, sizeof(path)) == 0 && is_regular(path)) return 1;
    return tz[0] != ':' && looks_posix(name);
}

/* Byte-for-byte equal regular files. Zone files are a few KiB. */
static int same_file_content(const char *a, const char *b)
{
    FILE *fa = fopen(a, "rb");
    if (!fa) return 0;
    FILE *fb = fopen(b, "rb");
    if (!fb) { fclose(fa); return 0; }
    int same = 1;
    char ba[4096], bb[4096];
    for (;;) {
        size_t na = fread(ba, 1, sizeof(ba), fa);
        size_t nb = fread(bb, 1, sizeof(bb), fb);
        if (na != nb || memcmp(ba, bb, na) != 0) { same = 0; break; }
        if (na == 0) break;
    }
    fclose(fa);
    fclose(fb);
    return same;
}

static int copy_name(const char *name, char *out, size_t outsz)
{
    const char *zi = strstr(name, "zoneinfo/");
    if (zi) name = zi + 9;
    /* A name that does not fit is not a name. */
    size_t len = strlen(name);
    if (!len || len >= outsz) return -1;
    memcpy(out, name, len + 1);
    return 0;
}

void tz_name_at(const char *tz, const char *localtime_path,
                const char *timezone_path, const char *zonedir,
                char *out, size_t outsz)
{
    if (!out || outsz == 0) return;
    out[0] = '\0';

    if (tz && *tz) {
        if (!tz_known(tz, zonedir)) { snprintf(out, outsz, "UTC"); return; }
        if (copy_name(tz[0] == ':' ? tz + 1 : tz, out, outsz) == 0) return;
    } else {
        char link[PATH_MAX];
        ssize_t n = readlink(localtime_path, link, sizeof(link) - 1);
        if (n > 0) {
            link[n] = '\0';
            if (copy_name(link, out, outsz) == 0) return;
        } else {
            /*
             * Not a symlink: absent, or a regular file such as a bind
             * mount. /etc/timezone names a zone only when /etc/localtime
             * is absent or holds that zone's file; otherwise it describes
             * whatever the image was built with, not what is mounted.
             */
            FILE *f = fopen(timezone_path, "r");
            if (f) {
                char cand[256], path[PATH_MAX];
                if (fgets(cand, sizeof(cand), f)) {
                    cand[strcspn(cand, "\r\n")] = '\0';
                    int lt_present = access(localtime_path, F_OK) == 0;
                    if (cand[0] &&
                        (!lt_present ||
                         (zone_path(zonedir, cand, path, sizeof(path)) == 0 &&
                          same_file_content(path, localtime_path))) &&
                        copy_name(cand, out, outsz) == 0) {
                        fclose(f);
                        return;
                    }
                }
                fclose(f);
            }
        }
    }

    time_t now = time(NULL);
    struct tm tm;
    localtime_r(&now, &tm);
    if (strftime(out, outsz, "%Z", &tm) == 0) snprintf(out, outsz, "UTC");
}

void tz_name(char *out, size_t outsz)
{
    tz_name_at(getenv("TZ"), "/etc/localtime", "/etc/timezone", tz_zonedir(),
               out, outsz);
}
