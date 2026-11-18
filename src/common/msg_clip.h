#pragma once

/*
 * Bounded copies of a message that say when they are cut (B213, B223).
 *
 * A refusal or warning crosses several fixed buffers between the helper
 * that raises it and the job that reports it, and each one used to clip it
 * with a "%.NNNs" precision: silently, mid-word, so the job said "..., or
 * c-b" and the operator never learnt the rest. MSG_CLIP_MAX is the bound
 * every buffer on that path holds; a message longer than its buffer ends
 * "..." instead, cut back to a UTF-8 character boundary because the text
 * is served as JSON.
 */

#include <stddef.h>
#include <string.h>

#define MSG_CLIP_MAX 2048

/* The last bytes of a full buffer of sz (text in it, NUL at sz - 1)
 * replaced by "...". */
static inline void msg_clip_mark(char *buf, size_t sz)
{
    if (sz < 4) return;
    size_t keep = sz - 4;
    while (keep > 0 && ((unsigned char)buf[keep] & 0xC0u) == 0x80u) keep--;
    memcpy(buf + keep, "...", 4);
}

/* src into dst of sz, ending "..." when it does not fit. */
static inline void msg_clip(char *dst, size_t sz, const char *src)
{
    if (!sz) return;
    if (!src) src = "";
    size_t n = strlen(src);
    if (n < sz) { memcpy(dst, src, n + 1); return; }
    memcpy(dst, src, sz - 1);
    dst[sz - 1] = '\0';
    msg_clip_mark(dst, sz);
}

/* After vsnprintf/snprintf into buf of sz returned n: marks a cut. */
static inline void msg_clip_after(char *buf, size_t sz, int n)
{
    if (n >= 0 && (size_t)n >= sz) msg_clip_mark(buf, sz);
}
