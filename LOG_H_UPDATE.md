## vendor/log.h — upstream update needed

c-log now has a `log_set_pre_write` callback hook instead of the hardcoded
stderr progress-line clearing. After pulling the new log.h, register a
pre-write callback in c-backup:

```c
static void clear_progress(FILE* out) {
    if (out == stderr)
        fprintf(stderr, "\r%-80s\r", "");
}

// during init:
log_set_pre_write(clear_progress);
```

This is also a good time to use the real terminal width instead of the
hardcoded 80 columns.
