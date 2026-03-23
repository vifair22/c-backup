#pragma once

typedef enum {
    OK = 0,
    ERR_IO,
    ERR_CORRUPT,
    ERR_NOMEM,
    ERR_INVALID,
    ERR_NOT_FOUND,
    ERR_TOO_LARGE
} status_t;
