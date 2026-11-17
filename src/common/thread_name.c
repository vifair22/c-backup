#define _GNU_SOURCE

#include "thread_name.h"

#include <pthread.h>
#include <string.h>

void thread_name(const char *name)
{
    char buf[16];
    if (!name) return;
    strncpy(buf, name, sizeof(buf) - 1);
    buf[sizeof(buf) - 1] = '\0';
    (void)pthread_setname_np(pthread_self(), buf);
}
