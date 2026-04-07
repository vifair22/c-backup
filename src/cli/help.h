#ifndef HELP_H
#define HELP_H

/* Set the active topic used by help_current() / validate_options() error
 * paths. `t` is copied into an internal static buffer, so the caller may
 * free or reuse its own memory afterwards. Passing NULL clears the topic
 * (help_current() then prints the top-level index). */
void        set_topic(const char *t);

/* Return the currently-set topic, or NULL if none is set. */
const char *current_topic(void);

/* Print the full top-level command index to stderr. */
void        help_all(void);

/* Print the help block for the named topic to stderr. If the topic is NULL
 * or unknown, prints the top-level index (same as help_all()). Multi-word
 * topics use a single space separator, e.g. "tag set", "policy edit". */
void        help_topic(const char *t);

/* Print help for the currently-set topic (or the top-level index if none). */
void        help_current(void);

#endif /* HELP_H */
