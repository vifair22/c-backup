/*
 * Where the suites put their scratch.
 *
 * Every suite builds its paths as TEST_TMP "/c_backup_test_<name>", so one
 * value moves all of them. It is a compile-time string rather than a getenv
 * because the suites splice it into hundreds of string literals, and a
 * runtime value would turn each of those into an snprintf. The Makefile
 * passes C_BACKUP_TEST_DIR and rebuilds the suites when it changes; built
 * any other way they write under /tmp as they always did.
 *
 * tests/scratch-guard.sh watches this directory while a suite runs and fails
 * one that grows it past its budget, which is what stops a test that writes
 * gigabytes from filling a CI runner's disk.
 */
#ifndef C_BACKUP_TEST_TMP_H
#define C_BACKUP_TEST_TMP_H

#ifndef C_BACKUP_TEST_DIR
#define C_BACKUP_TEST_DIR "/tmp"
#endif

#define TEST_TMP C_BACKUP_TEST_DIR

#endif
