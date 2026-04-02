#pragma once

#include "repo.h"

/*
 * JSON RPC interface: reads a JSON request from stdin, dispatches to
 * the appropriate handler, writes a JSON response to stdout.
 *
 * Request format:  {"action":"<name>","params":{...}}
 * Response format: {"status":"ok","data":{...}}
 *             or   {"status":"error","message":"..."}
 *
 * json_api_dispatch() — one-shot mode (--json): read one request, respond, exit.
 * json_api_session()  — persistent mode (--json-session): read/respond loop
 *                       on stdin/stdout (newline-delimited JSON).
 *
 * Returns 0 on success (even if the action itself reports an error via
 * the JSON envelope), 1 on protocol-level failure (bad JSON, etc.).
 */
int json_api_dispatch(repo_t *repo);
int json_api_session(repo_t *repo);
