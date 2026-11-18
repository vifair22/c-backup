-- Reserved id marks (B217, src/common/id_reserve.h). Per AUTOINCREMENT
-- table, a mark above every id it has handed out, made durable before an id
-- past it is told to anyone. state.db commits are not synced for up to about
-- a second, so a power cut can take back a row and its sqlite_sequence
-- update after the id went out; the start raises each sequence to its mark
-- so the id is never issued again. A clean stop brings the marks down to the
-- sequences, so only an unclean one leaves ids unused.
CREATE TABLE id_reserve (
    tbl  TEXT PRIMARY KEY,
    high INTEGER NOT NULL
);
