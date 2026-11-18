-- Journal entry ids are never issued twice (B217). They were MAX(id) + 1 per
-- repository, so a power cut that took back the newest entries handed their
-- ids to the next ones, and GET /repos/<id>/journal showed a client an id it
-- already held on a different entry. They are AUTOINCREMENT now, one sequence
-- for every repository, covered by the reserved marks (id_reserve, 008).
--
-- Existing ids are kept wherever that is possible. One table cannot hold
-- the same id twice, and per-repository ids repeat across repositories, so a
-- repository keeps its ids when none of them is also used by a repository
-- with a lower id; every other repository's entries are renumbered, in their
-- own order, above the highest id any repository had. So an id a client
-- held either still names its entry, or names nothing in that repository:
-- never a different entry. Each repository's entries keep their relative
-- order, which is what the route's ORDER BY ts DESC, id DESC relies on.
CREATE INDEX journal_mig_by_id ON journal (id);

CREATE TABLE journal_mig_moved AS
    SELECT DISTINCT j.repo_id FROM journal j
     WHERE EXISTS (SELECT 1 FROM journal k WHERE k.id = j.id AND k.repo_id < j.repo_id);

ALTER TABLE journal RENAME TO journal_v1;

CREATE TABLE journal (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    repo_id     INTEGER NOT NULL,
    ts          INTEGER NOT NULL,
    source      TEXT NOT NULL,
    operation   TEXT NOT NULL,
    result      TEXT NOT NULL,
    duration_ms INTEGER,
    message     TEXT,
    FOREIGN KEY (repo_id) REFERENCES repos(id) ON DELETE CASCADE
);

INSERT INTO journal (id, repo_id, ts, source, operation, result, duration_ms, message)
    SELECT id, repo_id, ts, source, operation, result, duration_ms, message
      FROM journal_v1
     WHERE repo_id NOT IN (SELECT repo_id FROM journal_mig_moved)
     ORDER BY id;

INSERT INTO journal (id, repo_id, ts, source, operation, result, duration_ms, message)
    SELECT (SELECT COALESCE(MAX(id), 0) FROM journal_v1)
               + ROW_NUMBER() OVER (ORDER BY repo_id, id),
           repo_id, ts, source, operation, result, duration_ms, message
      FROM journal_v1
     WHERE repo_id IN (SELECT repo_id FROM journal_mig_moved)
     ORDER BY repo_id, id;

DROP TABLE journal_v1;
DROP TABLE journal_mig_moved;

CREATE INDEX journal_by_ts ON journal (repo_id, ts);
