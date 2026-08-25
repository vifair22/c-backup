-- What a repository backs up, so nobody has to say it twice.
--
-- Until now a backup carried its sources in the request, which is right for
-- the CLI - `backup 1 /home/me` reads exactly what it says - and wrong for
-- everything else. A desktop button had nowhere to get them and offered no
-- way to type them, so it sent none and the daemon refused. A schedule had
-- the same problem, which is part of why scheduled backups never ran.
--
-- The paths are a property of the repository: this repo backs up these
-- directories. Storing them here means configuring once and then backing up
-- with a click, a schedule, or a bare `backup <id>` - and a request that does
-- name paths still overrides, because the CLI's explicitness is worth
-- keeping.
--
-- A row per path rather than a list in policy_values. Retention is a handful
-- of scalars and fits a key-value table; an ordered set of paths does not,
-- and encoding one as text inside a value column means writing a parser for
-- a format nobody chose.
CREATE TABLE repo_backup_paths (
    repo_id INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    -- 'source' or 'exclude'. Two lists with identical shape and lifetime, so
    -- one table with a discriminator rather than two that drift apart.
    kind    TEXT NOT NULL,
    value   TEXT NOT NULL,
    PRIMARY KEY (repo_id, kind, value)
);

CREATE INDEX repo_backup_paths_by_repo ON repo_backup_paths(repo_id, kind);
