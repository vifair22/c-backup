-- Encryption state, mirrored from the repository marker so the daemon can
-- answer "which repositories are encrypted, and has anyone saved their key"
-- without opening every marker on disk. The marker remains authoritative;
-- these columns are a derived index like everything else in this database.
ALTER TABLE repos ADD COLUMN encryption TEXT NOT NULL DEFAULT 'none';

-- Whether the operator has confirmed they hold a copy of the key somewhere
-- other than this machine.
--
-- A system-managed key exists only in a file on the host being backed up. If
-- that host is lost, every offsite copy of its repository is unreadable, which
-- is precisely the disaster backups are for. So this stays 0 until someone
-- says otherwise, and an unescrowed repository is reported by /metrics and
-- warned about on every backup rather than mentioned once at creation and
-- forgotten.
ALTER TABLE repos ADD COLUMN key_escrowed INTEGER NOT NULL DEFAULT 0;
