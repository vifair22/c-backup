-- Extended attributes and POSIX ACLs were collected by every scan and then
-- dropped on the floor. The manifest format reserved xattr_hash and acl_hash
-- per entry and the object store had types for the blobs, but the backup
-- pipeline never stored them and the index had nowhere to record them.
-- Content and basic metadata round-tripped, so nothing failed; the xattrs
-- and ACLs were simply gone on restore.
--
-- These columns mirror the manifest fields into the derived index so the
-- restore path can reach the blobs. Snapshots taken before the fix reindex
-- to NULL here, which is accurate: their trees carry zero hashes and there
-- is nothing to recover.
ALTER TABLE nodes ADD COLUMN xattr_hash TEXT;
ALTER TABLE nodes ADD COLUMN acl_hash TEXT;
