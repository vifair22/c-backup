-- store.fuse_readahead was registered and seeded but never read: no caller
-- passed it to the store, so the FUSE readahead ring it named could not be
-- switched on. The ring and the key are gone; this clears the seeded row so
-- a database does not carry a setting that no longer exists.
DELETE FROM config WHERE key = 'store.fuse_readahead';
