-- Device nodes were scanned, typed and indexed, and then restored as
-- nothing: the major/minor pair that is the entire content of a character
-- or block device had nowhere to live. The manifest format did not carry it
-- either, so this is the index half of a format change (tree version 2).
--
-- Zero for every other node type, and zero for device nodes indexed from a
-- version 1 tree, where the numbers were never recorded and cannot be
-- recovered. Restore treats 0:0 as "unknown" and skips rather than creating
-- a device pointing at the wrong driver.
ALTER TABLE nodes ADD COLUMN dev_major INTEGER NOT NULL DEFAULT 0;
ALTER TABLE nodes ADD COLUMN dev_minor INTEGER NOT NULL DEFAULT 0;
