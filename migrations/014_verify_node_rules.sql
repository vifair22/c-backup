-- Verify's two node rules (B268): file entries that name no content, and
-- file entries stored while still changing. Both used to be answered by
-- walking every node row of the repository through nodes_by_hash and
-- fetching each row from the table to read its type and flags. Those
-- fetches land in hash order, which is random against the table, so once
-- the node rows outgrew the page cache every row cost a cache miss, and
-- the verify grew far faster than the index (290 s at 256k entries, 938 s
-- at 364k). Both rules match nothing on a healthy repository, so a partial
-- index holding only their rows is near empty and answers them without
-- reading a single node that does not match.
--
-- The WHERE clauses must stay textually the terms verify's queries use:
-- SQLite only picks a partial index whose condition the query implies.
CREATE INDEX nodes_file_no_content ON nodes (repo_id)
    WHERE type = 1 AND (content_hash IS NULL OR content_hash = '');
CREATE INDEX nodes_unsettled ON nodes (repo_id)
    WHERE type = 1 AND (flags & 1) != 0;
