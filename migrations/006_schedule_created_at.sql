-- B155: when a schedule was created, the anchor for an interval schedule
-- that has never fired. Without it the scheduler anchored such a schedule to
-- whenever it last recomputed, which at every daemon start meant "now", so a
-- schedule whose interval was longer than the gap between restarts never
-- came due at all.
--
-- 006 rather than 005: the units branches hold 002 to 004 and 005 is spoken
-- for by work in flight. The runner applies by name, so a gap costs nothing.
ALTER TABLE schedules ADD COLUMN created_at INTEGER;

-- Rows from before this column. For a never-fired interval schedule the
-- stored due time is one interval after the last time it was anchored,
-- which is the best evidence of its creation there is (an earlier restart
-- may already have moved it). Every other row keeps NULL: a schedule that
-- has fired anchors to its last fire, and a calendar one to the clock.
UPDATE schedules
   SET created_at = next_due_at - CAST(json_extract(trigger_spec, '$.seconds') AS INTEGER)
 WHERE trigger_type = 'interval'
   AND last_fired_at IS NULL
   AND json_extract(trigger_spec, '$.seconds') IS NOT NULL;
