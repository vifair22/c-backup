-- scheduler.enabled, jobs.max_parallel and retention.keep_* moved from the
-- database to config.yaml. Nothing could change them in the database: there
-- is no route that writes configuration, and the retention defaults were
-- not even read. The seeded rows would only mislead someone reading the
-- table, so they go; the same keys in config.yaml are what the daemon reads.
DELETE FROM config WHERE key IN (
    'scheduler.enabled', 'jobs.max_parallel',
    'retention.keep_snaps', 'retention.keep_daily', 'retention.keep_weekly',
    'retention.keep_monthly', 'retention.keep_yearly'
);
