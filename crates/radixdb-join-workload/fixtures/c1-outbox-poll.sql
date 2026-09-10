SELECT id, revision
FROM outbox_jobs
WHERE (state = 'pending' AND available_at <= :now)
   OR (state = 'leased' AND lease_until IS NOT NULL AND lease_until <= :now)
ORDER BY available_at, created_at, id
LIMIT :limit;
