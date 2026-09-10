-- Ordinary indexes, partial unique indexes, SHOW INDEXES and ALTER INDEX.

DROP TABLE IF EXISTS rt_index_soft_delete_demo;

CREATE TABLE rt_index_soft_delete_demo (
    id UUID PRIMARY KEY AUTO_INCREMENT,
    email TEXT NOT NULL,
    external_ref TEXT NOT NULL,
    __deleted_at TIMESTAMP
);

CREATE UNIQUE INDEX rt_index_demo_email_active_uidx
    ON rt_index_soft_delete_demo (email)
    WHERE __deleted_at IS NULL;

CREATE INDEX rt_index_demo_ref_idx
    ON rt_index_soft_delete_demo (external_ref)
    USING HASH;

ALTER INDEX rt_index_demo_ref_idx
    RENAME TO rt_index_demo_ref_lookup_idx;

INSERT INTO rt_index_soft_delete_demo (email, external_ref)
VALUES ('active@example.test', 'first-active');

UPDATE rt_index_soft_delete_demo
SET __deleted_at = '2026-08-07T10:00:00Z'
WHERE email = 'active@example.test';

INSERT INTO rt_index_soft_delete_demo (email, external_ref)
VALUES ('active@example.test', 'second-active');

SELECT email, external_ref, __deleted_at
FROM rt_index_soft_delete_demo
ORDER BY external_ref;

SHOW INDEXES FROM rt_index_soft_delete_demo;

