BEGIN;
UPDATE employees SET name = 'Temporary' WHERE id = 1;
ROLLBACK;
