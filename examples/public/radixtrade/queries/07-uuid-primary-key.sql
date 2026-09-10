-- UUID primary keys can be explicit or generated with AUTO_INCREMENT.
-- RadixDB generates UUIDv7 values for UUID PRIMARY KEY AUTO_INCREMENT.

DROP TABLE IF EXISTS rt_uuid_demo;

CREATE TABLE rt_uuid_demo (
    id UUID PRIMARY KEY AUTO_INCREMENT,
    label TEXT NOT NULL
);

INSERT INTO rt_uuid_demo (label)
VALUES ('generated UUIDv7 id');

INSERT INTO rt_uuid_demo (id, label)
VALUES ('01940000-0020-7000-8000-000000000001', 'explicit UUID id');

SELECT CAST(id AS TEXT) AS id, label
FROM rt_uuid_demo
ORDER BY label;

