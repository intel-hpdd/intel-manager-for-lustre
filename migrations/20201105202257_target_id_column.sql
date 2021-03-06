ALTER TABLE IF EXISTS target DROP CONSTRAINT target_pkey;

ALTER TABLE IF EXISTS target
ADD COLUMN id serial PRIMARY KEY;

CREATE UNIQUE INDEX target_uuid ON target (uuid);

ALTER TABLE IF EXISTS target
ADD CONSTRAINT unique_target_uuid UNIQUE USING INDEX target_uuid;