-- A network the collector has run on. Detected networks are told apart by
-- their default gateway's hardware address; one named with `--network` and
-- never detected has none.
CREATE TABLE networks (
  id           INTEGER PRIMARY KEY,
  gateway_mac  TEXT UNIQUE,             -- e.g. '8a:2d:ec:c5:f4:5f'
  label        TEXT NOT NULL UNIQUE
) STRICT;

-- NULL: the network could not be told when the session began.
ALTER TABLE sessions ADD COLUMN network_id INTEGER REFERENCES networks(id);
