-- A network the collector has run on. Detected networks are told apart by
-- their default gateway's hardware address; one named with `--network` and
-- never detected has none.
CREATE TABLE networks (
  id           INTEGER PRIMARY KEY,
  gateway_mac  TEXT UNIQUE,             -- e.g. '00:00:5e:00:53:01'
  label        TEXT NOT NULL UNIQUE
) STRICT;

-- NULL: the network could not be told when the session began.
ALTER TABLE sessions ADD COLUMN network_id INTEGER REFERENCES networks(id);
