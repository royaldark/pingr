CREATE TABLE targets (
  id    INTEGER PRIMARY KEY,
  name  TEXT NOT NULL UNIQUE            -- as typed: hostname or IP literal
) STRICT;

CREATE TABLE addresses (
  id         INTEGER PRIMARY KEY,
  target_id  INTEGER NOT NULL REFERENCES targets(id),
  ip         TEXT NOT NULL,
  UNIQUE (target_id, ip)
) STRICT;

-- One row per uninterrupted collector run.
CREATE TABLE sessions (
  id             INTEGER PRIMARY KEY,
  started_us     INTEGER NOT NULL,      -- UTC microseconds since the Unix epoch
  last_alive_us  INTEGER NOT NULL,      -- updated with every write batch
  ended_us       INTEGER,               -- NULL: running, or died uncleanly
  end_reason     TEXT,                  -- 'stopped' | 'clock-jump' | 'stalled'
  interval_us    INTEGER NOT NULL,
  timeout_us     INTEGER NOT NULL,
  pingr_version  TEXT NOT NULL
) STRICT;

CREATE TABLE session_targets (
  session_id  INTEGER NOT NULL REFERENCES sessions(id),
  target_id   INTEGER NOT NULL REFERENCES targets(id),
  PRIMARY KEY (session_id, target_id)
) STRICT, WITHOUT ROWID;

-- Every probe sent, answered or not. The key carries session and sequence so
-- that a wall clock stepping backwards can never reject an insert.
CREATE TABLE samples (
  target_id   INTEGER NOT NULL REFERENCES targets(id),
  sent_us     INTEGER NOT NULL,         -- wall-clock UTC at send
  session_id  INTEGER NOT NULL REFERENCES sessions(id),
  seq         INTEGER NOT NULL,         -- per session and target, from 0
  address_id  INTEGER REFERENCES addresses(id),  -- NULL: name did not resolve
  rtt_us      INTEGER,                  -- NULL: lost
  loss        INTEGER,                  -- NULL: answered; 1 timeout,
                                        -- 2 unreachable, 3 other, 4 unresolved
  CHECK ((rtt_us IS NULL) <> (loss IS NULL)),
  PRIMARY KEY (target_id, sent_us, session_id, seq)
) STRICT, WITHOUT ROWID;
