-- description: create users table
-- authoredAt: 1425515766000
-- up:

CREATE TABLE IF NOT EXISTS users (
  id text,
  ts timestamp,
  events map<varchar, varchar>,  
  PRIMARY KEY (id, ts)
) WITH CLUSTERING ORDER BY (ts DESC)
;

-- down:

DROP TABLE users;
