CREATE TABLE records (user_id TEXT NOT NULL, id TEXT NOT NULL, data BLOB NOT NULL, revision INTEGER, PRIMARY KEY (user_id, id));
CREATE TABLE store_revisions (user_id TEXT NOT NULL, revision INTEGER NOT NULL, PRIMARY KEY (user_id, revision));