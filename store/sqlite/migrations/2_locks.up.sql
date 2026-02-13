CREATE TABLE locks (user_id TEXT NOT NULL, lock_name TEXT NOT NULL, instance_id TEXT NOT NULL, expires_at INTEGER NOT NULL, exclusive INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (user_id, lock_name, instance_id));
CREATE INDEX idx_locks_expires_at ON locks(expires_at);
CREATE INDEX idx_locks_user_lock ON locks(user_id, lock_name);
