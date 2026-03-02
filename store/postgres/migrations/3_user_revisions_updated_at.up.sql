ALTER TABLE user_revisions ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
CREATE INDEX idx_user_revisions_updated_at ON user_revisions(updated_at);
