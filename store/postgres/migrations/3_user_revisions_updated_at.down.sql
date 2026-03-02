DROP INDEX IF EXISTS idx_user_revisions_updated_at;
ALTER TABLE user_revisions DROP COLUMN IF EXISTS updated_at;
