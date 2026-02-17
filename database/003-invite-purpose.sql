-- ============================================================================
-- S-IMSY Reporting — Add 'invite' purpose to auth_otp
-- ============================================================================
-- Run: sudo -u postgres psql -d simsy_reporting -f 003-invite-purpose.sql
-- ============================================================================

-- Drop the existing CHECK constraint and recreate with 'invite' included
ALTER TABLE auth_otp DROP CONSTRAINT IF EXISTS auth_otp_purpose_check;
ALTER TABLE auth_otp ADD CONSTRAINT auth_otp_purpose_check
    CHECK (purpose IN ('login_2fa', 'password_reset', 'invite'));

-- Allow auth_users to have a NULL password_hash + salt for invited-but-not-yet-activated users
ALTER TABLE auth_users ALTER COLUMN password_hash DROP NOT NULL;
ALTER TABLE auth_users ALTER COLUMN salt DROP NOT NULL;

SELECT 'Invite purpose added to auth_otp, password_hash/salt now nullable.' AS status;
