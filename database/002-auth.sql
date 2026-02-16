-- ============================================================================
-- S-IMSY Reporting Platform — Authentication & Admin Management
-- ============================================================================
-- Run this script as the PostgreSQL superuser (postgres) on the Hetzner server
-- AFTER 001-init.sql has been applied.
--
-- Usage:
--   sudo -u postgres psql -d simsy_reporting -f 002-auth.sql
--
-- This script creates:
--   1. auth_users — user accounts with hashed passwords
--   2. auth_sessions — JWT session tracking for revocation
--   3. auth_otp — one-time password codes for 2FA and password reset
--   4. Grants for the API worker role (simsy_reporting_app)
-- ============================================================================

-- ============================================================================
-- STEP 1: Users Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS auth_users (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email               TEXT NOT NULL,
    email_lower         TEXT GENERATED ALWAYS AS (lower(email)) STORED NOT NULL,
    password_hash       TEXT NOT NULL,
    salt                TEXT NOT NULL,
    display_name        TEXT NOT NULL,
    role                TEXT NOT NULL CHECK (role IN ('admin', 'tenant', 'customer')),
    tenant_id           TEXT NOT NULL REFERENCES rpt_tenants(tenant_id),
    customer_name       TEXT,
    is_active           BOOLEAN DEFAULT true,
    failed_logins       INTEGER DEFAULT 0,
    locked_until        TIMESTAMPTZ,
    last_login_at       TIMESTAMPTZ,
    password_changed_at TIMESTAMPTZ DEFAULT now(),
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now(),
    created_by          UUID REFERENCES auth_users(id),

    -- Customer role must have a customer_name
    CONSTRAINT chk_customer_name CHECK (
        (role != 'customer') OR (customer_name IS NOT NULL)
    )
);

-- Case-insensitive email uniqueness
CREATE UNIQUE INDEX IF NOT EXISTS idx_auth_users_email_lower
    ON auth_users (email_lower);

-- Lookup indexes
CREATE INDEX IF NOT EXISTS idx_auth_users_tenant
    ON auth_users (tenant_id);
CREATE INDEX IF NOT EXISTS idx_auth_users_role
    ON auth_users (role);
CREATE INDEX IF NOT EXISTS idx_auth_users_active
    ON auth_users (is_active) WHERE is_active = true;

-- ============================================================================
-- STEP 2: Sessions Table
-- ============================================================================
-- Tracks JWT sessions for server-side revocation.
-- The token_hash is SHA-256 of the JWT's jti claim — we never store raw tokens.

CREATE TABLE IF NOT EXISTS auth_sessions (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id             UUID NOT NULL REFERENCES auth_users(id) ON DELETE CASCADE,
    token_hash          TEXT NOT NULL UNIQUE,
    issued_at           TIMESTAMPTZ DEFAULT now(),
    expires_at          TIMESTAMPTZ NOT NULL,
    last_activity_at    TIMESTAMPTZ DEFAULT now(),
    ip_address          TEXT,
    user_agent          TEXT
);

CREATE INDEX IF NOT EXISTS idx_auth_sessions_user
    ON auth_sessions (user_id);
CREATE INDEX IF NOT EXISTS idx_auth_sessions_expires
    ON auth_sessions (expires_at);

-- ============================================================================
-- STEP 3: OTP Table
-- ============================================================================
-- One-time passwords for login 2FA and password resets.
-- Codes are SHA-256 hashed before storage.
-- Max 3 attempts per code, 5-minute expiry.

CREATE TABLE IF NOT EXISTS auth_otp (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id             UUID NOT NULL REFERENCES auth_users(id) ON DELETE CASCADE,
    code_hash           TEXT NOT NULL,
    purpose             TEXT NOT NULL CHECK (purpose IN ('login_2fa', 'password_reset')),
    attempts            INTEGER DEFAULT 0,
    max_attempts        INTEGER DEFAULT 3,
    expires_at          TIMESTAMPTZ NOT NULL,
    used_at             TIMESTAMPTZ,
    created_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_auth_otp_user_purpose
    ON auth_otp (user_id, purpose);
CREATE INDEX IF NOT EXISTS idx_auth_otp_expires
    ON auth_otp (expires_at);

-- ============================================================================
-- STEP 4: Grants
-- ============================================================================
-- The API worker (simsy_reporting_app) needs read + write on auth tables
-- because it creates sessions, OTPs, and user records.
-- RLS is NOT enabled on auth tables — the API application code handles
-- authorization logic (auth queries need to look up users across all tenants).

GRANT SELECT, INSERT, UPDATE, DELETE ON auth_users TO simsy_reporting_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON auth_sessions TO simsy_reporting_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON auth_otp TO simsy_reporting_app;

-- Owner role also gets full access
GRANT ALL ON auth_users TO simsy_reporting;
GRANT ALL ON auth_sessions TO simsy_reporting;
GRANT ALL ON auth_otp TO simsy_reporting;

-- ============================================================================
-- STEP 5: Cleanup Function (optional — run periodically)
-- ============================================================================
-- Removes expired sessions and used/expired OTPs to keep tables tidy.

CREATE OR REPLACE FUNCTION cleanup_auth_expired()
RETURNS void AS $$
BEGIN
    -- Delete expired sessions
    DELETE FROM auth_sessions WHERE expires_at < now();

    -- Delete used or expired OTPs older than 1 hour
    DELETE FROM auth_otp
    WHERE (used_at IS NOT NULL AND created_at < now() - interval '1 hour')
       OR (expires_at < now() AND created_at < now() - interval '1 hour');
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- DONE
-- ============================================================================

SELECT 'Auth schema migration complete.' AS status;
SELECT 'Tables created: auth_users, auth_sessions, auth_otp' AS tables;
SELECT 'Grants applied for: simsy_reporting_app, simsy_reporting' AS grants;
SELECT 'No RLS on auth tables — authorization handled at application level.' AS note;
