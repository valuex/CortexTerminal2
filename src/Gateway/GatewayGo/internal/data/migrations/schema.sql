-- CortexTerminal2 GatewayGo — full schema + backfills.
--
-- This single file is the SQLite-native equivalent of the 19 EF Core
-- migrations in src/Gateway/CortexTerminal.Gateway/Migrations/. They are
-- applied in order on a fresh database. The `__migrations` table tracks
-- which migrations have already run so re-running this on an existing C#-
-- produced database is a no-op (each migration is idempotent).
--
-- EF Core types vs SQLite:
--   text            → TEXT
--   character varying(N) → TEXT (SQLite ignores length, kept as documentation)
--   bigint          → INTEGER
--   integer         → INTEGER
--   boolean         → INTEGER (0/1)
--   timestamp with time zone → TEXT (ISO 8601 UTC)
--   bytea           → BLOB
--
-- Order matches the timestamp-prefixed filenames in C#:
--   20260528070740_InitialCreate
--   20260601085900_AddAvatarColumns
--   20260609214257_AddSessionName
--   20260612033923_AddUserIdentities
--   20260622014124_AddPasswordHashToUserIdentity
--   20260622020000_BackfillPasswordIdentities
--   20260622030000_BackfillExistingPasswordIdentities
--   20260622040000_SeedMissingPasswordIdentities
--   20260622050000_MergeDuplicatePhoneAccounts
--   20260622220533_AddLastLoginAtUtcAndSessionBytes
--   20260622223744_AddSessionWorkerConnectionId
--   20260623092023_AddUserPreferences
--   20260624120805_AddArtifacts
--   20260625082948_AddAgentTracking
--   20260626110703_AddAuditRequestInfo
--   20260706090745_DropSessionLeaseExpires
--   20260801060017_AddTunnels
--
-- Note: the `auth_provider` column on Users is kept for legacy compatibility
-- with the original schema; new code should read from UserIdentities.

-- === 20260528070740 InitialCreate ===
CREATE TABLE IF NOT EXISTS AuditLogs (
    id              TEXT PRIMARY KEY,
    timestamp       TEXT NOT NULL,
    user_id         TEXT NOT NULL,
    user_name       TEXT NOT NULL,
    action          TEXT NOT NULL,
    target_entity   TEXT NOT NULL,
    target_id       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS IX_AuditLogs_action       ON AuditLogs(action);
CREATE INDEX IF NOT EXISTS IX_AuditLogs_timestamp    ON AuditLogs(timestamp);
CREATE INDEX IF NOT EXISTS IX_AuditLogs_user_id      ON AuditLogs(user_id);

CREATE TABLE IF NOT EXISTS Sessions (
    session_id                       TEXT PRIMARY KEY,
    user_id                          TEXT NOT NULL,
    worker_id                        TEXT NOT NULL,
    columns                          INTEGER NOT NULL,
    rows                             INTEGER NOT NULL,
    created_at_utc                   TEXT NOT NULL,
    last_activity_at_utc              TEXT NOT NULL,
    attachment_state                 TEXT NOT NULL,
    attached_client_connection_id    TEXT NULL,
    lease_expires_at_utc             TEXT NULL,
    exit_code                        INTEGER NULL,
    exit_reason                      TEXT NULL,
    replay_pending                   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS IX_Sessions_attachment_state   ON Sessions(attachment_state);
CREATE INDEX IF NOT EXISTS IX_Sessions_created_at_utc      ON Sessions(created_at_utc);
CREATE INDEX IF NOT EXISTS IX_Sessions_user_id             ON Sessions(user_id);
CREATE INDEX IF NOT EXISTS IX_Sessions_worker_id           ON Sessions(worker_id);

CREATE TABLE IF NOT EXISTS Users (
    id                  TEXT PRIMARY KEY,
    username            TEXT NOT NULL,
    email               TEXT NULL,
    display_name        TEXT NULL,
    avatar_url          TEXT NULL,
    role                TEXT NOT NULL,
    status              TEXT NOT NULL,
    auth_provider       TEXT NULL,
    auth_provider_id    TEXT NULL,
    password_hash       TEXT NULL,
    apple_refresh_token TEXT NULL,
    deleted_at_utc      TEXT NULL,
    created_at_utc      TEXT NOT NULL,
    updated_at_utc      TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS IX_Users_username ON Users(username);
CREATE INDEX IF NOT EXISTS IX_Users_auth_provider_auth_provider_id ON Users(auth_provider, auth_provider_id);

CREATE TABLE IF NOT EXISTS Workers (
    worker_id                TEXT PRIMARY KEY,
    owner_user_id            TEXT NULL,
    hostname                 TEXT NULL,
    operating_system         TEXT NULL,
    architecture             TEXT NULL,
    name                     TEXT NULL,
    version                  TEXT NULL,
    last_seen_at_utc         TEXT NOT NULL,
    first_connected_at_utc   TEXT NULL,
    is_online                INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS IX_Workers_owner_user_id   ON Workers(owner_user_id);
CREATE INDEX IF NOT EXISTS IX_Workers_is_online       ON Workers(is_online);

-- === 20260601085900 AddAvatarColumns ===
ALTER TABLE Users ADD COLUMN avatar_content_type TEXT NULL;
ALTER TABLE Users ADD COLUMN avatar_data         BLOB NULL;

-- === 20260609214257 AddSessionName ===
-- SQLite ALTER TABLE supports ADD COLUMN natively; no recreation needed.
ALTER TABLE Sessions ADD COLUMN name TEXT NULL;

-- === 20260612033923 AddUserIdentities ===
CREATE TABLE IF NOT EXISTS UserIdentities (
    id                  TEXT PRIMARY KEY,
    user_id             TEXT NOT NULL,
    auth_provider       TEXT NOT NULL,
    auth_provider_id    TEXT NOT NULL,
    email               TEXT NULL,
    phone_normalized    TEXT NULL,
    created_at_utc      TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS IX_UserIdentities_auth_provider_auth_provider_id
    ON UserIdentities(auth_provider, auth_provider_id);
CREATE INDEX IF NOT EXISTS IX_UserIdentities_email
    ON UserIdentities(email);
CREATE INDEX IF NOT EXISTS IX_UserIdentities_phone_normalized
    ON UserIdentities(phone_normalized);
CREATE INDEX IF NOT EXISTS IX_UserIdentities_user_id
    ON UserIdentities(user_id);

-- Seed identities from existing Users (matches EF seed SQL semantics,
-- with SQLite-compatible replacements for gen_random_uuid() and
-- REGEXP_REPLACE).
INSERT OR IGNORE INTO UserIdentities (id, user_id, auth_provider, auth_provider_id, email, phone_normalized, created_at_utc)
SELECT
    lower(hex(randomblob(4))) || lower(hex(randomblob(4))) || lower(hex(randomblob(4))) || lower(hex(randomblob(4))),
    u.id,
    COALESCE(u.auth_provider, 'password'),
    COALESCE(u.auth_provider_id, u.username),
    u.email,
    CASE
        WHEN u.auth_provider IN ('phone', 'huawei') AND u.auth_provider_id IS NOT NULL
        THEN substr(u.auth_provider_id, max(length(u.auth_provider_id) - 10, 1))
        ELSE NULL
    END,
    COALESCE(u.created_at_utc, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
FROM Users u
WHERE u.status = 'active';

-- === 20260622014124 AddPasswordHashToUserIdentity ===
ALTER TABLE UserIdentities ADD COLUMN password_hash TEXT NULL;

-- === 20260622020000 BackfillPasswordIdentities ===
INSERT OR IGNORE INTO UserIdentities (id, user_id, auth_provider, auth_provider_id, password_hash, created_at_utc)
SELECT
    lower(hex(randomblob(4))) || lower(hex(randomblob(4))) || lower(hex(randomblob(4))) || lower(hex(randomblob(4))),
    u.id,
    'password',
    u.username,
    u.password_hash,
    strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
FROM Users u
WHERE u.password_hash IS NOT NULL
  AND u.password_hash <> ''
  AND NOT EXISTS (
    SELECT 1 FROM UserIdentities ui
    WHERE ui.user_id = u.id
      AND ui.auth_provider = 'password'
  );

-- === 20260622030000 BackfillExistingPasswordIdentities ===
UPDATE UserIdentities
SET password_hash = (
    SELECT u.password_hash
    FROM Users u
    WHERE u.id = UserIdentities.user_id
      AND u.password_hash IS NOT NULL
      AND u.password_hash <> ''
)
WHERE auth_provider = 'password'
  AND (password_hash IS NULL OR password_hash = '')
  AND EXISTS (
    SELECT 1 FROM Users u
    WHERE u.id = UserIdentities.user_id
      AND u.password_hash IS NOT NULL
      AND u.password_hash <> ''
  );

-- === 20260622040000 SeedMissingPasswordIdentities ===
INSERT OR IGNORE INTO UserIdentities (id, user_id, auth_provider, auth_provider_id, email, phone_normalized, password_hash, created_at_utc)
SELECT
    lower(hex(randomblob(4))) || lower(hex(randomblob(4))) || lower(hex(randomblob(4))) || lower(hex(randomblob(4))),
    u.id,
    'password',
    u.username,
    u.email,
    CASE
        WHEN u.auth_provider IN ('phone', 'huawei') AND u.auth_provider_id IS NOT NULL
             AND length(u.auth_provider_id) >= 11
        THEN substr(u.auth_provider_id, length(u.auth_provider_id) - 10)
        ELSE NULL
    END,
    u.password_hash,
    strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
FROM Users u
WHERE u.password_hash IS NOT NULL
  AND u.password_hash <> ''
  AND NOT EXISTS (
    SELECT 1 FROM UserIdentities ui
    WHERE ui.user_id = u.id
      AND ui.auth_provider = 'password'
  );

UPDATE UserIdentities
SET phone_normalized = substr(phone_normalized, length(phone_normalized) - 10)
WHERE phone_normalized IS NOT NULL
  AND length(phone_normalized) > 11;

-- === 20260622050000 MergeDuplicatePhoneAccounts ===
-- The merge is hard-coded to two specific users from the original C# deploy.
-- On a fresh database, the UPDATEs no-op and the DELETE removes nothing.
UPDATE UserIdentities
SET phone_normalized = '17701565543'
WHERE user_id = '94ebf2c1ee80476faaa1c46d1295aa0b'
  AND auth_provider = 'password'
  AND (phone_normalized IS NULL OR phone_normalized = '');

UPDATE Sessions  SET user_id        = '94ebf2c1ee80476faaa1c46d1295aa0b' WHERE user_id        = '166d9d400007451fb5a7afc298c83f3c';
UPDATE Workers   SET owner_user_id  = '94ebf2c1ee80476faaa1c46d1295aa0b' WHERE owner_user_id  = '166d9d400007451fb5a7afc298c83f3c';
UPDATE AuditLogs SET user_id        = '94ebf2c1ee80476faaa1c46d1295aa0b' WHERE user_id        = '166d9d400007451fb5a7afc298c83f3c';
DELETE FROM UserIdentities WHERE user_id = '166d9d400007451fb5a7afc298c83f3c';
DELETE FROM Users          WHERE id      = '166d9d400007451fb5a7afc298c83f3c';

-- === 20260622220533 AddLastLoginAtUtcAndSessionBytes ===
ALTER TABLE Users    ADD COLUMN last_login_at_utc TEXT NULL;
ALTER TABLE Sessions ADD COLUMN bytes_ingested     INTEGER NOT NULL DEFAULT 0;

-- === 20260622223744 AddSessionWorkerConnectionId ===
ALTER TABLE Sessions ADD COLUMN worker_connection_id TEXT NULL;

-- === 20260623092023 AddUserPreferences ===
CREATE TABLE IF NOT EXISTS UserPreferences (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         TEXT NOT NULL,
    key             TEXT NOT NULL,
    value           TEXT NOT NULL,
    updated_at_utc  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS IX_UserPreferences_user_id ON UserPreferences(user_id);
CREATE UNIQUE INDEX IF NOT EXISTS IX_UserPreferences_user_id_key ON UserPreferences(user_id, key);

-- === 20260624120805 AddArtifacts ===
CREATE TABLE IF NOT EXISTS Artifacts (
    id                  TEXT PRIMARY KEY,
    session_id          TEXT NOT NULL,
    filename            TEXT NOT NULL,
    size_bytes          INTEGER NOT NULL,
    status              TEXT NOT NULL,
    origin              TEXT NOT NULL,
    owner_user_id       TEXT NOT NULL,
    content_sha256      TEXT NULL,
    file_category       TEXT NOT NULL,
    created_at_utc      TEXT NOT NULL,
    completed_at_utc    TEXT NULL,
    expires_at_utc      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS IX_Artifacts_session_id            ON Artifacts(session_id);
CREATE INDEX IF NOT EXISTS IX_Artifacts_owner_user_id          ON Artifacts(owner_user_id);
CREATE INDEX IF NOT EXISTS IX_Artifacts_expires_at_utc         ON Artifacts(expires_at_utc);
CREATE UNIQUE INDEX IF NOT EXISTS IX_Artifacts_session_id_filename ON Artifacts(session_id, filename);

-- === 20260625082948 AddAgentTracking ===
ALTER TABLE Sessions ADD COLUMN agent_kind       TEXT NULL;
ALTER TABLE Sessions ADD COLUMN agent_session_id TEXT NULL;
ALTER TABLE Sessions ADD COLUMN inferred_title   TEXT NULL;

CREATE TABLE IF NOT EXISTS SessionAgentEvents (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    payload         TEXT NOT NULL,
    created_at_utc  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS IX_SessionAgentEvents_session_id_created_at_utc
    ON SessionAgentEvents(session_id, created_at_utc);
CREATE INDEX IF NOT EXISTS IX_SessionAgentEvents_session_id
    ON SessionAgentEvents(session_id);

-- === 20260626110703 AddAuditRequestInfo ===
ALTER TABLE AuditLogs ADD COLUMN client_ip     TEXT NULL;
ALTER TABLE AuditLogs ADD COLUMN device_model  TEXT NULL;
ALTER TABLE AuditLogs ADD COLUMN user_agent    TEXT NULL;

-- === 20260706090745 DropSessionLeaseExpires ===
-- SQLite ALTER TABLE DROP COLUMN is supported in 3.35+. If running against
-- an older SQLite, comment this out and the column becomes a harmless
-- unused TEXT NULL.
ALTER TABLE Sessions DROP COLUMN lease_expires_at_utc;

-- === 20260801060017 AddTunnels ===
CREATE TABLE IF NOT EXISTS Tunnels (
    id                      TEXT PRIMARY KEY,
    tunnel_key              TEXT NOT NULL,
    owner_user_id           TEXT NOT NULL,
    worker_id               TEXT NOT NULL,
    worker_connection_id    TEXT NOT NULL,
    session_id              TEXT NOT NULL,
    port                    INTEGER NOT NULL,
    secret_hash             TEXT NOT NULL,
    transport_type          TEXT NOT NULL,
    expires_at_utc          TEXT NOT NULL,
    created_at_utc          TEXT NOT NULL,
    revoked_at_utc          TEXT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS IX_Tunnels_tunnel_key  ON Tunnels(tunnel_key);
CREATE INDEX IF NOT EXISTS IX_Tunnels_owner_user_id     ON Tunnels(owner_user_id);
CREATE INDEX IF NOT EXISTS IX_Tunnels_session_id        ON Tunnels(session_id);
CREATE INDEX IF NOT EXISTS IX_Tunnels_expires_at_utc    ON Tunnels(expires_at_utc);