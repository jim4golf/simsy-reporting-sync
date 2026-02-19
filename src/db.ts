/**
 * PostgreSQL database client using the postgres (postgres.js) library.
 * Connects via Cloudflare Hyperdrive for connection pooling.
 */

import postgres from 'postgres';
import type { Env } from './types';

export function createDbClient(env: Env) {
  // Hyperdrive provides a connection string that routes through Cloudflare's
  // connection pooling infrastructure to the Hetzner PostgreSQL server.
  return postgres(env.HYPERDRIVE.connectionString, {
    // Hyperdrive manages the connection pool — we use a single connection per invocation
    max: 1,
    // Disable prepare statements (required for Hyperdrive compatibility)
    prepare: false,
    // Connection timeout
    connect_timeout: 10,
    // Idle timeout
    idle_timeout: 20,
  });
}

/**
 * Backfill data_used_mb on rpt_bundle_instances by aggregating
 * charged_consumption from rpt_usage.
 *
 * Strategy 1: ICCID + bundle_moniker + sequence (case-insensitive)
 * Strategy 2: ICCID + date range (usage falls within instance start/end)
 *
 * charged_consumption is in bytes — convert to MB.
 * ICCIDs may have trailing whitespace, so we TRIM.
 */
export async function backfillBundleUsage(sql: postgres.Sql) {
  console.log('[DB] Backfilling bundle instance data usage from charged_consumption...');

  try {
    // First trim trailing whitespace on ICCIDs
    const trimResult = await sql.unsafe(`
      UPDATE rpt_bundle_instances SET iccid = TRIM(iccid)
      WHERE iccid IS NOT NULL AND iccid != TRIM(iccid)
    `);
    if (Number(trimResult.count) > 0) {
      console.log(`[DB] Trimmed whitespace from ${trimResult.count} ICCIDs`);
    }

    // Reset all data_used_mb to recompute fresh each cycle
    await sql.unsafe(`UPDATE rpt_bundle_instances SET data_used_mb = NULL WHERE data_used_mb IS NOT NULL`);

    // Strategy 1: Match by ICCID + bundle moniker + sequence (case-insensitive)
    const result1 = await sql.unsafe(`
      UPDATE rpt_bundle_instances bi
      SET data_used_mb = usage.total_mb
      FROM (
        SELECT
          TRIM(iccid) AS iccid,
          LOWER(bundle_moniker) AS bundle_moniker_lc,
          sequence,
          ROUND(SUM(COALESCE(charged_consumption, 0)) / (1024.0 * 1024.0))::BIGINT AS total_mb
        FROM rpt_usage
        WHERE iccid IS NOT NULL
          AND charged_consumption > 0
          AND bundle_moniker IS NOT NULL
          AND sequence IS NOT NULL
        GROUP BY TRIM(iccid), LOWER(bundle_moniker), sequence
      ) usage
      WHERE TRIM(bi.iccid) = usage.iccid
        AND LOWER(bi.bundle_moniker) = usage.bundle_moniker_lc
        AND bi.sequence = usage.sequence
    `);
    console.log(`[DB] Backfill (ICCID+moniker+seq): updated ${result1.count} instances`);

    // Strategy 2: For remaining unmatched, use ICCID + date range
    // Sum all charged_consumption where usage_date falls within instance period
    const result2 = await sql.unsafe(`
      UPDATE rpt_bundle_instances bi
      SET data_used_mb = usage.total_mb
      FROM (
        SELECT
          TRIM(u.iccid) AS iccid,
          bi2.id AS instance_id,
          ROUND(SUM(COALESCE(u.charged_consumption, 0)) / (1024.0 * 1024.0))::BIGINT AS total_mb
        FROM rpt_usage u
        INNER JOIN rpt_bundle_instances bi2
          ON TRIM(u.iccid) = TRIM(bi2.iccid)
          AND u.usage_date >= bi2.start_time::date
          AND u.usage_date <= bi2.end_time::date
        WHERE u.iccid IS NOT NULL
          AND u.charged_consumption > 0
          AND bi2.data_used_mb IS NULL
          AND bi2.start_time IS NOT NULL
          AND bi2.end_time IS NOT NULL
        GROUP BY TRIM(u.iccid), bi2.id
      ) usage
      WHERE bi.id = usage.instance_id
        AND bi.data_used_mb IS NULL
    `);
    console.log(`[DB] Backfill (ICCID+daterange): updated ${result2.count} additional instances`);

  } catch (error) {
    console.error('[DB] Backfill data_used_mb failed:', error instanceof Error ? error.message : String(error));
  }
}

/**
 * Refresh all materialised views after a sync cycle.
 * Uses CONCURRENTLY to avoid blocking reads during refresh.
 */
export async function refreshMaterialisedViews(sql: postgres.Sql) {
  console.log('[DB] Refreshing materialised views...');

  const views = [
    'mv_usage_daily',
    'mv_usage_monthly',
    'mv_usage_annual',
    'mv_bundle_expiry',
  ];

  for (const view of views) {
    try {
      // CONCURRENTLY requires a unique index on the materialised view
      // which we created in the init SQL
      await sql.unsafe(`REFRESH MATERIALIZED VIEW CONCURRENTLY ${view}`);
      console.log(`[DB] Refreshed ${view}`);
    } catch (error) {
      // If CONCURRENTLY fails (e.g., view is empty), fall back to regular refresh
      console.warn(`[DB] CONCURRENTLY failed for ${view}, trying regular refresh`);
      try {
        await sql.unsafe(`REFRESH MATERIALIZED VIEW ${view}`);
        console.log(`[DB] Refreshed ${view} (non-concurrent)`);
      } catch (fallbackError) {
        console.error(`[DB] Failed to refresh ${view}:`, fallbackError);
      }
    }
  }
}
