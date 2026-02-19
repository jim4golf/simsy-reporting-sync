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
 * charged_consumption from rpt_usage per bundle_instance_id.
 * charged_consumption is in bytes — convert to MB.
 */
export async function backfillBundleUsage(sql: postgres.Sql) {
  console.log('[DB] Backfilling bundle instance data usage from charged_consumption...');

  try {
    const result = await sql.unsafe(`
      UPDATE rpt_bundle_instances bi
      SET data_used_mb = usage.total_mb
      FROM (
        SELECT
          bundle_instance_id,
          ROUND(SUM(COALESCE(charged_consumption, 0)) / (1024.0 * 1024.0))::BIGINT AS total_mb
        FROM rpt_usage
        WHERE bundle_instance_id IS NOT NULL
          AND bundle_instance_id != ''
        GROUP BY bundle_instance_id
      ) usage
      WHERE bi.bundle_instance_id = usage.bundle_instance_id
        AND bi.bundle_instance_id IS NOT NULL
    `);
    console.log(`[DB] Backfill complete: updated ${result.count} bundle instances with data_used_mb`);
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
