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
