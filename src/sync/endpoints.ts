/**
 * Sync: endpoints (Supabase) → rpt_endpoints (PostgreSQL)
 *
 * Strips ALL sensitive identifiers: ICCID, IMSI, MSISDN, IMEI, EID,
 * activation_code, lpa_string, IP address, lat/lon, raw_data
 *
 * Keeps: endpoint name, type, status, rolling usage/charge metrics, activity dates
 *
 * Uses bulk INSERT with multi-row VALUES for performance.
 */

import type postgres from 'postgres';
import type { SupabaseClient } from '../supabase-client';
import type { SupabaseEndpointRecord, SyncResult } from '../types';
import { resolveTenantId } from '../sanitise';

// Only select non-sensitive columns
const SELECT_COLUMNS = [
  'id', 'endpoint_identifier',
  'endpoint_name', 'endpoint_type', 'endpoint_type_name',
  'status', 'endpoint_status_name', 'endpoint_network_status_name',
  'tenant_id', 'customer_id',
  'usage_rolling_24h', 'usage_rolling_7d', 'usage_rolling_28d', 'usage_rolling_1y',
  'charge_rolling_24h', 'charge_rolling_7d', 'charge_rolling_28d', 'charge_rolling_1y',
  'first_activity', 'latest_activity',
  'created_at', 'updated_at',
  // Deliberately NOT selecting:
  // iccid, imsi, msisdn, imei, eid, epid, activation_code, lpa_string,
  // ip_address, endpoint_http_address, endpoint_http_addresses,
  // latest_lat, latest_lon, raw_data, identities, tags
].join(',');

const BULK_INSERT_SIZE = 500;

/** Escape a value for a raw SQL VALUES clause. */
function esc(v: string | number | null | undefined): string {
  if (v === null || v === undefined) return 'NULL';
  if (typeof v === 'number') return String(v);
  return `'${String(v).replace(/'/g, "''")}'`;
}

export async function syncEndpoints(
  supabase: SupabaseClient,
  sql: postgres.Sql,
  watermark: string | null,
  batchSize: number
): Promise<SyncResult> {
  const start = Date.now();
  let recordsSynced = 0;

  try {
    console.log(`[ENDPOINTS] Starting sync (watermark: ${watermark || 'none'})`);

    const records = await supabase.fetchAll<SupabaseEndpointRecord>(
      'endpoints',
      {
        select: SELECT_COLUMNS,
        watermark,
        watermarkColumn: 'updated_at',
        batchSize,
      }
    );

    console.log(`[ENDPOINTS] Fetched ${records.length} records from Supabase`);

    if (records.length === 0) {
      return { table: 'rpt_endpoints', recordsSynced: 0, duration: Date.now() - start };
    }

    // Map and filter records
    const mapped: string[] = [];
    for (const r of records) {
      const tenantId = resolveTenantId(null, r.tenant_id);
      if (!tenantId) continue;

      // If status/endpoint_status_name are null, fall back to network_status_name
      const effectiveStatus = r.status || r.endpoint_status_name || r.endpoint_network_status_name || null;
      const effectiveStatusName = r.endpoint_status_name || r.endpoint_network_status_name || r.status || null;

      mapped.push(`(${esc(r.endpoint_identifier)}, ${esc(tenantId)}, ${esc(r.customer_id)}, ${esc(r.endpoint_name)}, ${esc(r.endpoint_type)}, ${esc(r.endpoint_type_name)}, ${esc(effectiveStatus)}, ${esc(effectiveStatusName)}, ${esc(r.endpoint_network_status_name)}, ${esc(r.usage_rolling_24h)}, ${esc(r.usage_rolling_7d)}, ${esc(r.usage_rolling_28d)}, ${esc(r.usage_rolling_1y)}, ${esc(r.charge_rolling_24h)}, ${esc(r.charge_rolling_7d)}, ${esc(r.charge_rolling_28d)}, ${esc(r.charge_rolling_1y)}, ${esc(r.first_activity)}, ${esc(r.latest_activity)}, NOW())`);
    }

    console.log(`[ENDPOINTS] Mapped ${mapped.length} records (${records.length - mapped.length} skipped — unknown tenant)`);

    if (mapped.length === 0) {
      return { table: 'rpt_endpoints', recordsSynced: 0, duration: Date.now() - start };
    }

    // Bulk insert in chunks
    for (let i = 0; i < mapped.length; i += BULK_INSERT_SIZE) {
      const chunk = mapped.slice(i, i + BULK_INSERT_SIZE);
      const valuesClauses = chunk.join(',\n');

      await sql.unsafe(`
        INSERT INTO rpt_endpoints (
          source_id, tenant_id, customer_id,
          endpoint_name, endpoint_type, endpoint_type_name,
          status, endpoint_status_name, network_status_name,
          usage_rolling_24h, usage_rolling_7d, usage_rolling_28d, usage_rolling_1y,
          charge_rolling_24h, charge_rolling_7d, charge_rolling_28d, charge_rolling_1y,
          first_activity, latest_activity, synced_at
        ) VALUES ${valuesClauses}
        ON CONFLICT (source_id, tenant_id) WHERE source_id IS NOT NULL
        DO UPDATE SET
          endpoint_name = EXCLUDED.endpoint_name,
          status = EXCLUDED.status,
          endpoint_status_name = EXCLUDED.endpoint_status_name,
          network_status_name = EXCLUDED.network_status_name,
          usage_rolling_24h = EXCLUDED.usage_rolling_24h,
          usage_rolling_7d = EXCLUDED.usage_rolling_7d,
          usage_rolling_28d = EXCLUDED.usage_rolling_28d,
          usage_rolling_1y = EXCLUDED.usage_rolling_1y,
          charge_rolling_24h = EXCLUDED.charge_rolling_24h,
          charge_rolling_7d = EXCLUDED.charge_rolling_7d,
          charge_rolling_28d = EXCLUDED.charge_rolling_28d,
          charge_rolling_1y = EXCLUDED.charge_rolling_1y,
          latest_activity = EXCLUDED.latest_activity,
          synced_at = NOW()
      `);

      recordsSynced += chunk.length;

      if (recordsSynced % 5000 === 0 || i + BULK_INSERT_SIZE >= mapped.length) {
        console.log(`[ENDPOINTS] Upserted ${recordsSynced} / ${mapped.length}`);
      }
    }

    console.log(`[ENDPOINTS] Sync complete: ${recordsSynced} records in ${((Date.now() - start) / 1000).toFixed(1)}s`);
    return { table: 'rpt_endpoints', recordsSynced, duration: Date.now() - start };
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error);
    console.error(`[ENDPOINTS] Sync failed: ${msg}`);
    return { table: 'rpt_endpoints', recordsSynced, duration: Date.now() - start, error: msg };
  }
}
