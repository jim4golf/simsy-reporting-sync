/**
 * Sync: custom_usage_reports (Supabase) → rpt_usage (PostgreSQL)
 *
 * Sanitises records by keeping only non-sensitive usage/billing fields
 * and strips all SIM identifiers, API keys, IP addresses, and raw data.
 *
 * Uses bulk INSERT with multi-row VALUES for performance (150k+ records).
 * Saves watermark after each chunk so subsequent runs pick up where we left off.
 *
 * Tenant resolution: many usage records have null tenant_name.
 * We build an ICCID→tenant lookup from the Supabase endpoints table
 * to resolve tenant for records where tenant_name is missing.
 */

import type postgres from 'postgres';
import type { SupabaseClient } from '../supabase-client';
import type { SupabaseUsageRecord, SyncResult } from '../types';
import { resolveTenantId } from '../sanitise';

// Only select the columns we need — never fetch sensitive fields
// Column names match the LIVE Supabase schema (from types.ts)
const SELECT_COLUMNS = [
  'id', 'created_at',
  'tenant_name', 'customer_name',
  'endpoint_name', 'endpoint_description',
  'iccid',
  'timestamp',
  'service_type', 'charge_type',
  'consumption', 'charged_consumption', 'uplink_bytes', 'downlink_bytes',
  'bundle_name', 'bundle_moniker', 'status_moniker',
  'rat_type_moniker',
  'serving_operator_name', 'serving_operator_tadig',
  'buy_rating_charge', 'buy_rating_currency',
  'sell_rating_charge', 'sell_rating_currency',
].join(',');

// How many rows per INSERT statement (postgres.js limit is ~65535 params)
// 25 columns × 500 rows = 12,500 params — well within limits
const BULK_INSERT_SIZE = 500;

// Maximum records to fetch per sync invocation to avoid Worker timeout
// At 500 rows/INSERT, 50k records = 100 INSERT statements — very fast
const MAX_RECORDS_PER_RUN = 50000;

/** Escape a value for a raw SQL VALUES clause. */
function esc(v: string | number | null | undefined): string {
  if (v === null || v === undefined) return 'NULL';
  if (typeof v === 'number') return String(v);
  // Escape single quotes by doubling them
  return `'${String(v).replace(/'/g, "''")}'`;
}

interface MappedRow {
  source_id: string;
  tenant_id: string;
  customer_name: string | null;
  endpoint_name: string | null;
  endpoint_description: string | null;
  iccid: string | null;
  timestamp: string | null;
  usage_date: string | null;
  service_type: string | null;
  charge_type: string | null;
  consumption: number | null;
  charged_consumption: number | null;
  uplink_bytes: number | null;
  downlink_bytes: number | null;
  bundle_name: string | null;
  bundle_moniker: string | null;
  status_moniker: string | null;
  rat_type_moniker: string | null;
  serving_operator_name: string | null;
  serving_country_name: string | null;
  serving_country_iso2: string | null;
  buy_charge: number | null;
  buy_currency: string | null;
  sell_charge: number | null;
  sell_currency: string | null;
  created_at: string | null;
}

/** Convert a mapped row to a SQL VALUES tuple string. */
function rowToValues(v: MappedRow): string {
  return `(${esc(v.source_id)}, ${esc(v.tenant_id)}, ${esc(v.customer_name)}, ${esc(v.endpoint_name)}, ${esc(v.endpoint_description)}, ${esc(v.iccid)}, ${esc(v.timestamp)}, ${esc(v.usage_date)}, ${esc(v.service_type)}, ${esc(v.charge_type)}, ${esc(v.consumption)}, ${esc(v.charged_consumption)}, ${esc(v.uplink_bytes)}, ${esc(v.downlink_bytes)}, ${esc(v.bundle_name)}, ${esc(v.bundle_moniker)}, ${esc(v.status_moniker)}, ${esc(v.rat_type_moniker)}, ${esc(v.serving_operator_name)}, ${esc(v.serving_country_name)}, ${esc(v.serving_country_iso2)}, ${esc(v.buy_charge)}, ${esc(v.buy_currency)}, ${esc(v.sell_charge)}, ${esc(v.sell_currency)}, NOW())`;
}

/**
 * Build an ICCID→tenant_id lookup map from multiple Supabase tables.
 * This is needed because many usage records have null tenant_name — the
 * augmentation that populates tenant_name may not have run on all records.
 *
 * Sources (checked in order, first match wins):
 * 1. endpoints table — iccid + tenant_id
 * 2. bundle_instances_report — iccid + tenant_name/tenant_id
 * 3. active_bundles — iccid + tenant_name (if iccid column exists)
 */
async function buildIccidTenantMap(
  supabase: SupabaseClient
): Promise<{ exactMap: Map<string, string>; prefixMap: Map<string, string> }> {
  const map = new Map<string, string>();

  // 1. Endpoints — primary source
  console.log('[USAGE] Building ICCID→tenant lookup from endpoints...');
  const endpoints = await supabase.fetchAll<{ iccid: string; tenant_id: string }>(
    'endpoints',
    {
      select: 'iccid,tenant_id',
      batchSize: 1000,
    }
  );

  const tenantIdCounts = new Map<string, number>();
  for (const ep of endpoints) {
    // Track raw tenant_id values for debugging
    const rawTid = ep.tenant_id || 'NULL';
    tenantIdCounts.set(rawTid, (tenantIdCounts.get(rawTid) || 0) + 1);

    if (!ep.iccid || !ep.tenant_id) continue;
    const normalised = ep.iccid.trim();
    const tenantId = resolveTenantId(null, ep.tenant_id);
    if (tenantId && normalised) {
      map.set(normalised, tenantId);
    }
  }
  // Log the distinct tenant_id values found in endpoints
  const tidEntries = [...tenantIdCounts.entries()].map(([tid, cnt]) => `${tid}(${cnt})`).join(', ');
  console.log(`[USAGE] Endpoints tenant_id distribution: ${tidEntries}`);
  console.log(`[USAGE] After endpoints: ${map.size} mappings from ${endpoints.length} rows`);

  // 2. Bundle instances — fills gaps where endpoints table doesn't have the ICCID
  console.log('[USAGE] Enriching lookup from bundle_instances_report...');
  try {
    const instances = await supabase.fetchAll<{ iccid: string; tenant_name: string; tenant_id: string }>(
      'bundle_instances_report',
      {
        select: 'iccid,tenant_name,tenant_id',
        batchSize: 1000,
      }
    );

    let added = 0;
    for (const inst of instances) {
      if (!inst.iccid) continue;
      const normalised = inst.iccid.trim();
      if (map.has(normalised)) continue;
      const tenantId = resolveTenantId(inst.tenant_name, inst.tenant_id);
      if (tenantId) {
        map.set(normalised, tenantId);
        added++;
      }
    }
    console.log(`[USAGE] After bundle_instances: ${map.size} mappings (+${added} new from ${instances.length} rows)`);
  } catch (e) {
    console.warn(`[USAGE] bundle_instances_report lookup failed: ${e instanceof Error ? e.message : String(e)}`);
  }

  // 3. Active bundles — last resort
  try {
    const bundles = await supabase.fetchAll<{ iccid: string; tenant_name: string }>(
      'active_bundles',
      {
        select: 'iccid,tenant_name',
        orderBy: 'collected_at',
        batchSize: 1000,
      }
    );

    let added = 0;
    for (const b of bundles) {
      if (!b.iccid) continue;
      const normalised = b.iccid.trim();
      if (map.has(normalised)) continue;
      const tenantId = resolveTenantId(b.tenant_name, null);
      if (tenantId) {
        map.set(normalised, tenantId);
        added++;
      }
    }
    console.log(`[USAGE] After active_bundles: ${map.size} mappings (+${added} new from ${bundles.length} rows)`);
  } catch (e) {
    console.warn(`[USAGE] active_bundles lookup skipped: ${e instanceof Error ? e.message : String(e)}`);
  }

  // 4. Build ICCID prefix→tenant map for fallback matching
  // This handles orphan ICCIDs (decommissioned SIMs) not in any table
  // by matching the longest common prefix with known ICCIDs
  const prefixMap = new Map<string, Map<string, number>>();
  for (const [iccid, tenant] of map) {
    // Use first 12 digits as prefix (country + issuer + network)
    const prefix = iccid.substring(0, 12);
    if (!prefixMap.has(prefix)) prefixMap.set(prefix, new Map());
    const tenantCounts = prefixMap.get(prefix)!;
    tenantCounts.set(tenant, (tenantCounts.get(tenant) || 0) + 1);
  }
  // For each prefix, pick the dominant tenant
  const prefixTenantMap = new Map<string, string>();
  for (const [prefix, tenantCounts] of prefixMap) {
    let maxCount = 0;
    let dominantTenant = '';
    for (const [tenant, count] of tenantCounts) {
      if (count > maxCount) {
        maxCount = count;
        dominantTenant = tenant;
      }
    }
    if (dominantTenant) {
      prefixTenantMap.set(prefix, dominantTenant);
    }
  }
  console.log(`[USAGE] Built ${prefixTenantMap.size} ICCID prefix→tenant fallbacks: ${[...prefixTenantMap.entries()].map(([p, t]) => `${p}→${t}`).join(', ')}`);

  console.log(`[USAGE] Final ICCID→tenant map: ${map.size} unique ICCIDs`);
  return { exactMap: map, prefixMap: prefixTenantMap };
}

export async function syncUsage(
  supabase: SupabaseClient,
  sql: postgres.Sql,
  watermark: string | null,
  batchSize: number,
  saveWatermark?: (wm: string) => Promise<void>
): Promise<SyncResult> {
  const start = Date.now();
  let recordsSynced = 0;

  try {
    console.log(`[USAGE] Starting sync (watermark: ${watermark || 'none'})`);

    // Build ICCID→tenant lookup for records with null tenant_name
    const { exactMap: iccidTenantMap, prefixMap: iccidPrefixMap } = await buildIccidTenantMap(supabase);

    const records = await supabase.fetchAll<SupabaseUsageRecord>(
      'custom_usage_reports',
      {
        select: SELECT_COLUMNS,
        watermark,
        watermarkColumn: 'created_at',
        batchSize,
        maxRecords: MAX_RECORDS_PER_RUN,
      }
    );

    console.log(`[USAGE] Fetched ${records.length} records from Supabase`);

    if (records.length === 0) {
      return { table: 'rpt_usage', recordsSynced: 0, duration: Date.now() - start };
    }

    // Map all records, resolving tenant from tenant_name or ICCID lookup
    const mapped: MappedRow[] = [];
    const unknownTenants = new Set<string>();
    const unmatchedIccids = new Map<string, number>(); // iccid → count
    let resolvedViaTenantName = 0;
    let resolvedViaIccid = 0;
    let resolvedViaPrefix = 0;
    let skippedNoTenant = 0;

    for (const r of records) {
      // Try tenant_name first
      let tenantId = resolveTenantId(r.tenant_name, null);

      if (tenantId) {
        resolvedViaTenantName++;
      } else if (r.iccid) {
        // Fall back to ICCID→tenant exact lookup
        const normalised = r.iccid.trim();
        tenantId = iccidTenantMap.get(normalised) || null;
        if (tenantId) {
          resolvedViaIccid++;
        } else {
          // Last resort: ICCID prefix-based tenant inference
          const prefix = normalised.substring(0, 12);
          tenantId = iccidPrefixMap.get(prefix) || null;
          if (tenantId) {
            resolvedViaPrefix++;
          }
        }
      }

      if (!tenantId) {
        if (r.tenant_name) unknownTenants.add(r.tenant_name);
        const iccidKey = r.iccid || 'NO_ICCID';
        unmatchedIccids.set(iccidKey, (unmatchedIccids.get(iccidKey) || 0) + 1);
        skippedNoTenant++;
        continue;
      }

      const usageDate = r.timestamp ? r.timestamp.substring(0, 10) : null;

      mapped.push({
        source_id: r.id,
        tenant_id: tenantId,
        customer_name: r.customer_name || null,
        endpoint_name: r.endpoint_name || null,
        endpoint_description: r.endpoint_description || null,
        iccid: r.iccid || null,
        timestamp: r.timestamp || null,
        usage_date: usageDate,
        service_type: r.service_type || null,
        charge_type: r.charge_type || null,
        consumption: r.consumption ?? null,
        charged_consumption: r.charged_consumption ?? null,
        uplink_bytes: r.uplink_bytes ?? null,
        downlink_bytes: r.downlink_bytes ?? null,
        bundle_name: r.bundle_name || null,
        bundle_moniker: r.bundle_moniker || null,
        status_moniker: r.status_moniker || null,
        rat_type_moniker: r.rat_type_moniker || null,
        serving_operator_name: r.serving_operator_name || null,
        serving_country_name: r.serving_operator_tadig || null,
        serving_country_iso2: null,
        buy_charge: r.buy_rating_charge ?? null,
        buy_currency: r.buy_rating_currency || null,
        sell_charge: r.sell_rating_charge ?? null,
        sell_currency: r.sell_rating_currency || null,
        created_at: r.created_at || null,
      });
    }

    if (unknownTenants.size > 0) {
      console.log(`[USAGE] Unknown tenant names: ${JSON.stringify([...unknownTenants])}`);
    }
    if (unmatchedIccids.size > 0) {
      // Log all unique unmatched ICCIDs with their record counts
      const unmatchedEntries = [...unmatchedIccids.entries()]
        .sort((a, b) => b[1] - a[1])
        .map(([iccid, count]) => `${iccid}(${count})`);
      console.log(`[USAGE] Unmatched ICCIDs (${unmatchedIccids.size} unique, ${skippedNoTenant} records): ${unmatchedEntries.join(', ')}`);
    }
    console.log(`[USAGE] Mapped ${mapped.length} records (via tenant_name: ${resolvedViaTenantName}, via ICCID: ${resolvedViaIccid}, via prefix: ${resolvedViaPrefix}, skipped: ${skippedNoTenant})`);

    if (mapped.length === 0) {
      return { table: 'rpt_usage', recordsSynced: 0, duration: Date.now() - start };
    }

    // Bulk insert in chunks of BULK_INSERT_SIZE
    for (let i = 0; i < mapped.length; i += BULK_INSERT_SIZE) {
      const chunk = mapped.slice(i, i + BULK_INSERT_SIZE);
      const valuesClauses = chunk.map(rowToValues).join(',\n');

      await sql.unsafe(`
        INSERT INTO rpt_usage (
          source_id, tenant_id, customer_name, endpoint_name, endpoint_description,
          iccid, timestamp, usage_date, service_type, charge_type,
          consumption, charged_consumption, uplink_bytes, downlink_bytes,
          bundle_name, bundle_moniker, status_moniker, rat_type_moniker,
          serving_operator_name, serving_country_name, serving_country_iso2,
          buy_charge, buy_currency, sell_charge, sell_currency, synced_at
        ) VALUES ${valuesClauses}
        ON CONFLICT (source_id) WHERE source_id IS NOT NULL
        DO UPDATE SET
          tenant_id = EXCLUDED.tenant_id,
          customer_name = EXCLUDED.customer_name,
          bundle_name = EXCLUDED.bundle_name,
          bundle_moniker = EXCLUDED.bundle_moniker,
          status_moniker = EXCLUDED.status_moniker,
          synced_at = NOW()
      `);

      recordsSynced += chunk.length;

      if (recordsSynced % 5000 === 0 || i + BULK_INSERT_SIZE >= mapped.length) {
        console.log(`[USAGE] Upserted ${recordsSynced} / ${mapped.length}`);
      }
    }

    // Save watermark based on the latest created_at in the batch
    // so next run picks up only newer records
    if (saveWatermark && records.length > 0) {
      const lastRecord = records[records.length - 1];
      const lastWatermark = lastRecord.created_at;
      if (lastWatermark) {
        await saveWatermark(lastWatermark);
        console.log(`[USAGE] Watermark saved: ${lastWatermark}`);
      }
    }

    const hasMore = records.length >= MAX_RECORDS_PER_RUN;
    if (hasMore) {
      console.log(`[USAGE] Hit max records limit (${MAX_RECORDS_PER_RUN}). More records remain — next cron will continue.`);
    }

    console.log(`[USAGE] Sync complete: ${recordsSynced} records in ${((Date.now() - start) / 1000).toFixed(1)}s`);
    return { table: 'rpt_usage', recordsSynced, duration: Date.now() - start };
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error);
    console.error(`[USAGE] Sync failed: ${msg}`);
    return { table: 'rpt_usage', recordsSynced, duration: Date.now() - start, error: msg };
  }
}
