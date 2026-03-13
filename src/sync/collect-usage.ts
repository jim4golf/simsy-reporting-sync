/**
 * Collect Custom Usage Reports from S-IMSY API
 *
 * This module handles the full lifecycle of usage report collection:
 * 1. Create a report for a given date range
 * 2. Poll until the report is completed
 * 3. Download the CSV via tenant-api-proxy (same path as the browser UI)
 * 4. Parse CSV and insert into Supabase custom_usage_reports
 * 5. Trigger augmentation RPC
 *
 * Runs inside the Cloudflare sync worker which has enough memory/CPU
 * for the large CSV files (~170k rows). Supabase Edge Functions cannot
 * handle this due to ~150MB memory limits.
 *
 * State is persisted in KV so the workflow survives across cron invocations:
 *   config:usage_report_id   — report ID currently being processed
 *   config:usage_report_step — "poll" | "download" (create is implicit)
 *   config:usage_month_key   — "YYYY-MM" of the report being collected
 */

import type { Env } from '../types';

const SIMSY_TENANT_ID = '4d6e6a34-6caa-4d36-a31c-0f525d6ef56c';
const BATCH_INSERT_SIZE = 100;
const POLL_INTERVAL_MS = 30_000;
const MAX_POLL_ATTEMPTS = 8; // 8 × 30s = 4 minutes max poll time per cron invocation

interface CollectResult {
  status: 'created' | 'pending' | 'completed' | 'skipped' | 'error';
  reportId?: string;
  records?: number;
  downloadSessionId?: string;
  durationMs?: number;
  message?: string;
}

/**
 * Call the tenant-api-proxy Edge Function (same as browser does).
 * Uses fetch() with the Supabase Functions invoke URL.
 */
async function callProxy(
  env: Env,
  body: Record<string, unknown>
): Promise<Record<string, unknown>> {
  const url = `${env.SUPABASE_URL}/functions/v1/tenant-api-proxy`;

  const resp = await fetch(url, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${env.SUPABASE_SERVICE_KEY}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Proxy HTTP ${resp.status}: ${text.substring(0, 200)}`);
  }

  return await resp.json() as Record<string, unknown>;
}

/**
 * Check if data for a given month already exists in Supabase.
 * Uses a timestamp range query to find any records in the target month.
 */
async function monthDataExists(env: Env, monthKey: string): Promise<boolean> {
  // Parse monthKey "YYYY-MM" to get range
  const [yearStr, monthStr] = monthKey.split('-');
  const year = parseInt(yearStr, 10);
  const month = parseInt(monthStr, 10);
  const firstDay = `${monthKey}-01T00:00:00`;
  const nextMonth = month === 12
    ? `${year + 1}-01-01T00:00:00`
    : `${year}-${String(month + 1).padStart(2, '0')}-01T00:00:00`;

  const url = `${env.SUPABASE_URL}/rest/v1/custom_usage_reports?select=id&timestamp=gte.${firstDay}&timestamp=lt.${nextMonth}&limit=1`;
  const resp = await fetch(url, {
    headers: {
      'apikey': env.SUPABASE_SERVICE_KEY,
      'Authorization': `Bearer ${env.SUPABASE_SERVICE_KEY}`,
    },
  });

  if (!resp.ok) return false;
  const rows = await resp.json() as unknown[];
  console.log(`[BACKFILL] monthDataExists(${monthKey}): ${rows.length} rows found`);
  return rows.length > 0;
}

/**
 * Create a new usage report for the previous month.
 */
async function createReport(
  env: Env,
  reportStart: string,
  reportEnd: string
): Promise<{ reportId: string }> {
  console.log(`[COLLECT] Creating report for ${reportStart} to ${reportEnd}`);

  const data = await callProxy(env, {
    endpoint: `/api/v1/tenants/${SIMSY_TENANT_ID}/usage/reports`,
    method: 'POST',
    data: {
      reportingValue: SIMSY_TENANT_ID,
      reportingType: 'tenant',
      exportType: 'csv',
      reportStart,
      reportEnd,
      description: 'Auto-collection',
    },
  });

  console.log('[COLLECT] Create response:', JSON.stringify(data).substring(0, 200));

  // Extract report ID (same normalisation as usage-collector-server.mjs)
  let reportId: string | null = null;
  const d = data as Record<string, unknown>;
  const dd = d.data as Record<string, unknown> | string | undefined;

  if (typeof dd === 'string') reportId = dd;
  else if (dd && typeof dd === 'object') {
    const ddd = (dd as Record<string, unknown>).data;
    if (typeof ddd === 'string') reportId = ddd;
    else if (dd.id && typeof dd.id === 'string') reportId = dd.id as string;
  }

  if (!reportId) throw new Error('Could not extract report ID from create response');

  console.log(`[COLLECT] Report ID: ${reportId}`);
  return { reportId };
}

/**
 * Fetch all reports from the S-IMSY API.
 * Returns the normalised array of report objects.
 */
async function fetchAllReports(env: Env): Promise<Array<Record<string, unknown>>> {
  const data = await callProxy(env, {
    endpoint: '/api/v1/usage/reports',
    method: 'GET',
    queryParams: { offset: 0, limit: 0, sortby: '', filter: '' },
  });

  // Normalise proxy response (same as usage-collector-server.mjs)
  let reports: Array<Record<string, unknown>> = [];
  const d = data as Record<string, unknown>;
  if (Array.isArray(d.data)) {
    reports = d.data;
  } else if (d.data && typeof d.data === 'object') {
    const nested = (d.data as Record<string, unknown>).data;
    if (Array.isArray(nested)) reports = nested;
  } else if (Array.isArray(d)) {
    reports = d;
  }

  return reports;
}

/**
 * Poll the report list to check if our report is completed.
 * Returns the status moniker.
 */
async function checkReportStatus(
  env: Env,
  reportId: string
): Promise<'completed' | 'pending' | 'not_found'> {
  const reports = await fetchAllReports(env);

  const our = reports.find((r: Record<string, unknown>) => r.id === reportId);
  if (!our) return 'not_found';

  const statusType = our.reportStatusType as Record<string, unknown> | undefined;
  const moniker = statusType?.moniker || our.status || '';

  return moniker === 'completed' ? 'completed' : 'pending';
}

/**
 * Find an existing completed report that covers the target month.
 * Searches through all reports for one whose date range matches the target YYYY-MM.
 * Returns the report ID if found, null otherwise.
 */
async function findExistingReport(
  env: Env,
  year: number,
  month: number
): Promise<string | null> {
  const reports = await fetchAllReports(env);
  const targetPrefix = `${year}-${String(month).padStart(2, '0')}`;

  console.log(`[BACKFILL] Searching ${reports.length} reports for month ${targetPrefix}...`);

  for (const report of reports) {
    const statusType = report.reportStatusType as Record<string, unknown> | undefined;
    const moniker = (statusType?.moniker || report.status || '') as string;

    if (moniker !== 'completed') continue;

    // Check reportStart to see if it matches our target month
    const reportStart = (report.reportStart || report.report_start || '') as string;
    if (reportStart.startsWith(targetPrefix)) {
      const reportId = report.id as string;
      console.log(`[BACKFILL] Found existing completed report ${reportId} for ${targetPrefix} (start: ${reportStart})`);
      return reportId;
    }
  }

  console.log(`[BACKFILL] No existing completed report found for ${targetPrefix}`);
  return null;
}

/**
 * Download the completed report CSV and insert into Supabase.
 */
async function downloadAndStore(
  env: Env,
  reportId: string,
  monthKey: string
): Promise<CollectResult> {
  const start = Date.now();
  console.log(`[COLLECT] Downloading CSV for report ${reportId}...`);

  const data = await callProxy(env, {
    endpoint: `/api/v1/usage/reports/${reportId}`,
    method: 'GET',
  });

  const d = data as Record<string, unknown>;
  if (!d.success) {
    throw new Error(`Download failed: ${JSON.stringify(data).substring(0, 200)}`);
  }

  const csvData = d.data;
  if (typeof csvData !== 'string') {
    throw new Error(`Response was not CSV text (type: ${typeof csvData})`);
  }

  console.log(`[COLLECT] CSV downloaded: ${csvData.length} chars`);

  const downloadSessionId = `auto_${monthKey}_${Date.now()}`;
  const sourceEndpoint = `/api/v1/usage/reports/${reportId}`;

  const lines = csvData.split('\n').filter((l: string) => l.trim());
  if (lines.length < 2) {
    return { status: 'completed', reportId, records: 0, message: 'Empty CSV' };
  }

  const headers = parseCsvLine(lines[0]);
  const dataLines = lines.slice(1);
  console.log(`[COLLECT] ${headers.length} columns, ${dataLines.length} rows`);

  // Insert in batches via Supabase REST API
  let totalInserted = 0;
  let firstError = '';
  let errorCount = 0;

  for (let i = 0; i < dataLines.length; i += BATCH_INSERT_SIZE) {
    const batch = dataLines.slice(i, i + BATCH_INSERT_SIZE);
    const records = batch.map((line: string) => {
      // Proper CSV parsing: handle quoted fields that may contain commas
      const values = parseCsvLine(line);
      const record: Record<string, unknown> = {
        report_id: reportId,
        download_session_id: downloadSessionId,
        source_api_endpoint: sourceEndpoint,
      };

      headers.forEach((header: string, idx: number) => {
        const value = values[idx] || '';
        mapCsvColumn(record, header, value);
      });

      return record;
    });

    // Insert via Supabase REST
    const resp = await fetch(`${env.SUPABASE_URL}/rest/v1/custom_usage_reports`, {
      method: 'POST',
      headers: {
        'apikey': env.SUPABASE_SERVICE_KEY,
        'Authorization': `Bearer ${env.SUPABASE_SERVICE_KEY}`,
        'Content-Type': 'application/json',
        'Prefer': 'return=minimal,resolution=merge-duplicates',
      },
      body: JSON.stringify(records),
    });

    if (!resp.ok) {
      const errText = await resp.text();
      errorCount++;
      if (!firstError) firstError = `Batch ${i} (${resp.status}): ${errText.substring(0, 300)}`;
      console.error(`[COLLECT] Batch ${i} insert error (${resp.status}): ${errText.substring(0, 200)}`);
    } else {
      totalInserted += records.length;
    }

    if (totalInserted % 5000 < BATCH_INSERT_SIZE || i + BATCH_INSERT_SIZE >= dataLines.length) {
      console.log(`[COLLECT] Progress: ${totalInserted}/${dataLines.length}`);
    }
  }

  // Trigger augmentation RPC
  if (totalInserted > 0) {
    console.log('[COLLECT] Running augmentation RPC...');
    try {
      const rpcResp = await fetch(
        `${env.SUPABASE_URL}/rest/v1/rpc/augment_custom_usage_with_bundle_instances_fixed`,
        {
          method: 'POST',
          headers: {
            'apikey': env.SUPABASE_SERVICE_KEY,
            'Authorization': `Bearer ${env.SUPABASE_SERVICE_KEY}`,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            p_session_id: downloadSessionId,
            p_from: null,
            p_to: null,
          }),
        }
      );

      if (rpcResp.ok) {
        const rpcData = await rpcResp.json() as Array<{ updated_rows?: number }>;
        const updated = rpcData?.[0]?.updated_rows || 0;
        console.log(`[COLLECT] Augmented: ${updated} records`);
      } else {
        const errText = await rpcResp.text();
        console.error(`[COLLECT] Augmentation RPC failed: ${errText.substring(0, 200)}`);
      }
    } catch (e) {
      console.error(`[COLLECT] Augmentation error: ${e instanceof Error ? e.message : String(e)}`);
    }
  }

  const dur = Date.now() - start;
  console.log(`[COLLECT] Done: ${totalInserted} records in ${(dur / 1000).toFixed(1)}s (${errorCount} batch errors)`);

  return {
    status: 'completed',
    reportId,
    records: totalInserted,
    downloadSessionId,
    durationMs: dur,
    message: errorCount > 0 ? `${errorCount} batch errors. First: ${firstError}` : undefined,
  };
}

/**
 * Parse a single CSV line, properly handling quoted fields that may contain commas.
 */
function parseCsvLine(line: string): string[] {
  const result: string[] = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        if (i + 1 < line.length && line[i + 1] === '"') {
          // Escaped quote
          current += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        current += ch;
      }
    } else {
      if (ch === '"') {
        inQuotes = true;
      } else if (ch === ',') {
        result.push(current.trim());
        current = '';
      } else {
        current += ch;
      }
    }
  }
  result.push(current.trim());
  return result;
}

/**
 * CSV column mapping — matches the frontend's useCustomUsageCollection.ts
 */
function mapCsvColumn(record: Record<string, unknown>, header: string, value: string): void {
  switch (header) {
    case 'Timestamp': record.timestamp = value; break;
    case 'Endpoint Description': record.endpoint_description = value; break;
    case 'Endpoint Reference': record.endpoint_reference = value; break;
    case 'IMSI': record.imsi = value; break;
    case 'MSISDN': record.msisdn = value; break;
    case 'ICCID': record.iccid = value; break;
    case 'IMEI': record.imei = value; break;
    case 'RAT Type': record.rat_type_moniker = value; break;
    case 'Service Type': record.service_type = value; break;
    case 'Charge Type': record.charge_type = value; break;
    case 'Buy Rated Zone Moniker': record.buy_rating_zone_moniker = value; break;
    case 'Buy Currency': record.buy_rating_currency = value; break;
    case 'Buy Rate': record.buy_rating_rate = parseFloat(value) || 0; break;
    case 'Buy Charge': record.buy_rating_charge = parseFloat(value) || 0; break;
    case 'Sell Currency': record.sell_rating_currency = value; break;
    case 'Sell Rate': record.sell_rating_rate = parseFloat(value) || 0; break;
    case 'Sell Charge': record.sell_rating_charge = parseFloat(value) || 0; break;
    case 'Consumer Type': record.consumer_type = value; break;
    case 'Consumer Moniker': record.consumer_moniker = value; break;
    case 'Consumer Id': record.consumer_id = value; break;
    case 'Bundle Moniker': record.bundle_moniker = value; break;
    case 'Bundle Id': record.bundle_id = value; break;
    case 'Bundle Instance Id': record.bundle_instance_id = value; break;
    case 'Consumption': record.consumption = parseFloat(value) || 0; break;
    case 'Charged Consumption': record.charged_consumption = parseFloat(value) || 0; break;
    case 'Uplink Bytes': record.uplink_bytes = parseFloat(value) || 0; break;
    case 'Downlink Bytes': record.downlink_bytes = parseFloat(value) || 0; break;
    case 'Calling Party': record.calling_party = value; break;
    case 'Called Party': record.called_party = value; break;
    case 'Serving Operator': record.serving_operator_name = value; break;
    case 'Serving TADIG': record.serving_operator_tadig = value; break;
    case 'Serving MCC/MNC': record.serving_operator_mcc_mnc = value; break;
    case 'Host Operator Name': record.host_operator_name = value; break;
    case 'Host Operator': record.host_operator = value; break;
    case 'Session Id': record.session_id = value; break;
  }
}

/**
 * List all reports from the S-IMSY API.
 * Useful for inspecting report IDs and statuses.
 */
export async function listReports(env: Env): Promise<Array<Record<string, unknown>>> {
  const reports = await fetchAllReports(env);
  // Return a simplified view with key fields
  return reports.map((r) => {
    const statusType = r.reportStatusType as Record<string, unknown> | undefined;
    return {
      id: r.id,
      status: statusType?.moniker || r.status || 'unknown',
      reportStart: r.reportStart || r.report_start || '',
      reportEnd: r.reportEnd || r.report_end || '',
      description: r.description || '',
      createdAt: r.createdAt || r.created_at || '',
    };
  });
}

/**
 * Main entry point — called from the sync worker's scheduled handler.
 *
 * State machine across cron invocations:
 *   No state → check if previous month data exists → create report → save state
 *   State: poll → poll status → if completed, download → clear state
 *   State: download → download CSV → clear state
 */
export async function collectUsageReport(env: Env): Promise<CollectResult> {
  const start = Date.now();

  try {
    // Read persisted state from KV
    const savedReportId = await env.SYNC_KV.get('config:usage_report_id');
    const savedStep = await env.SYNC_KV.get('config:usage_report_step');
    const savedMonthKey = await env.SYNC_KV.get('config:usage_month_key');

    // If we have a report in progress, continue from where we left off
    if (savedReportId && savedStep) {
      console.log(`[COLLECT] Resuming: reportId=${savedReportId}, step=${savedStep}, month=${savedMonthKey}`);

      if (savedStep === 'poll') {
        // Poll until completed or timeout
        for (let attempt = 0; attempt < MAX_POLL_ATTEMPTS; attempt++) {
          const status = await checkReportStatus(env, savedReportId);
          console.log(`[COLLECT] Poll attempt ${attempt + 1}/${MAX_POLL_ATTEMPTS}: ${status}`);

          if (status === 'completed') {
            // Update state to download, then download
            await env.SYNC_KV.put('config:usage_report_step', 'download');
            const result = await downloadAndStore(env, savedReportId, savedMonthKey || 'unknown');
            // Clear state on success
            await clearState(env);
            return result;
          }

          if (status === 'not_found') {
            console.error(`[COLLECT] Report ${savedReportId} not found — clearing state`);
            await clearState(env);
            return { status: 'error', reportId: savedReportId, message: 'Report not found' };
          }

          // Wait before next poll (except on last attempt)
          if (attempt < MAX_POLL_ATTEMPTS - 1) {
            await sleep(POLL_INTERVAL_MS);
          }
        }

        // Still pending after max attempts — leave state for next cron
        console.log(`[COLLECT] Report still pending after ${MAX_POLL_ATTEMPTS} attempts. Will retry next cron.`);
        return { status: 'pending', reportId: savedReportId };
      }

      if (savedStep === 'download') {
        const result = await downloadAndStore(env, savedReportId, savedMonthKey || 'unknown');
        await clearState(env);
        return result;
      }
    }

    // No report in progress — check if we need to collect the previous month
    const now = new Date();
    const prevMonth = new Date(now.getFullYear(), now.getMonth() - 1, 1);
    const monthKey = `${prevMonth.getFullYear()}-${String(prevMonth.getMonth() + 1).padStart(2, '0')}`;

    // Check if data for this month already exists
    const exists = await monthDataExists(env, monthKey);
    if (exists) {
      console.log(`[COLLECT] Data for ${monthKey} already exists — skipping`);
      return { status: 'skipped', message: `Data for ${monthKey} already collected` };
    }

    // Calculate date range for previous month
    const lastDay = new Date(now.getFullYear(), now.getMonth(), 0);
    const reportStart = `${prevMonth.getFullYear()}-${String(prevMonth.getMonth() + 1).padStart(2, '0')}-01T00:00`;
    const reportEnd = `${lastDay.getFullYear()}-${String(lastDay.getMonth() + 1).padStart(2, '0')}-${String(lastDay.getDate()).padStart(2, '0')}T23:59`;

    // Create the report
    const { reportId } = await createReport(env, reportStart, reportEnd);

    // Save state so next cron can poll
    await env.SYNC_KV.put('config:usage_report_id', reportId);
    await env.SYNC_KV.put('config:usage_report_step', 'poll');
    await env.SYNC_KV.put('config:usage_month_key', monthKey);

    // Try polling immediately (report might complete quickly)
    await sleep(5000); // Brief wait before first poll

    for (let attempt = 0; attempt < MAX_POLL_ATTEMPTS; attempt++) {
      const status = await checkReportStatus(env, reportId);
      console.log(`[COLLECT] Initial poll ${attempt + 1}/${MAX_POLL_ATTEMPTS}: ${status}`);

      if (status === 'completed') {
        const result = await downloadAndStore(env, reportId, monthKey);
        await clearState(env);
        return result;
      }

      if (attempt < MAX_POLL_ATTEMPTS - 1) {
        await sleep(POLL_INTERVAL_MS);
      }
    }

    // Report not ready yet — state is saved, next cron will continue
    console.log(`[COLLECT] Report created but not ready. Will poll on next cron.`);
    return { status: 'created', reportId, message: 'Report generating, will poll next cron' };

  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error);
    console.error(`[COLLECT] Error: ${msg}`);
    return { status: 'error', message: msg, durationMs: Date.now() - start };
  }
}

async function clearState(env: Env): Promise<void> {
  await env.SYNC_KV.delete('config:usage_report_id');
  await env.SYNC_KV.delete('config:usage_report_step');
  await env.SYNC_KV.delete('config:usage_month_key');
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Backfill a specific month's usage data.
 * Unlike collectUsageReport, this accepts an explicit year+month
 * and skips the "previous month" logic.
 *
 * IMPORTANT: First checks for existing completed reports on the S-IMSY API
 * and downloads those. Only creates a new report if no existing one is found.
 */
export async function backfillMonth(env: Env, year: number, month: number): Promise<CollectResult> {
  const start = Date.now();
  const monthKey = `${year}-${String(month).padStart(2, '0')}`;

  try {
    // Check if data already exists in Supabase
    const exists = await monthDataExists(env, monthKey);
    if (exists) {
      console.log(`[BACKFILL] Data for ${monthKey} already exists in Supabase — skipping`);
      return { status: 'skipped', message: `Data for ${monthKey} already collected` };
    }

    // Step 1: Look for an existing completed report on the S-IMSY API
    console.log(`[BACKFILL] Looking for existing completed report for ${monthKey}...`);
    const existingReportId = await findExistingReport(env, year, month);

    if (existingReportId) {
      // Download the existing report — no need to create a new one
      console.log(`[BACKFILL] Downloading existing report ${existingReportId} for ${monthKey}`);
      const result = await downloadAndStore(env, existingReportId, monthKey);
      result.durationMs = Date.now() - start;
      return result;
    }

    // Step 2: No existing report found — create a new one
    const lastDay = new Date(year, month, 0);
    const reportStart = `${year}-${String(month).padStart(2, '0')}-01T00:00`;
    const reportEnd = `${lastDay.getFullYear()}-${String(lastDay.getMonth() + 1).padStart(2, '0')}-${String(lastDay.getDate()).padStart(2, '0')}T23:59`;

    console.log(`[BACKFILL] No existing report found. Creating new report for ${monthKey}: ${reportStart} to ${reportEnd}`);

    const { reportId } = await createReport(env, reportStart, reportEnd);

    // Poll until completed
    await sleep(5000);
    for (let attempt = 0; attempt < MAX_POLL_ATTEMPTS * 2; attempt++) {
      const status = await checkReportStatus(env, reportId);
      console.log(`[BACKFILL] Poll ${attempt + 1}: ${status}`);

      if (status === 'completed') {
        const result = await downloadAndStore(env, reportId, monthKey);
        result.durationMs = Date.now() - start;
        return result;
      }

      if (status === 'not_found') {
        return { status: 'error', reportId, message: 'Report not found', durationMs: Date.now() - start };
      }

      await sleep(POLL_INTERVAL_MS);
    }

    return { status: 'pending', reportId, message: `Report ${reportId} still generating for ${monthKey}`, durationMs: Date.now() - start };
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error);
    console.error(`[BACKFILL] Error for ${monthKey}: ${msg}`);
    return { status: 'error', message: msg, durationMs: Date.now() - start };
  }
}
