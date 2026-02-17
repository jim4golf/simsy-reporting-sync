/**
 * Tenant name resolution and data sanitisation.
 *
 * Maps Supabase tenant_name values to canonical tenant_id values
 * used in the reporting database. Also strips sensitive fields.
 */

// Canonical mapping from Supabase tenant_name → reporting tenant_id
// This maps all known variations of tenant names to our canonical IDs.
//
// Hierarchy:
//   s-imsy (Master) → allsee, cellular-lan, simsy-app, travel-simsy, trvllr (sub-tenants)
//   Each sub-tenant and s-imsy itself have their own customers.
const TENANT_NAME_MAP: Record<string, string> = {
  // S-IMSY — the master tenant (has its own customers: Eclipse, Pete Scott, etc.)
  's-imsy': 's-imsy',

  // Allsee Technologies — sub-tenant of S-IMSY
  'allsee technologies limited': 'allsee',
  'allsee technologies': 'allsee',
  'allsee': 'allsee',

  // Cellular-Lan — sub-tenant of S-IMSY
  'cellular-lan': 'cellular-lan',
  'cellularlan': 'cellular-lan',
  'cellular lan': 'cellular-lan',

  // SIMSY_application — sub-tenant of S-IMSY (separate from S-IMSY itself)
  'simsy_application': 'simsy-app',
  'simsy application': 'simsy-app',
  'simsy': 'simsy-app',

  // Travel-SIMSY — sub-tenant of S-IMSY
  'travel-simsy': 'travel-simsy',
  'travel simsy': 'travel-simsy',
  'travelsimsy': 'travel-simsy',

  // Trvllr — sub-tenant of S-IMSY
  'trvllr': 'trvllr',

  // Test / internal accounts — map to s-imsy (master)
  'dave (testing)': 's-imsy',
  'dave testing': 's-imsy',
};

/**
 * Resolve a tenant name from Supabase to a canonical tenant_id.
 * Tries tenant_name first, then falls back to tenant_id field.
 * Returns null if no mapping found (record will be skipped).
 */
export function resolveTenantId(
  tenantName: string | null | undefined,
  tenantId: string | null | undefined
): string | null {
  // Try tenant_name first (this is the augmented human-readable name)
  if (tenantName) {
    const normalised = tenantName.trim().toLowerCase();
    if (TENANT_NAME_MAP[normalised]) {
      return TENANT_NAME_MAP[normalised];
    }
  }

  // Fall back to tenant_id (the API's internal identifier)
  if (tenantId) {
    const normalised = tenantId.trim().toLowerCase();
    if (TENANT_NAME_MAP[normalised]) {
      return TENANT_NAME_MAP[normalised];
    }
    // If tenant_id is already a canonical form, check directly
    const canonical = ['s-imsy', 'allsee', 'cellular-lan', 'simsy-app', 'travel-simsy', 'trvllr'];
    if (canonical.includes(normalised)) {
      return normalised;
    }
  }

  return null;
}

/**
 * Build a composite source_id for bundle instances (dedup key).
 * Matches the unique constraint from the Supabase migration.
 */
export function buildBundleInstanceSourceId(
  bundleInstanceId: string | null,
  iccid: string | null,
  startTime: string | null
): string {
  const parts = [
    bundleInstanceId || '',
    iccid || '',
    startTime || '',
  ];
  return parts.join('|');
}

/**
 * Normalise an ICCID by stripping non-digit characters and trimming.
 * Matches the normalisation used in the Supabase augmentation functions.
 */
export function normaliseIccid(iccid: string | null | undefined): string | null {
  if (!iccid) return null;
  return iccid.trim().replace(/\D/g, '') || null;
}
