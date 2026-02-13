// Environment bindings available to the Worker
export interface Env {
  HYPERDRIVE: Hyperdrive;
  SYNC_KV: KVNamespace;
  SUPABASE_URL: string;
  SUPABASE_SERVICE_KEY: string;
  SYNC_BATCH_SIZE: string;
}

// Supabase source record types

export interface SupabaseUsageRecord {
  id: string;
  created_at: string | null;
  tenant_name: string | null;
  customer_name: string | null;
  endpoint_name: string | null;
  endpoint_description: string | null;
  iccid: string | null;
  timestamp: string | null;
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
  serving_operator_tadig: string | null;
  buy_rating_charge: number | null;
  buy_rating_currency: string | null;
  sell_rating_charge: number | null;
  sell_rating_currency: string | null;
  // Sensitive fields exist in source but we never read them
}

export interface SupabaseBundleRecord {
  id: string;
  bundle_id: string;
  bundle_name: string;
  bundle_moniker: string;
  status_name: string | null;
  status_moniker: string | null;
  tenant_name: string | null;
  start_time: string | null;
  end_time: string | null;
  collected_at: string | null;
}

export interface SupabaseBundleInstanceRecord {
  id: string;
  created_at: string;
  tenant_id: string | null;
  tenant_name: string | null;
  customer_id: string | null;
  customer_name: string | null;
  endpoint_name: string | null;
  iccid: string | null;
  bundle_name: string | null;
  bundle_moniker: string | null;
  bundle_instance_id: string | null;
  start_time: string | null;
  end_time: string | null;
  status_name: string | null;
  status_moniker: string | null;
  sequence: number | null;
  sequence_max: number | null;
}

export interface SupabaseEndpointRecord {
  id: string;
  endpoint_identifier: string;
  endpoint_name: string | null;
  endpoint_type: string | null;
  endpoint_type_name: string | null;
  status: string | null;
  endpoint_status_name: string | null;
  endpoint_network_status_name: string | null;
  tenant_id: string | null;
  customer_id: string | null;
  usage_rolling_24h: number | null;
  usage_rolling_7d: number | null;
  usage_rolling_28d: number | null;
  usage_rolling_1y: number | null;
  charge_rolling_24h: number | null;
  charge_rolling_7d: number | null;
  charge_rolling_28d: number | null;
  charge_rolling_1y: number | null;
  first_activity: string | null;
  latest_activity: string | null;
  created_at: string;
  updated_at: string;
}

export interface SyncResult {
  table: string;
  recordsSynced: number;
  duration: number;
  error?: string;
}
