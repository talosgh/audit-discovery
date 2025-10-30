export interface AuditSummary {
  audit_uuid: string;
  building_address: string | null;
  building_owner: string | null;
  device_type: string | null;
  bank_name: string | null;
  city_id: string | null;
  submitted_on: string | null;
  updated_at: string | null;
  deficiency_count?: number | null;
}

export interface AuditRecord extends AuditSummary {
  submitted_by: string | null;
  user_name: string | null;
  workflow_stage: string | null;
  workflow_user: string | null;
  elevator_contractor: string | null;
  building_id: string | null;
  machine_manufacturer: string | null;
  machine_type: string | null;
  controller_manufacturer: string | null;
  controller_type: string | null;
  controller_power_system: string | null;
  car_speed: number | null;
  capacity: number | null;
  number_of_stops: number | null;
  rope_condition_score: number | null;
  floors_served: string[] | null;
  cars_in_bank: string[] | null;
  total_floor_stop_names: string[] | null;
  has_hoistway_access_keyswitches: boolean | null;
  sump_pump_present: boolean | null;
  scavenger_pump_present: boolean | null;
  general_notes: string | null;
  machine_room_location: string | null;
  machine_room_location_other: string | null;
  controller_model: string | null;
  controller_install_year: number | null;
  maintenance_log_up_to_date: boolean | null;
  last_maintenance_log_date: string | null;
  code_data_plate_present: boolean | null;
  code_data_year: number | null;
  cat1_tag_current: boolean | null;
  cat1_tag_date: string | null;
  cat5_tag_current: boolean | null;
  cat5_tag_date: string | null;
  brake_tag_current: boolean | null;
  brake_tag_date: string | null;
  number_of_ropes: number | null;
  roping: string | null;
  motor_data_plate_present: boolean | null;
  motor_type: string | null;
  brake_type: string | null;
  single_or_dual_core_brake: string | null;
  rope_gripper_present: boolean | null;
  governor_manufacturer: string | null;
  governor_type: string | null;
  counterweight_governor: boolean | null;
  pump_motor_manufacturer: string | null;
  oil_condition: string | null;
  oil_level: string | null;
  valve_manufacturer: string | null;
  tank_heater_present: boolean | null;
  oil_cooler_present: boolean | null;
  door_operation: string | null;
  door_operation_type: string | null;
  number_of_openings: number | null;
  pi_type: string | null;
  rail_type: string | null;
  guide_type: string | null;
  car_door_equipment_manufacturer: string | null;
  car_door_lock_manufacturer: string | null;
  car_door_operator_manufacturer: string | null;
  car_door_operator_model: string | null;
  restrictor_type: string | null;
  hallway_pi_type: string | null;
  hatch_door_unlocking_type: string | null;
  hatch_door_equipment_manufacturer: string | null;
  hatch_door_lock_manufacturer: string | null;
  pit_access: string | null;
  safety_type: string | null;
  buffer_type: string | null;
  compensation_type: string | null;
  jack_piston_type: string | null;
  door_opening_width: number | null;
  expected_stop_count: number | null;
  mobile_device: string | null;
  mobile_app_name: string | null;
  mobile_app_version: string | null;
  mobile_app_type: string | null;
  mobile_sdk_release: string | null;
  mobile_memory_mb: number | null;
  is_first_car: boolean | null;
  dlm_compliant: boolean | null;
}

export interface Deficiency {
  id?: number;
  section_counter: number | null;
  violation_device_id: string | null;
  equipment_code: string | null;
  condition_code: string | null;
  remedy_code: string | null;
  overlay_code: string | null;
  violation_equipment: string | null;
  violation_condition: string | null;
  violation_remedy: string | null;
  violation_note: string | null;
  resolved_at?: string | null;
}

export interface PhotoAsset {
  photo_filename: string;
  content_type: string;
  photo_bytes: string;
}

export interface AuditDetailResponse {
  audit: AuditRecord;
  deficiencies: Deficiency[];
  photos: PhotoAsset[];
}

export interface LocationSummary {
  location_code: string | null;
  location_row_id: number | null;
  address: string;
  formatted_address?: string | null;
  site_name: string | null;
  street: string | null;
  city: string | null;
  state: string | null;
  zip: string | null;
  building_owner: string | null;
  vendor_name: string | null;
  device_count: number;
  open_deficiencies: number;
  has_audits?: boolean;
  has_service_records?: boolean;
  has_financial_records?: boolean;
}

export interface LocationListParams {
  page?: number;
  pageSize?: number;
  search?: string;
}

export interface LocationListResponse {
  page: number;
  page_size: number;
  total: number;
  items: LocationSummary[];
}

export interface LocationDeviceDeficiency {
  id: number;
  equipment: string | null;
  condition: string | null;
  remedy: string | null;
  note: string | null;
  condition_code: string | null;
  resolved: boolean;
  resolved_at: string | null;
}

export interface LocationDevice {
  audit_uuid: string;
  device_id: string | null;
  device_type: string | null;
  bank_name: string | null;
  city_id: string | null;
  submitted_on: string | null;
  controller_install_year: number | null;
  controller_age: number | null;
  dlm_compliant: boolean | null;
  cat1_tag_current: boolean | null;
  cat5_tag_current: boolean | null;
  maintenance_log_up_to_date: boolean | null;
  is_first_car: boolean | null;
  total_deficiencies: number;
  open_deficiencies: number;
  cars_in_bank: string[];
  floors_served: string[];
  total_floor_stop_names: string[];
  deficiencies: LocationDeviceDeficiency[];
}

export interface LocationProfileInfo {
  row_id: number | null;
  location_code: string | null;
  device_count: number | null;
  site_name: string | null;
  street: string | null;
  city: string | null;
  state: string | null;
  zip: string | null;
  address_label: string | null;
  owner: {
    name: string | null;
    id: string | null;
  };
  operator: {
    name: string | null;
    id: string | null;
  };
  vendor: {
    name: string | null;
    id: string | null;
  };
}

export interface ServiceProblem {
  problem: string | null;
  count: number;
}

export interface ServiceTrendPoint {
  month: string | null;
  spend?: number; // compatibility
  tickets?: number;
  hours?: number;
}

export interface ServiceActivityBreakdown {
  code: string | null;
  label: string | null;
  category: string | null;
  tickets: number;
  hours: number;
  description?: string | null;
}

export interface ServiceActivitySummaryItem {
  category: string | null;
  tickets: number;
  hours: number;
  share: number;
  short_label: string | null;
}

export interface ServiceVendorMix {
  vendor: string | null;
  tickets: number;
}

export interface ServiceSummary {
  total_tickets: number;
  total_hours: number;
  last_service: string | null;
  top_problems: ServiceProblem[];
  monthly_trend: ServiceTrendPoint[];
  vendor_mix: ServiceVendorMix[];
  activity_breakdown: ServiceActivityBreakdown[];
  activity_summary: ServiceActivitySummaryItem[];
}

export interface FinancialTrendPoint {
  month: string | null;
  spend: number;
}

export interface FinancialSavingsPoint {
  month: string | null;
  savings: number;
}

export interface FinancialBreakdownItem {
  category?: string | null;
  status?: string | null;
  spend: number;
}

export interface FinancialSummary {
  total_records: number;
  total_spend: number;
  approved_spend: number;
  open_spend: number;
  last_statement: string | null;
  monthly_trend: FinancialTrendPoint[];
  category_breakdown: FinancialBreakdownItem[];
  status_breakdown: FinancialBreakdownItem[];
  total_savings: number;
  savings_rate: number;
  monthly_savings: FinancialSavingsPoint[];
  cumulative_savings: FinancialSavingsPoint[];
}

export interface VisitSummary {
  visit_id: string | null;
  label: string | null;
  started_at: string | null;
  completed_at: string | null;
  audit_count: number;
  device_count: number;
  open_deficiencies: number;
}

export interface ReportVersion {
  job_id: string;
  version: number | null;
  created_at: string | null;
  completed_at: string | null;
  filename: string | null;
  size_bytes: number | null;
  download_url: string | null;
  include_all?: boolean;
  selected_count?: number;
}

export interface LocationDetail {
  summary: {
    location_code: string | null;
    location_row_id: number | null;
    site_name: string | null;
    street: string | null;
    city: string | null;
    state: string | null;
    zip: string | null;
    owner_name: string | null;
    owner_id: string | null;
    operator_name: string | null;
    operator_id: string | null;
    vendor_name: string | null;
    vendor_id: string | null;
    address: string;
    building_owner: string | null;
    elevator_contractor: string | null;
    city_id: string | null;
    device_count: number;
    audit_count: number;
    first_audit: string | null;
    last_audit: string | null;
    total_deficiencies: number;
    open_deficiencies: number;
    deficiencies_by_code: Record<string, number>;
  };
  devices: LocationDevice[];
  profile: LocationProfileInfo;
  service: ServiceSummary;
  financial: FinancialSummary;
  visits: VisitSummary[];
  reports: ReportVersion[];
  deficiency_reports: ReportVersion[];
  analytics: LocationAnalytics;
}

export type CoverageStatus = 'available' | 'partial' | 'missing';

export type TrendDirection = 'up' | 'down' | 'flat' | 'insufficient';

export interface TrendMetrics {
  direction: TrendDirection;
  percent_change: number | null;
  forecast: number | null;
}

export interface DeficiencyAnalytics {
  status: CoverageStatus;
  metrics: {
    total: number;
    open: number;
    closure_rate: number | null;
    open_per_device: number | null;
    avg_per_device: number | null;
  };
  trend: TrendMetrics;
}

export interface ServiceAnalyticsSection {
  status: CoverageStatus;
  metrics: {
    tickets: number | null;
    hours: number | null;
    per_device: number | null;
  };
  trend: TrendMetrics;
  activities: ServiceActivitySummaryItem[];
}

export interface FinancialAnalyticsSection {
  status: CoverageStatus;
  metrics: {
    total_spend: number | null;
    approved_spend: number | null;
    open_spend: number | null;
    per_device: number | null;
    savings_total: number | null;
    savings_rate: number | null;
    savings_per_device: number | null;
  };
  trend: TrendMetrics;
  savings_trend: TrendMetrics;
}

export interface LocationAnalyticsOverview {
  device_count: number;
  data_coverage: {
    deficiencies: CoverageStatus;
    service: CoverageStatus;
    financial: CoverageStatus;
  };
}

export interface LocationAnalytics {
  overview: LocationAnalyticsOverview;
  deficiencies: DeficiencyAnalytics;
  service: ServiceAnalyticsSection;
  financial: FinancialAnalyticsSection;
}

export interface ReportJobCreateRequest {
  address: string;
  locationRowId?: number | null;
  notes?: string;
  recommendations?: string;
  coverBuildingOwner?: string;
  coverStreet?: string;
  coverCity?: string;
  coverState?: string;
  coverZip?: string;
  coverContactName?: string;
  coverContactEmail?: string;
  deficiencyOnly?: boolean;
  visitIds?: string[];
  auditIds?: string[];
}

export interface ReportJobCreateResponse {
  status: string;
  job_id: string;
  address: string | null;
}

export interface ReportJobStatus {
  job_id: string;
  status: string;
  address: string | null;
  created_at: string | null;
  started_at: string | null;
  completed_at: string | null;
  error: string | null;
  download_ready: boolean;
  deficiency_only: boolean;
  include_all: boolean;
  location_id?: string | null;
  artifact_filename?: string | null;
  artifact_size?: number | null;
  version?: number | null;
  download_url?: string | null;
  selected_audit_count?: number;
}
