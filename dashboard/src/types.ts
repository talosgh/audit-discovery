export interface AuditSummary {
  audit_uuid: string;
  building_address: string | null;
  building_owner: string | null;
  device_type: string | null;
  bank_name: string | null;
  city_id: string | null;
  submitted_on: string | null;
  updated_at: string | null;
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
  bank_name: string | null;
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
}

export interface Deficiency {
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
