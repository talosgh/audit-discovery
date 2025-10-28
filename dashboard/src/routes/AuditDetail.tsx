import type { Component } from 'solid-js';
import { For, Match, Show, Switch, createEffect, createMemo, createResource, createSignal, onCleanup } from 'solid-js';
import { fetchAuditDetail } from '../api';
import LoadingIndicator from '../components/LoadingIndicator';
import ErrorMessage from '../components/ErrorMessage';
import type { AuditDetailResponse, Deficiency, PhotoAsset } from '../types';
import { dataUrl, formatArray, formatBoolean, formatDateTime, formatNumber } from '../utils';

interface AuditDetailProps {
  auditId: string;
  onBack(): void;
}

interface DetailField {
  label: string;
  value: string;
}

interface DetailSection {
  title: string;
  fields: DetailField[];
}

const normalizeValue = (value: unknown): string => {
  if (value === null || value === undefined) {
    return '—';
  }
  if (typeof value === 'string') {
    return value.trim().length > 0 ? value : '—';
  }
  if (typeof value === 'number') {
    return Number.isNaN(value) ? '—' : String(value);
  }
  if (typeof value === 'boolean') {
    return value ? 'Yes' : 'No';
  }
  return String(value);
};

const numberOrNull = (value: unknown): number | null => {
  if (value === null || value === undefined || value === '') return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const AuditDetail: Component<AuditDetailProps> = (props) => {
  const [auditDetail, { refetch }] = createResource<AuditDetailResponse, string>(() => props.auditId, fetchAuditDetail);

  const audit = createMemo(() => auditDetail()?.audit);
  const deficiencies = createMemo<Deficiency[]>(() => auditDetail()?.deficiencies ?? []);
  const photos = createMemo<PhotoAsset[]>(() => auditDetail()?.photos ?? []);
  const [previewIndex, setPreviewIndex] = createSignal<number | null>(null);

  const closePreview = () => setPreviewIndex(null);
  const openPreview = (index: number) => setPreviewIndex(index);
  const navigatePreview = (direction: number) => {
    const list = photos();
    if (list.length === 0) return;
    setPreviewIndex((prev) => {
      const current = prev ?? (direction > 0 ? 0 : list.length - 1);
      const next = (current + direction + list.length) % list.length;
      return next;
    });
  };

  createEffect(() => {
    if (previewIndex() === null) return;
    const handleKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        closePreview();
      } else if (event.key === 'ArrowLeft') {
        event.preventDefault();
        navigatePreview(-1);
      } else if (event.key === 'ArrowRight') {
        event.preventDefault();
        navigatePreview(1);
      }
    };
    window.addEventListener('keydown', handleKey);
    onCleanup(() => window.removeEventListener('keydown', handleKey));
  });

  const previewPhoto = createMemo<PhotoAsset | null>(() => {
    const index = previewIndex();
    if (index === null) return null;
    const list = photos();
    return list[index] ?? null;
  });

  const sections = createMemo<DetailSection[]>(() => {
    const record = audit();
    if (!record) return [];

    const doorWidth = formatNumber(numberOrNull(record.door_opening_width));

    return [
      {
        title: 'Submission',
        fields: [
          { label: 'Submitted by', value: normalizeValue(record.submitted_by ?? record.user_name) },
          { label: 'Submitted on', value: formatDateTime(record.submitted_on) },
          { label: 'Updated', value: formatDateTime(record.updated_at) },
          { label: 'Workflow stage', value: normalizeValue(record.workflow_stage) },
          { label: 'Workflow user', value: normalizeValue(record.workflow_user) },
          { label: 'Elevator contractor', value: normalizeValue(record.elevator_contractor) }
        ]
      },
      {
        title: 'Building',
        fields: [
          { label: 'Building address', value: normalizeValue(record.building_address) },
          { label: 'Building owner', value: normalizeValue(record.building_owner) },
          { label: 'Building ID', value: normalizeValue(record.building_id) },
          { label: 'City ID', value: normalizeValue(record.city_id) },
          { label: 'Bank name', value: normalizeValue(record.bank_name) },
          { label: 'Building info', value: normalizeValue(record.building_information) },
          { label: 'Device type', value: normalizeValue(record.device_type) },
          { label: 'Machine room', value: normalizeValue(record.machine_room_location) },
          { label: 'Machine room (notes)', value: normalizeValue(record.machine_room_location_other) },
          { label: 'Cars in bank', value: formatArray(record.cars_in_bank) },
          { label: 'Floors served', value: formatArray(record.floors_served) },
          { label: 'Total floor stops', value: formatArray(record.total_floor_stop_names) }
        ]
      },
      {
        title: 'Controller & Machine',
        fields: [
          { label: 'Controller manufacturer', value: normalizeValue(record.controller_manufacturer) },
          {
            label: 'Controller model',
            value: normalizeValue(
              [record.controller_model, record.controller_install_year ? `(${record.controller_install_year})` : null]
                .filter(Boolean)
                .join(' ')
            )
          },
          { label: 'Controller type', value: normalizeValue(record.controller_type) },
          { label: 'Controller install year', value: formatNumber(numberOrNull(record.controller_install_year)) },
          { label: 'Controller power', value: normalizeValue(record.controller_power_system) },
          { label: 'Machine manufacturer', value: normalizeValue(record.machine_manufacturer) },
          { label: 'Machine type', value: normalizeValue(record.machine_type) },
          { label: 'Motor type', value: normalizeValue(record.motor_type) },
          { label: 'Brake type', value: normalizeValue(record.brake_type) },
          { label: 'Single/Dual core brake', value: normalizeValue(record.single_or_dual_core_brake) },
          { label: 'Roping', value: normalizeValue(record.roping) },
          { label: 'Number of ropes', value: formatNumber(numberOrNull(record.number_of_ropes)) },
          { label: 'Car speed (fpm)', value: formatNumber(numberOrNull(record.car_speed)) },
          { label: 'Capacity (lbs)', value: formatNumber(numberOrNull(record.capacity)) },
          { label: 'Number of stops', value: formatNumber(numberOrNull(record.number_of_stops)) },
          { label: 'Rope condition score', value: formatNumber(numberOrNull(record.rope_condition_score)) }
        ]
      },
      {
        title: 'Compliance & Tags',
        fields: [
          { label: 'Maintenance log up to date', value: formatBoolean(record.maintenance_log_up_to_date) },
          { label: 'Last maintenance log date', value: normalizeValue(record.last_maintenance_log_date) },
          { label: 'Code data plate present', value: formatBoolean(record.code_data_plate_present) },
          { label: 'Code data year', value: formatNumber(numberOrNull(record.code_data_year)) },
          { label: 'Cat 1 tag current', value: formatBoolean(record.cat1_tag_current) },
          { label: 'Cat 1 tag date', value: normalizeValue(record.cat1_tag_date) },
          { label: 'Cat 5 tag current', value: formatBoolean(record.cat5_tag_current) },
          { label: 'Cat 5 tag date', value: normalizeValue(record.cat5_tag_date) },
          { label: 'Brake tag current', value: formatBoolean(record.brake_tag_current) },
          { label: 'Brake tag date', value: normalizeValue(record.brake_tag_date) },
          { label: 'DLM compliant', value: formatBoolean(record.dlm_compliant) },
          { label: 'First or only car', value: formatBoolean(record.is_first_car) }
        ]
      },
      {
        title: 'Safety & Hoistway',
        fields: [
          { label: 'Hoistway access keyswitches', value: formatBoolean(record.has_hoistway_access_keyswitches) },
          { label: 'Rope gripper present', value: formatBoolean(record.rope_gripper_present) },
          { label: 'Counterweight governor', value: formatBoolean(record.counterweight_governor) },
          { label: 'Governor manufacturer', value: normalizeValue(record.governor_manufacturer) },
          { label: 'Governor type', value: normalizeValue(record.governor_type) },
          { label: 'Sump pump present', value: formatBoolean(record.sump_pump_present) },
          { label: 'Scavenger pump present', value: formatBoolean(record.scavenger_pump_present) },
          { label: 'Safety type', value: normalizeValue(record.safety_type) },
          { label: 'Buffer type', value: normalizeValue(record.buffer_type) },
          { label: 'Compensation type', value: normalizeValue(record.compensation_type) },
          { label: 'Jack / piston type', value: normalizeValue(record.jack_piston_type) }
        ]
      },
      {
        title: 'Doors & Signage',
        fields: [
          { label: 'Door operation', value: normalizeValue(record.door_operation) },
          { label: 'Door operation type', value: normalizeValue(record.door_operation_type) },
          { label: 'Number of openings', value: formatNumber(numberOrNull(record.number_of_openings)) },
          { label: 'Door restrictor type', value: normalizeValue(record.restrictor_type) },
          { label: 'Door opening width (in)', value: doorWidth },
          { label: 'Car door equipment manufacturer', value: normalizeValue(record.car_door_equipment_manufacturer) },
          { label: 'Car door lock manufacturer', value: normalizeValue(record.car_door_lock_manufacturer) },
          { label: 'Car door operator manufacturer', value: normalizeValue(record.car_door_operator_manufacturer) },
          { label: 'Car door operator model', value: normalizeValue(record.car_door_operator_model) },
          { label: 'Hallway PI type', value: normalizeValue(record.hallway_pi_type) },
          { label: 'PI type', value: normalizeValue(record.pi_type) },
          { label: 'Rail type', value: normalizeValue(record.rail_type) },
          { label: 'Guide type', value: normalizeValue(record.guide_type) }
        ]
      },
      {
        title: 'Hydraulic & Oil',
        fields: [
          { label: 'Pump motor manufacturer', value: normalizeValue(record.pump_motor_manufacturer) },
          { label: 'Valve manufacturer', value: normalizeValue(record.valve_manufacturer) },
          { label: 'Oil condition', value: normalizeValue(record.oil_condition) },
          { label: 'Oil level', value: normalizeValue(record.oil_level) },
          { label: 'Tank heater present', value: formatBoolean(record.tank_heater_present) },
          { label: 'Oil cooler present', value: formatBoolean(record.oil_cooler_present) }
        ]
      },
      {
        title: 'Device metadata',
        fields: [
          { label: 'Mobile device', value: normalizeValue(record.mobile_device) },
          { label: 'App name', value: normalizeValue(record.mobile_app_name) },
          { label: 'App version', value: normalizeValue(record.mobile_app_version) },
          { label: 'App type', value: normalizeValue(record.mobile_app_type) },
          { label: 'SDK release', value: normalizeValue(record.mobile_sdk_release) },
          { label: 'Device memory (MB)', value: formatNumber(numberOrNull(record.mobile_memory_mb)) }
        ]
      }
    ];
  });

  return (
    <section class="page-section" aria-live="polite">
      <div class="section-header">
        <div>
          <button type="button" class="action-button back-link" onClick={props.onBack}>
            ← Back to list
          </button>
          <h1 style={{ margin: '0.75rem 0 0' }}>{audit()?.building_address ?? 'Audit detail'}</h1>
          <p class="section-subtitle">
            Explore the captured metadata, maintenance notes, deficiencies, and media attached to this elevator audit.
          </p>
        </div>
        <div class="section-actions">
          <button type="button" class="action-button refresh-button" onClick={() => refetch()}>
            Refresh
          </button>
        </div>
      </div>

      <Switch>
        <Match when={auditDetail.error}>
          <ErrorMessage message={(auditDetail.error as Error).message} onRetry={() => refetch()} />
        </Match>
        <Match when={auditDetail.loading}>
          <LoadingIndicator message="Loading audit details…" />
        </Match>
        <Match when={audit()}>
          <div class="detail-sections">
            <For each={sections()}>
              {(section) => (
                <section class="detail-card">
                  <h2>{section.title}</h2>
                  <dl class="details-grid">
                    <For each={section.fields}>
                      {(field) => (
                        <div>
                          <dt>{field.label}</dt>
                          <dd>{field.value}</dd>
                        </div>
                      )}
                    </For>
                  </dl>
                </section>
              )}
            </For>

            <section class="detail-card full-span">
              <h2>General notes</h2>
              <p>{audit()?.general_notes?.trim() || '—'}</p>
            </section>
          </div>

          <section class="page-section" style={{ background: 'rgba(15,23,42,0.6)' }} aria-labelledby="deficiency-heading">
            <h2 id="deficiency-heading">Deficiencies</h2>
            <Show when={deficiencies().length > 0} fallback={<div class="empty-state">No deficiencies recorded.</div>}>
              <div class="table-wrapper">
                <table>
                  <thead>
                    <tr>
                      <th scope="col">Equipment</th>
                      <th scope="col">Condition</th>
                      <th scope="col">Remedy</th>
                      <th scope="col">Note</th>
                    </tr>
                  </thead>
                  <tbody>
                    <For each={deficiencies()}>
                      {(item) => (
                        <tr>
                          <td>{item.violation_equipment ?? item.equipment_code ?? '—'}</td>
                          <td>{item.violation_condition ?? item.condition_code ?? '—'}</td>
                          <td>{item.violation_remedy ?? item.remedy_code ?? '—'}</td>
                          <td>{item.violation_note ?? '—'}</td>
                        </tr>
                      )}
                    </For>
                  </tbody>
                </table>
              </div>
            </Show>
          </section>

          <section class="page-section" style={{ background: 'rgba(15,23,42,0.6)' }} aria-labelledby="photos-heading">
            <h2 id="photos-heading">Photos</h2>
            <Show when={photos().length > 0} fallback={<div class="empty-state">No photos attached.</div>}>
              <div class="photos-grid">
                <For each={photos()}>
                  {(photo, index) => (
                    <figure class="photo-card">
                      <button
                        type="button"
                        class="photo-button"
                        onClick={() => openPreview(index())}
                        aria-label={`Preview ${photo.photo_filename}`}
                      >
                        <img src={dataUrl(photo.content_type, photo.photo_bytes)} alt={photo.photo_filename} loading="lazy" />
                      </button>
                      <figcaption>{photo.photo_filename}</figcaption>
                      <a href={dataUrl(photo.content_type, photo.photo_bytes)} download={photo.photo_filename}>
                        Download
                      </a>
                    </figure>
                  )}
                </For>
              </div>
            </Show>
          </section>
        </Match>
      </Switch>

      <Show when={previewPhoto()}>
        {(photo) => (
          <div class="photo-modal" role="dialog" aria-modal="true" onClick={closePreview}>
            <button type="button" class="photo-modal-nav prev" aria-label="Previous photo" onClick={(event) => { event.stopPropagation(); navigatePreview(-1); }}>
              ‹
            </button>
            <div class="photo-modal-content" onClick={(event) => event.stopPropagation()}>
              <button type="button" class="photo-modal-close" aria-label="Close" onClick={closePreview}>
                ×
              </button>
              <img src={dataUrl(photo().content_type, photo().photo_bytes)} alt={photo().photo_filename} />
              <p>{photo().photo_filename}</p>
            </div>
            <button type="button" class="photo-modal-nav next" aria-label="Next photo" onClick={(event) => { event.stopPropagation(); navigatePreview(1); }}>
              ›
            </button>
          </div>
        )}
      </Show>
    </section>
  );
};

export default AuditDetail;
