import type { Component } from 'solid-js';
import { For, Match, Show, Switch, createMemo, createResource } from 'solid-js';
import { fetchAuditDetail } from '../api';
import LoadingIndicator from '../components/LoadingIndicator';
import ErrorMessage from '../components/ErrorMessage';
import type { AuditDetailResponse, Deficiency, PhotoAsset } from '../types';
import { dataUrl, formatArray, formatBoolean, formatDateTime, formatNumber } from '../utils';

interface AuditDetailProps {
  auditId: string;
  onBack(): void;
}

const AuditDetail: Component<AuditDetailProps> = (props) => {
  const [auditDetail, { refetch }] = createResource<AuditDetailResponse, string>(
    () => props.auditId,
    fetchAuditDetail
  );

  const audit = createMemo(() => auditDetail()?.audit);
  const deficiencies = createMemo<Deficiency[]>(() => auditDetail()?.deficiencies ?? []);
  const photos = createMemo<PhotoAsset[]>(() => auditDetail()?.photos ?? []);

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
          <dl class="details-grid" style={{ margin: '1.5rem 0' }}>
            <div>
              <dt>Building owner</dt>
              <dd>{audit()?.building_owner ?? '—'}</dd>
            </div>
            <div>
              <dt>City ID</dt>
              <dd>{audit()?.city_id ?? '—'}</dd>
            </div>
            <div>
              <dt>Device type</dt>
              <dd>{audit()?.device_type ?? '—'}</dd>
            </div>
            <div>
              <dt>Submitted by</dt>
              <dd>{audit()?.submitted_by ?? audit()?.user_name ?? '—'}</dd>
            </div>
            <div>
              <dt>Submitted on</dt>
              <dd>{formatDateTime(audit()?.submitted_on)}</dd>
            </div>
            <div>
              <dt>Workflow stage</dt>
              <dd>{audit()?.workflow_stage ?? '—'}</dd>
            </div>
            <div>
              <dt>Contractor</dt>
              <dd>{audit()?.elevator_contractor ?? '—'}</dd>
            </div>
            <div>
              <dt>Car speed</dt>
              <dd>
                {audit()?.car_speed !== null && audit()?.car_speed !== undefined
                  ? `${formatNumber(audit()?.car_speed)} fpm`
                  : '—'}
              </dd>
            </div>
            <div>
              <dt>Capacity (lbs)</dt>
              <dd>{formatNumber(audit()?.capacity)}</dd>
            </div>
            <div>
              <dt>Number of stops</dt>
              <dd>{formatNumber(audit()?.number_of_stops)}</dd>
            </div>
            <div>
              <dt>Cars in bank</dt>
              <dd>{formatArray(audit()?.cars_in_bank)}</dd>
            </div>
            <div>
              <dt>Floors served</dt>
              <dd>{formatArray(audit()?.floors_served)}</dd>
            </div>
            <div>
              <dt>Total stops (building)</dt>
              <dd>{formatArray(audit()?.total_floor_stop_names)}</dd>
            </div>
            <div>
              <dt>Machine manufacturer</dt>
              <dd>{audit()?.machine_manufacturer ?? '—'}</dd>
            </div>
            <div>
              <dt>Machine type</dt>
              <dd>{audit()?.machine_type ?? '—'}</dd>
            </div>
            <div>
              <dt>Controller</dt>
              <dd>
                {audit()?.controller_manufacturer ?? '—'} {audit()?.controller_type ? `• ${audit()?.controller_type}` : ''}
              </dd>
            </div>
            <div>
              <dt>Controller power</dt>
              <dd>{audit()?.controller_power_system ?? '—'}</dd>
            </div>
            <div>
              <dt>Hoistway keyswitches</dt>
              <dd>{formatBoolean(audit()?.has_hoistway_access_keyswitches)}</dd>
            </div>
            <div>
              <dt>Sump pump present</dt>
              <dd>{formatBoolean(audit()?.sump_pump_present)}</dd>
            </div>
            <div>
              <dt>Scavenger pump present</dt>
              <dd>{formatBoolean(audit()?.scavenger_pump_present)}</dd>
            </div>
            <div style={{ gridColumn: '1 / -1' }}>
              <dt>General notes</dt>
              <dd>{audit()?.general_notes?.trim() || '—'}</dd>
            </div>
          </dl>

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
                  {(photo) => (
                    <figure class="photo-card">
                      <img src={dataUrl(photo.content_type, photo.photo_bytes)} alt={photo.photo_filename} loading="lazy" />
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
    </section>
  );
};

export default AuditDetail;
