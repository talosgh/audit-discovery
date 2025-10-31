# Location Analytics & Reporting Whitepaper

## Overview

The webhook + dashboard workflow aggregates three primary data domains to surface operational
insight for each location:

1. **Audits (`audits`, `audit_deficiencies`, `audit_visits`)** – device inspection results,
   deficiency tracking, and visit metadata.
2. **Service history (`esa_in_progress`)** – technician dispatch logs with CityWide activity
   codes (`sd_cw_at`), hours, and problem descriptions.
3. **Financial records (`financial_data`)** – proposal vs. negotiated spend with explicit savings
   deltas (`delta`) and statement timestamps.

Address normalization occurs at ingestion time (Google Address Validation API) so that audits,
service logs, and financial records can be joined deterministically via `location_id`.

## Calculations & Data Products

### Service Activity Classification

`sd_cw_at` codes are mapped to categories via `service_activity_lookup`:

| Category                              | Codes                              | Description                                             |
|---------------------------------------|------------------------------------|---------------------------------------------------------|
| Preventative maintenance              | `PM`                               | Scheduled proactive maintenance                         |
| Testing – no-load                     | `TST`                              | Category 1 / fire panel testing                         |
| Testing – full-load                   | `TST-FL`                           | Category 5 / weighted tests                             |
| Callback – Entrapment                 | `CB-EMG`                           | Passenger entrapments                                   |
| Callback – Equipment failure          | `CB-EF`                            | Maintainable equipment failures                         |
| Callback – Misuse / Vandalism         | `CB-MU`                            | Improper use or intentional damage                      |
| Callback – Environmental / Utility    | `CB-ENV`, `CB-UTIL`, `CB-FP`, `CB-MISC` | External causes (weather, utilities, fire systems) |
| Repair / Return Service               | `RP`, `RS`                         | Repair crew or revisit work                             |
| Standby Support                       | `STBY`                             | On-site standby coverage                                |
| Site visit / Advisory                 | `SV`                               | Non-maintenance surveys / consultation                  |
| Unclassified                          | `NDE` or missing                   | Insufficient detail                                     |

For every location we compute:

- Tickets and hours per activity code (`activity_breakdown`).
- Aggregate share by category (`activity_summary`), used in analytics and charts.
- Preventative maintenance share used in dashboard metrics.

### Service Trends

Service trends group records by `DATE_TRUNC('month', sd_work_date)`:

- **Tickets** = count of rows.
- **Hours** = sum of `sd_hours`.

The dashboard displays the 12 most recent months, with tooltips showing both tickets and hours.
Trend direction is classified via `determine_trend_direction` (latest vs. previous month) with a
5% tolerance band:

- `up` if latest > previous by more than tolerance.
- `down` if latest < previous by more than tolerance.
- `flat` if within tolerance.
- `insufficient` if either value is zero/missing.

Forecasts extrapolate a simple linear projection `latest + (latest - previous)` when two months
are available.

### Financial Savings Analysis

Each financial record includes:

- `proposed_cost` – vendor proposal.
- `new_cost` – negotiated/approved spend.
- `delta` – savings (`proposed_cost - new_cost` when dismissed or reduced).

We aggregate per location:

- `total_spend`, `approved_spend`, `open_spend` (existing dashboard KPIs).
- `total_savings = SUM(delta)`.
- `savings_rate = total_savings / SUM(proposed_cost)`.
- `savings_per_device = total_savings / device_count`.
- Monthly and cumulative savings trends (12 most recent statements) with the same trend
  classification + forecast logic as service trends.

These metrics feed both the summary cards and analytics panel. Tooltips clarify that savings
reflect negotiated delta values.

### Deficiency Analytics

- `total_deficiencies` and `open_deficiencies` originate from `audit_deficiencies`.
- `closure_rate = (total - open) / total`.
- `open_per_device = open / device_count`.
- Deficiency trend is currently reported as *insufficient* (future work: visit-to-visit comparison).

### Performance Insights Heuristic

Devices are ranked by a composite score used in the "Performance Insights" chart:

```
score = open_deficiencies * 4
      + closed_deficiencies * 2
      + compliance_penalty (Cat1, Cat5, DLM failures) * 3
      + controller_age / 10
```

The score highlights units with persistent issues or compliance gaps. Tooltips on the chart show
the exact score per unit.

### Modernization & Vendor Advisory

The advisory module combines multiple signals to deliver actionable guidance:

- **Preventative maintenance cadence** – expected PM visits are derived from
  `device_count × (months_observed ÷ 3)`. Locations fall into a "needs attention" posture when
  actual PM coverage drops below 75% or when annual testing (minimum one TST per device per year)
  is not met. Messaging directs operators to stabilize PM and schedule a site audit before
  considering capital work.
- **Modernization ROI** – the environment variable `MODERNIZATION_COST_PER_DEVICE`
  (default `250000`) sets the modernization baseline. OPEX from the latest 6–12 months is
  annualised and paired with equipment-failure callbacks (`CB-EF`). A savings factor (15–50%) is
  scaled by failure intensity; if PM coverage is weak, the factor is heavily discounted. Payback is
  computed as `(cost_per_device × device_count) / expected_savings`, producing statuses of
  *insufficient*, *defer*, *monitor*, *consider*, or *plan* (<8 year payback or ≥3 failures per
  device annually).
- **Vendor posture** – proposal outcomes contribute denied, negotiated, and challenged rates, and
  audit closure velocity supplies a service-quality check. >20% denied, >15% challenged, or
  deficiency closure <65% triggers a "needs review" posture; negotiated rates above 40% prompt a
  "monitor" recommendation for potential overbilling.
- Advisory JSON includes ratios, expected vs. actual counts, annualised spend, and synthesized
  messaging so the dashboard can render human-readable guidance backed by consistent math.

### Location List Risk Badges

The locations landing grid reuses the same input metrics to flag "critical" or "warning" properties.
The risk score ranks locations by:

- Open deficiencies per device (>=1.5 critical, >=0.75 warning).
- 12-month equipment-failure callbacks (`CB-EF`, `CB-EMG`) [>=6 critical, >=3 warning].
- Open spend exposure (`financial_data.status ILIKE 'Open%'`) [>=50k critical, >=20k warning].
- Preventative/testing gaps (less than 4 PMs or 1 test per device/year adds a penalty).

The "Highest Risk" sort orders by this score so dangerous locations bubble to the top of pagination.

### Coverage Indicators

Location list data dots use backend booleans:

- `has_audits` – any audit rows with `location_id`.
- `has_service_records` – any `esa_in_progress` rows matched by location.
- `has_financial_records` – any `financial_data` rows matched by location ID.

The detail page also surfaces coverage in the analytics overview (`available`, `missing`).

### Address Validation Dependency

Service and financial linkage relies on successful address normalization. When no normalized match
exists, coverage markers remain "missing" and summary cards render as inactive.

## Dashboard Tooltips & UX Notes

- **Summary cards** include `title` attributes explaining each KPI (e.g., "Open deficiencies per
  device" or "Total savings (delta)").
- **Trend rows** show month + value in plain English when hovered.
- **Analytics tiles** highlight whether the underlying data exists; inactive tiles fade to 60%
  opacity but still retain tooltips.
- **Service activity tables** surface both category-level share and raw codes so dispatch teams
  can drill into specific callbacks (e.g., frequent `CB-EMG` entrapped callbacks).
- **Financial savings section** visualizes both monthly savings and cumulative total for executive
  reporting, reinforcing the savings rate KPI.

## Future Enhancements

- Compute deficiency trend vs. prior visit (requires visit-level deficiency snapshots).
- Incorporate predictive modeling (e.g., exponential smoothing) for service tickets & savings.
- Enrich analytics with MTBF / MTTR once failure timestamps are captured consistently.

This whitepaper should help stakeholders understand how each visualization is computed and why the
supporting tooltips and metrics readouts are trustworthy.
