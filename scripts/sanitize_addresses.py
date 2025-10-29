#!/usr/bin/env python3
"""
Database address sanitization utility.

This script normalizes addresses in the Citywide database using the Google
Address Validation API, then reconciles foreign-key style relationships
between locations, service records (esa_in_progress), and audits.

Usage:
    python scripts/sanitize_addresses.py [--dry-run] [--limit N]

Environment:
    DATABASE_URL        - Postgres connection string (required)
    GOOGLE_API_KEY      - Google Address Validation API key (required)
    GOOGLE_REGION_CODE  - Optional region (default: US)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import asdict, dataclass
from typing import Dict, Iterable, Optional, Tuple

import psycopg
import requests
import random


# -----------------------------------------------------------------------------
# Data structures
# -----------------------------------------------------------------------------

@dataclass
class NormalizedAddress:
    primary_line: str
    formatted_address: str
    city: str
    state: str
    postal_code: str
    postal_code_suffix: Optional[str]
    country: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    plus_code: Optional[str]
    place_id: Optional[str]

    @property
    def key(self) -> Tuple[str, str, str, str]:
        return (
            normalize_component(self.primary_line),
            normalize_component(self.city),
            normalize_component(self.state),
            (self.postal_code or "").strip()[:5],
        )


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def normalize_component(value: Optional[str]) -> str:
    return value.strip().upper() if value else ""


def make_location_key(
    line: Optional[str], city: Optional[str], state: Optional[str], postal: Optional[str]
) -> Tuple[str, str, str, str]:
    return (
        normalize_component(line),
        normalize_component(city),
        normalize_component(state),
        (postal or "").strip()[:5],
    )


# -----------------------------------------------------------------------------
# Google Address Validation client
# -----------------------------------------------------------------------------

class GoogleValidator:
    ENDPOINT = "https://addressvalidation.googleapis.com/v1:validateAddress"

    def __init__(self, api_key: str, region: str = "US", cache_path: Optional[str] = None):
        self.api_key = api_key
        self.region = region
        self.cache_path = cache_path
        self.cache: Dict[str, dict] = {}
        self._dirty = False
        self.session = requests.Session()
        self._backoff = 0.5
        if cache_path and os.path.exists(cache_path):
            with open(cache_path, "r", encoding="utf-8") as fh:
                try:
                    raw = json.load(fh)
                    if isinstance(raw, dict):
                        self.cache = raw
                except json.JSONDecodeError:
                    pass

    def save_cache(self) -> None:
        if not self.cache_path or not self._dirty:
            return
        tmp_path = f"{self.cache_path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(self.cache, fh, indent=2, sort_keys=True)
        os.replace(tmp_path, self.cache_path)
        self._dirty = False

    def validate(self, raw_address: str) -> Optional[NormalizedAddress]:
        key = raw_address.strip()
        if not key:
            return None

        cached = self.cache.get(key)
        if cached is not None:
            if cached == "__ERROR__":
                return None
            return NormalizedAddress(**cached)

        payload = {
            "address": {
                "regionCode": self.region,
                "languageCode": "en",
                "addressLines": [raw_address],
            },
            "enableUspsCass": True,
        }
        params = {"key": self.api_key}

        resp = None
        for attempt in range(5):
            try:
                resp = self.session.post(self.ENDPOINT, params=params, json=payload, timeout=20)
            except requests.RequestException as exc:
                wait = min(self._backoff * (2 ** attempt), 15)
                print(f"[validator] HTTP error for '{raw_address}': {exc} (retry in {wait:.1f}s)", file=sys.stderr)
                time.sleep(wait)
                continue

            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                wait = min(self._backoff * (2 ** attempt), 15)
                print(
                    f"[validator] HTTP {resp.status_code} for '{raw_address}', retry in {wait:.1f}s",
                    file=sys.stderr,
                )
                time.sleep(wait + random.uniform(0, 0.5))
                continue
            break
        else:
            self.cache[key] = "__ERROR__"
            self._dirty = True
            return None

        if resp.status_code != 200:
            print(
                f"[validator] Non-200 ({resp.status_code}) for '{raw_address}': {resp.text}",
                file=sys.stderr,
            )
            self.cache[key] = "__ERROR__"
            self._dirty = True
            return None

        try:
            data = resp.json()
        except json.JSONDecodeError:
            print(f"[validator] JSON decode failure for '{raw_address}'", file=sys.stderr)
            self.cache[key] = "__ERROR__"
            self._dirty = True
            return None

        result = data.get("result", {})
        address = result.get("address", {})
        postal = address.get("postalAddress", {}) or {}
        geocode = result.get("geocode", {}) or {}

        normalized = NormalizedAddress(
            primary_line=_extract_primary_line(address, postal),
            formatted_address=address.get("formattedAddress") or "",
            city=_choose(
                postal.get("locality"),
                address.get("addressComponents", []),
                component_type="locality",
            ),
            state=_choose(
                postal.get("administrativeArea"),
                address.get("addressComponents", []),
                component_type="administrative_area_level_1",
            ),
            postal_code=postal.get("postalCode") or "",
            postal_code_suffix=_extract_postal_suffix(postal),
            country=postal.get("regionCode") or _choose(
                None,
                address.get("addressComponents", []),
                component_type="country",
            ),
            latitude=_extract_float(geocode, "latitude"),
            longitude=_extract_float(geocode, "longitude"),
            plus_code=(geocode.get("plusCode") or {}).get("globalCode"),
            place_id=geocode.get("placeId"),
        )

        self.cache[key] = asdict(normalized)
        self._dirty = True
        time.sleep(0.05)
        return normalized


def _extract_primary_line(address: dict, postal: dict) -> str:
    try:
        components = address.get("addressComponents", [])
        street_number = None
        route = None
        for comp in components:
            ctype = comp.get("componentType")
            text = (comp.get("componentName") or {}).get("text")
            if ctype == "street_number" and text:
                street_number = text
            elif ctype == "route" and text:
                route = text
        if street_number and route:
            return f"{street_number} {route}".strip()
    except Exception:
        pass

    lines = postal.get("addressLines") or []
    if lines:
        return lines[0]
    return address.get("formattedAddress") or ""


def _choose(default_value, components, *, component_type):
    if default_value:
        return default_value
    try:
        for comp in components:
            if comp.get("componentType") == component_type:
                text = (comp.get("componentName") or {}).get("text")
                if text:
                    return text
    except Exception:
        pass
    return default_value or ""


def _extract_postal_suffix(postal: dict) -> Optional[str]:
    extension = postal.get("postalCodeExtension")
    if extension:
        return extension
    zip_code = postal.get("postalCode")
    if zip_code and "-" in zip_code:
        return zip_code.split("-", 1)[1]
    return None


def _extract_float(obj: dict, key: str) -> Optional[float]:
    try:
        value = obj.get("location", {}).get(key)
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


# -----------------------------------------------------------------------------
# Database sanitation operations
# -----------------------------------------------------------------------------

def normalize_address_set(label: str, addresses: Iterable[str], validator: GoogleValidator) -> Dict[str, Optional[NormalizedAddress]]:
    unique: list[str] = []
    seen = set()
    for addr in addresses:
        if not addr:
            continue
        norm = addr.strip()
        if not norm or norm in seen:
            continue
        seen.add(norm)
        unique.append(norm)

    total = len(unique)
    results: Dict[str, Optional[NormalizedAddress]] = {}
    if not total:
        return results

    print(f"[validator:{label}] normalizing {total} unique addresses...")
    for idx, raw in enumerate(unique, 1):
        results[raw] = validator.validate(raw)
        if idx % 50 == 0 or idx == total:
            print(f"[validator:{label}] {idx}/{total}", flush=True)
    return results


def sanitize_locations(conn, validator: GoogleValidator, limit: Optional[int], dry_run: bool) -> Dict[Tuple[str, str, str, str], dict]:
    print("[locations] fetching records...")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, location_id, street, city, state, zip_code, site_name
            FROM locations
            ORDER BY id
            """
        )
        rows = cur.fetchall()

    address_rows = []
    raw_addresses = []
    updates = []
    location_index: Dict[Tuple[str, str, str, str], dict] = {}
    processed = 0
    for row in rows:
        if limit and processed >= limit:
            break
        processed += 1

        row_id, location_code, street, city, state, zip_code, site_name = row
        raw_address_parts = [street or "", city or "", state or "", zip_code or ""]
        raw_address = ", ".join(part for part in raw_address_parts if part).strip()
        if not raw_address:
            # fall back to site name if street is missing
            if site_name:
                raw_address = site_name
            else:
                continue

        address_rows.append((row_id, location_code, raw_address))
        raw_addresses.append(raw_address)

    normalized_map = normalize_address_set("locations", raw_addresses, validator)

    for row_id, location_code, raw_address in address_rows:
        normalized = normalized_map.get(raw_address.strip())
        if not normalized:
            print(f"[locations] validation failed for id={row_id} ({raw_address})", file=sys.stderr)
            continue

        updates.append(
            (
                normalized.primary_line,
                normalized.city,
                normalized.state,
                normalized.postal_code,
                row_id,
            )
        )

        key = normalized.key
        entry = {
            "id": row_id,
            "location_id": location_code,
            "street": normalized.primary_line,
            "city": normalized.city,
            "state": normalized.state,
            "postal": normalized.postal_code,
        }
        location_index[key] = entry
        location_index[(key[0], key[1], key[2], "")] = entry

    if not dry_run and updates:
        print(f"[locations] updating {len(updates)} rows...")
        with conn.cursor() as cur:
            cur.executemany(
                """
                UPDATE locations
                SET street = %s,
                    city = %s,
                    state = %s,
                    zip_code = %s
                WHERE id = %s
                """,
                updates,
            )
        conn.commit()
    else:
        print("[locations] dry-run mode, no updates applied.")

    return location_index


def sanitize_service_records(
    conn,
    validator: GoogleValidator,
    location_index: Dict[Tuple[str, str, str, str], dict],
    limit: Optional[int],
    dry_run: bool,
) -> None:
    print("[service] fetching esa_in_progress records...")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT sd_record_id, sd_location_id, sd_normalized_street,
                   sd_normalized_city, sd_normalized_state, sd_normalized_zip_code
            FROM esa_in_progress
            ORDER BY sd_record_id
            """
        )
        rows = cur.fetchall()

    updates = []
    processed = 0
    matched = 0
    address_rows = []
    raw_addresses = []
    for row in rows:
        if limit and processed >= limit:
            break
        processed += 1

        record_id, current_location_id, street, city, state, zip_code = row
        raw_address_parts = [street or "", city or "", state or "", zip_code or ""]
        raw_address = ", ".join(part for part in raw_address_parts if part).strip()
        if not raw_address:
            continue

        address_rows.append((record_id, current_location_id, raw_address))
        raw_addresses.append(raw_address)

    normalized_map = normalize_address_set("service", raw_addresses, validator)

    for record_id, current_location_id, raw_address in address_rows:
        normalized = normalized_map.get(raw_address.strip())
        if not normalized:
            print(f"[service] validation failed for record {record_id}", file=sys.stderr)
            continue

        new_location_id = current_location_id
        key = normalized.key
        match = location_index.get(key) or location_index.get((key[0], key[1], key[2], ""))
        if match:
            matched += 1
            new_location_id = match["location_id"]

        updates.append(
            (
                normalized.primary_line,
                normalized.city,
                normalized.state,
                normalized.postal_code,
                new_location_id,
                record_id,
            )
        )

    if not dry_run and updates:
        print(f"[service] updating {len(updates)} rows ({matched} matched to locations)...")
        with conn.cursor() as cur:
            cur.executemany(
                """
                UPDATE esa_in_progress
                SET sd_normalized_street = %s,
                    sd_normalized_city = %s,
                    sd_normalized_state = %s,
                    sd_normalized_zip_code = %s,
                    sd_location_id = %s
                WHERE sd_record_id = %s
                """,
                updates,
            )
        conn.commit()
    else:
        print("[service] dry-run mode, no updates applied.")


def sanitize_audits(
    conn,
    validator: GoogleValidator,
    location_index: Dict[Tuple[str, str, str, str], dict],
    limit: Optional[int],
    dry_run: bool,
) -> None:
    print("[audits] fetching audit records...")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT audit_uuid,
                   building_address,
                   building_city,
                   building_state,
                   building_postal_code,
                   location_id
            FROM audits
            ORDER BY audit_uuid
            """
        )
        rows = cur.fetchall()

    updates = []
    processed = 0
    matched = 0
    address_rows = []
    raw_addresses = []
    for row in rows:
        if limit and processed >= limit:
            break
        processed += 1

        (
            audit_uuid,
            building_address,
            building_city,
            building_state,
            building_postal_code,
            current_location_id,
        ) = row

        raw_address = building_address or ""
        if not raw_address.strip():
            continue

        address_rows.append((audit_uuid, current_location_id, raw_address))
        raw_addresses.append(raw_address)

    normalized_map = normalize_address_set("audits", raw_addresses, validator)

    for audit_uuid, current_location_id, raw_address in address_rows:
        normalized = normalized_map.get(raw_address.strip())
        if not normalized:
            print(f"[audits] validation failed for audit {audit_uuid}", file=sys.stderr)
            continue

        new_location_id = current_location_id
        key = normalized.key
        match = location_index.get(key) or location_index.get((key[0], key[1], key[2], ""))
        if match:
            matched += 1
            new_location_id = match["id"]

        updates.append(
            (
                normalized.primary_line,
                normalized.city,
                normalized.state,
                normalized.postal_code,
                normalized.postal_code_suffix,
                normalized.country,
                normalized.formatted_address,
                normalized.latitude,
                normalized.longitude,
                normalized.plus_code,
                normalized.place_id,
                new_location_id,
                audit_uuid,
            )
        )

    if not dry_run and updates:
        print(f"[audits] updating {len(updates)} rows ({matched} matched to locations)...")
        with conn.cursor() as cur:
            cur.executemany(
                """
                UPDATE audits
                SET building_address = %s,
                    building_city = %s,
                    building_state = %s,
                    building_postal_code = %s,
                    building_postal_code_suffix = %s,
                    building_country = %s,
                    building_formatted_address = %s,
                    building_latitude = %s,
                    building_longitude = %s,
                    building_plus_code = %s,
                    building_place_id = %s,
                    location_id = %s
                WHERE audit_uuid = %s
                """,
                updates,
            )
        conn.commit()
    else:
        print("[audits] dry-run mode, no updates applied.")


# -----------------------------------------------------------------------------
# Main entry point
# -----------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sanitize database addresses via Google Address Validation API.")
    parser.add_argument("--dry-run", action="store_true", help="Perform validation but do not update the database.")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process at most N rows per table (useful for testing).",
    )
    parser.add_argument(
        "--cache",
        default="address_cache.json",
        help="Path to JSON file for caching address validation results.",
    )
    parser.add_argument(
        "--region",
        default=os.environ.get("GOOGLE_REGION_CODE", "US"),
        help="Region code to pass to the Google validator (default: %(default)s).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    database_url = os.environ.get("DATABASE_URL") or os.environ.get("POSTGRES_DSN")
    if not database_url:
        print("DATABASE_URL (or POSTGRES_DSN) must be set", file=sys.stderr)
        return 1

    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        print("GOOGLE_API_KEY must be set", file=sys.stderr)
        return 1

    validator = GoogleValidator(api_key=api_key, region=args.region, cache_path=args.cache)

    try:
        with psycopg.connect(database_url) as conn:
            print("[main] connected to database")
            location_index = sanitize_locations(conn, validator, args.limit, args.dry_run)
            sanitize_service_records(conn, validator, location_index, args.limit, args.dry_run)
            sanitize_audits(conn, validator, location_index, args.limit, args.dry_run)
            if args.dry_run:
                print("[main] dry-run completed, rolling back...")
                conn.rollback()
    except psycopg.Error as exc:
        print(f"[main] database error: {exc}", file=sys.stderr)
        return 1
    finally:
        validator.save_cache()

    print("[main] done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
