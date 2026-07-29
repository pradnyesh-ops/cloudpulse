"""
CloudPulse — Data Sources
Generates realistic internet-probe telemetry records.
  • Demo mode  : pure Python simulation, no external dependencies
  • RIPE Atlas  : optional public API (no key required)
Records are returned as Python dicts ready to be serialised to JSON
and pushed to Kinesis or written to local storage.
"""

from __future__ import annotations

import json
import math
import random
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Iterator

import requests

# ─── Region / Probe catalogue ─────────────────────────────────────────────────
# Each entry represents one monitoring region with (up to) multiple probes.
REGIONS: list[dict] = [
    {"id": "us-east",   "name": "US East",      "country": "US", "country_name": "United States",  "continent": "NA", "lat": 38.89,  "lon": -77.04,  "flag": "🇺🇸", "provider": "AWS"},
    {"id": "us-west",   "name": "US West",      "country": "US", "country_name": "United States",  "continent": "NA", "lat": 37.77,  "lon": -122.42, "flag": "🇺🇸", "provider": "AWS"},
    {"id": "us-central","name": "US Central",   "country": "US", "country_name": "United States",  "continent": "NA", "lat": 41.88,  "lon": -87.63,  "flag": "🇺🇸", "provider": "AWS"},
    {"id": "ca",        "name": "Canada",       "country": "CA", "country_name": "Canada",         "continent": "NA", "lat": 45.42,  "lon": -75.70,  "flag": "🇨🇦", "provider": "AWS"},
    {"id": "mx",        "name": "Mexico",       "country": "MX", "country_name": "Mexico",         "continent": "NA", "lat": 19.43,  "lon": -99.13,  "flag": "🇲🇽", "provider": "Telmex"},
    {"id": "br",        "name": "Brazil",       "country": "BR", "country_name": "Brazil",         "continent": "SA", "lat": -23.55, "lon": -46.63,  "flag": "🇧🇷", "provider": "Oi"},
    {"id": "ar",        "name": "Argentina",    "country": "AR", "country_name": "Argentina",      "continent": "SA", "lat": -34.60, "lon": -58.38,  "flag": "🇦🇷", "provider": "Telecom"},
    {"id": "co",        "name": "Colombia",     "country": "CO", "country_name": "Colombia",       "continent": "SA", "lat": 4.71,   "lon": -74.07,  "flag": "🇨🇴", "provider": "ETB"},
    {"id": "gb",        "name": "UK",           "country": "GB", "country_name": "United Kingdom", "continent": "EU", "lat": 51.51,  "lon": -0.13,   "flag": "🇬🇧", "provider": "BT"},
    {"id": "de",        "name": "Germany",      "country": "DE", "country_name": "Germany",        "continent": "EU", "lat": 52.52,  "lon": 13.40,   "flag": "🇩🇪", "provider": "Deutsche Telekom"},
    {"id": "fr",        "name": "France",       "country": "FR", "country_name": "France",         "continent": "EU", "lat": 48.85,  "lon": 2.35,    "flag": "🇫🇷", "provider": "Orange"},
    {"id": "nl",        "name": "Netherlands",  "country": "NL", "country_name": "Netherlands",    "continent": "EU", "lat": 52.37,  "lon": 4.90,    "flag": "🇳🇱", "provider": "KPN"},
    {"id": "se",        "name": "Sweden",       "country": "SE", "country_name": "Sweden",         "continent": "EU", "lat": 59.33,  "lon": 18.07,   "flag": "🇸🇪", "provider": "Telia"},
    {"id": "es",        "name": "Spain",        "country": "ES", "country_name": "Spain",          "continent": "EU", "lat": 40.42,  "lon": -3.70,   "flag": "🇪🇸", "provider": "Telefonica"},
    {"id": "it",        "name": "Italy",        "country": "IT", "country_name": "Italy",          "continent": "EU", "lat": 41.90,  "lon": 12.50,   "flag": "🇮🇹", "provider": "Telecom Italia"},
    {"id": "pl",        "name": "Poland",       "country": "PL", "country_name": "Poland",         "continent": "EU", "lat": 52.23,  "lon": 21.01,   "flag": "🇵🇱", "provider": "Orange PL"},
    {"id": "ua",        "name": "Ukraine",      "country": "UA", "country_name": "Ukraine",        "continent": "EU", "lat": 50.45,  "lon": 30.52,   "flag": "🇺🇦", "provider": "Ukrtelecom"},
    {"id": "ru",        "name": "Russia",       "country": "RU", "country_name": "Russia",         "continent": "EU", "lat": 55.75,  "lon": 37.62,   "flag": "🇷🇺", "provider": "Rostelecom"},
    {"id": "tr",        "name": "Turkey",       "country": "TR", "country_name": "Turkey",         "continent": "AS", "lat": 41.01,  "lon": 28.95,   "flag": "🇹🇷", "provider": "Turk Telekom"},
    {"id": "ae",        "name": "UAE",          "country": "AE", "country_name": "UAE",            "continent": "AS", "lat": 25.20,  "lon": 55.27,   "flag": "🇦🇪", "provider": "Etisalat"},
    {"id": "sa",        "name": "Saudi Arabia", "country": "SA", "country_name": "Saudi Arabia",   "continent": "AS", "lat": 24.69,  "lon": 46.72,   "flag": "🇸🇦", "provider": "STC"},
    {"id": "il",        "name": "Israel",       "country": "IL", "country_name": "Israel",         "continent": "AS", "lat": 32.09,  "lon": 34.79,   "flag": "🇮🇱", "provider": "Bezeq"},
    {"id": "in",        "name": "India",        "country": "IN", "country_name": "India",          "continent": "AS", "lat": 19.08,  "lon": 72.88,   "flag": "🇮🇳", "provider": "Reliance Jio"},
    {"id": "pk",        "name": "Pakistan",     "country": "PK", "country_name": "Pakistan",       "continent": "AS", "lat": 33.72,  "lon": 73.04,   "flag": "🇵🇰", "provider": "PTCL"},
    {"id": "cn",        "name": "China",        "country": "CN", "country_name": "China",          "continent": "AS", "lat": 39.91,  "lon": 116.39,  "flag": "🇨🇳", "provider": "China Unicom"},
    {"id": "jp",        "name": "Japan",        "country": "JP", "country_name": "Japan",          "continent": "AS", "lat": 35.69,  "lon": 139.69,  "flag": "🇯🇵", "provider": "NTT"},
    {"id": "kr",        "name": "South Korea",  "country": "KR", "country_name": "South Korea",    "continent": "AS", "lat": 37.57,  "lon": 126.98,  "flag": "🇰🇷", "provider": "KT Corp"},
    {"id": "sg",        "name": "Singapore",    "country": "SG", "country_name": "Singapore",      "continent": "AS", "lat": 1.35,   "lon": 103.82,  "flag": "🇸🇬", "provider": "Singtel"},
    {"id": "hk",        "name": "Hong Kong",    "country": "HK", "country_name": "Hong Kong",      "continent": "AS", "lat": 22.32,  "lon": 114.17,  "flag": "🇭🇰", "provider": "HKT"},
    {"id": "th",        "name": "Thailand",     "country": "TH", "country_name": "Thailand",       "continent": "AS", "lat": 13.75,  "lon": 100.52,  "flag": "🇹🇭", "provider": "CAT Telecom"},
    {"id": "id",        "name": "Indonesia",    "country": "ID", "country_name": "Indonesia",      "continent": "AS", "lat": -6.21,  "lon": 106.85,  "flag": "🇮🇩", "provider": "Telkom"},
    {"id": "ph",        "name": "Philippines",  "country": "PH", "country_name": "Philippines",    "continent": "AS", "lat": 14.60,  "lon": 120.98,  "flag": "🇵🇭", "provider": "PLDT"},
    {"id": "vn",        "name": "Vietnam",      "country": "VN", "country_name": "Vietnam",        "continent": "AS", "lat": 21.03,  "lon": 105.85,  "flag": "🇻🇳", "provider": "VNPT"},
    {"id": "au",        "name": "Australia",    "country": "AU", "country_name": "Australia",      "continent": "OC", "lat": -33.87, "lon": 151.21,  "flag": "🇦🇺", "provider": "Telstra"},
    {"id": "nz",        "name": "New Zealand",  "country": "NZ", "country_name": "New Zealand",    "continent": "OC", "lat": -36.87, "lon": 174.77,  "flag": "🇳🇿", "provider": "Spark NZ"},
    {"id": "za",        "name": "South Africa", "country": "ZA", "country_name": "South Africa",   "continent": "AF", "lat": -26.20, "lon": 28.04,   "flag": "🇿🇦", "provider": "MTN"},
    {"id": "ng",        "name": "Nigeria",      "country": "NG", "country_name": "Nigeria",        "continent": "AF", "lat": 6.52,   "lon": 3.38,    "flag": "🇳🇬", "provider": "MTN NG"},
    {"id": "eg",        "name": "Egypt",        "country": "EG", "country_name": "Egypt",          "continent": "AF", "lat": 30.06,  "lon": 31.25,   "flag": "🇪🇬", "provider": "TE Data"},
    {"id": "ke",        "name": "Kenya",        "country": "KE", "country_name": "Kenya",          "continent": "AF", "lat": -1.29,  "lon": 36.82,   "flag": "🇰🇪", "provider": "Safaricom"},
]

# Baseline latency (ms) for each region — reflects real-world geography
BASELINE_LATENCY: dict[str, float] = {
    "us-east": 12, "us-west": 15, "us-central": 14, "ca": 18, "mx": 35,
    "br": 80, "ar": 90, "co": 70,
    "gb": 20, "de": 18, "fr": 22, "nl": 16, "se": 25, "es": 30, "it": 28, "pl": 35, "ua": 45, "ru": 55,
    "tr": 50, "ae": 60, "sa": 65, "il": 55,
    "in": 70, "pk": 85, "cn": 90, "jp": 40, "kr": 38, "sg": 45, "hk": 42, "th": 60, "id": 70, "ph": 75, "vn": 65,
    "au": 110, "nz": 130,
    "za": 95, "ng": 120, "eg": 80, "ke": 110,
}

TARGET_HOSTS = [
    {"ip": "8.8.8.8",        "name": "Google DNS"},
    {"ip": "1.1.1.1",        "name": "Cloudflare DNS"},
    {"ip": "208.67.222.222", "name": "OpenDNS"},
    {"ip": "9.9.9.9",        "name": "Quad9 DNS"},
]

# ─── Region state machine ──────────────────────────────────────────────────────
# States: 'healthy', 'degraded', 'warning', 'critical'
_region_state: dict[str, str] = {r["id"]: "healthy" for r in REGIONS}
_state_duration: dict[str, int] = {r["id"]: 0 for r in REGIONS}

_TRANSITION_PROBABILITIES: dict[str, dict[str, float]] = {
    "healthy":  {"healthy": 0.96, "degraded": 0.03, "warning": 0.01, "critical": 0.00},
    "degraded": {"healthy": 0.30, "degraded": 0.60, "warning": 0.08, "critical": 0.02},
    "warning":  {"healthy": 0.10, "degraded": 0.20, "warning": 0.60, "critical": 0.10},
    "critical": {"healthy": 0.05, "degraded": 0.20, "warning": 0.35, "critical": 0.40},
}

_LATENCY_MULTIPLIER: dict[str, float] = {
    "healthy": 1.0, "degraded": 2.5, "warning": 6.0, "critical": 15.0
}
_LOSS_RATE: dict[str, float] = {
    "healthy": 0.5, "degraded": 5.0, "warning": 20.0, "critical": 60.0
}


def _transition_state(region_id: str) -> str:
    """Markov-chain state transition for one region."""
    current = _region_state[region_id]
    _state_duration[region_id] += 1
    # Stay in critical/warning longer for realism
    if _state_duration[region_id] < 3 and current in ("critical", "warning"):
        return current
    probs = _TRANSITION_PROBABILITIES[current]
    states, weights = zip(*probs.items())
    new_state = random.choices(states, weights=weights, k=1)[0]
    if new_state != current:
        _state_duration[region_id] = 0
    _region_state[region_id] = new_state
    return new_state


def _compute_health_score(latency_ms: float, packet_loss_pct: float, reachable: bool) -> float:
    """Deterministic health score 0-100."""
    if not reachable:
        return 0.0
    latency_penalty = min(40.0, max(0.0, (latency_ms - 30) / 12))
    loss_penalty    = min(40.0, packet_loss_pct * 0.8)
    score = max(0.0, 100.0 - latency_penalty - loss_penalty)
    return round(score, 1)


def generate_probe_record(region: dict) -> dict:
    """Generate one simulated probe measurement for a region."""
    region_id = region["id"]
    state     = _transition_state(region_id)
    baseline  = BASELINE_LATENCY.get(region_id, 50.0)

    multiplier  = _LATENCY_MULTIPLIER[state]
    jitter      = random.uniform(0.8, 1.3)
    avg_rtt     = round(baseline * multiplier * jitter + random.gauss(0, 2), 2)
    avg_rtt     = max(1.0, avg_rtt)
    min_rtt     = round(avg_rtt * random.uniform(0.7, 0.95), 2)
    max_rtt     = round(avg_rtt * random.uniform(1.1, 2.5), 2)

    base_loss   = _LOSS_RATE[state]
    packet_loss = round(min(100.0, max(0.0, random.gauss(base_loss, base_loss * 0.3))), 2)
    reachable   = state != "critical" or random.random() > 0.4
    hop_count   = random.randint(6, 20) if reachable else 0
    outage      = state in ("warning", "critical") or packet_loss > 30
    target      = random.choice(TARGET_HOSTS)

    now    = datetime.now(timezone.utc)
    health = _compute_health_score(avg_rtt, packet_loss, reachable)

    return {
        "event_id":          str(uuid.uuid4()),
        "timestamp":         now.isoformat(),
        "unix_ts":           int(now.timestamp()),
        "probe_id":          f"probe-{region_id}-{random.randint(1, 5):03d}",
        "region_id":         region_id,
        "region_name":       region["name"],
        "country_code":      region["country"],
        "country_name":      region["country_name"],
        "continent":         region["continent"],
        "lat":               region["lat"],
        "lon":               region["lon"],
        "flag":              region["flag"],
        "provider":          region["provider"],
        "target_ip":         target["ip"],
        "target_name":       target["name"],
        "avg_rtt_ms":        avg_rtt,
        "min_rtt_ms":        min_rtt,
        "max_rtt_ms":        max_rtt,
        "packet_loss_pct":   packet_loss,
        "hop_count":         hop_count,
        "is_reachable":      reachable,
        "outage_detected":   outage,
        "health_score":      health,
        "state":             state,
        "measurement_type":  "ping",
    }


def simulate_stream(regions: list[dict] | None = None, rate: float = 5.0) -> Iterator[dict]:
    """
    Continuously yield probe records at `rate` records/sec.
    Iterates over all regions in round-robin fashion.
    """
    pool = regions or REGIONS
    interval = 1.0 / max(rate, 0.1)
    while True:
        for region in pool:
            yield generate_probe_record(region)
            time.sleep(interval)


# ─── RIPE Atlas (optional, real public data) ───────────────────────────────────
# ─── Wikimedia Event Streams (real public SSE — the brief's recommended source) ──
# Maps Wikipedia language edition → CloudPulse region_id.
# English Wikipedia edits can come from anywhere; we map it to us-east as the
# proxy (the majority of en.wp editors are in North America/Europe).
WIKI_LANG_TO_REGION: dict[str, str] = {
    "en":       "us-east",   "de":  "de",       "fr":    "fr",
    "ja":       "jp",        "zh":  "cn",        "es":    "es",
    "ru":       "ru",        "pt":  "br",        "it":    "it",
    "pl":       "pl",        "nl":  "nl",        "ko":    "kr",
    "tr":       "tr",        "sv":  "se",        "uk":    "ua",
    "vi":       "vn",        "id":  "id",        "he":    "il",
    "th":       "th",        "ar":  "eg",        "fa":    "ae",
    "fi":       "se",        "cs":  "pl",        "hu":    "de",
    "ro":       "de",        "ca":  "es",        "hi":    "in",
    "bn":       "in",        "commons": "nl",    "wikidata": "nl",
}


def _wiki_domain_to_region(domain: str) -> dict | None:
    lang      = domain.split(".")[0] if domain else ""
    region_id = WIKI_LANG_TO_REGION.get(lang)
    if not region_id:
        return None
    return next((r for r in REGIONS if r["id"] == region_id), None)


def wikimedia_event_to_probe(event: dict) -> dict | None:
    """
    Convert one Wikimedia recentchange SSE event into a CloudPulse probe record.

    Internet health interpretation:
    • A successful Wikipedia edit proves the editor's internet is working.
    • We derive latency from geographic baseline + jitter.
    • Edit rate drops below expected baseline are flagged by the speed layer as
      potential internet issues (implemented in stream_processor sliding window).
    """
    domain = event.get("meta", {}).get("domain", "")
    region = _wiki_domain_to_region(domain)
    if not region:
        return None

    region_id = region["id"]
    baseline  = BASELINE_LATENCY.get(region_id, 50.0)
    latency   = round(baseline * random.uniform(0.85, 1.35) + random.gauss(0, 3), 2)
    latency   = max(1.0, latency)
    loss      = round(random.uniform(0.0, 1.2), 2)   # low: edit proved connectivity
    health    = _compute_health_score(latency, loss, True)
    ts        = event.get("meta", {}).get("dt") or datetime.now(timezone.utc).isoformat()

    return {
        "event_id":         str(uuid.uuid4()),
        "timestamp":        ts,
        "unix_ts":          event.get("timestamp") or int(time.time()),
        "probe_id":         f"wiki-{domain}",
        "region_id":        region_id,
        "region_name":      region["name"],
        "country_code":     region["country"],
        "country_name":     region["country_name"],
        "continent":        region["continent"],
        "lat":              region["lat"],
        "lon":              region["lon"],
        "flag":             region["flag"],
        "provider":         region["provider"],
        "target_ip":        "wikimedia.org",
        "target_name":      f"Wikipedia ({domain})",
        "avg_rtt_ms":       latency,
        "min_rtt_ms":       round(latency * 0.80, 2),
        "max_rtt_ms":       round(latency * 1.40, 2),
        "packet_loss_pct":  loss,
        "hop_count":        random.randint(6, 15),
        "is_reachable":     True,
        "outage_detected":  False,
        "health_score":     health,
        "state":            "healthy",
        "measurement_type": "wikimedia-edit-proxy",
        "wiki_domain":      domain,
        "wiki_type":        event.get("type", ""),
        "is_bot":           event.get("bot", False),
    }


def stream_wikimedia(reconnect_delay: int = 5) -> Iterator[dict]:
    """
    Consume the Wikimedia Event Streams SSE feed in real time.
    URL: https://stream.wikimedia.org/v2/stream/recentchange

    Yields CloudPulse probe records derived from Wikipedia edit events.
    No API key required — this is a fully public endpoint.

    Internet health justification:
    - Each Wikipedia edit proves the contributing editor has a working internet
      connection from their region.
    - The speed layer tracks the edit rate per region per 5-minute window.
    - A significant drop below the historical baseline (batch layer) indicates
      a potential internet disruption in that region.
    """
    url = "https://stream.wikimedia.org/v2/stream/recentchange"
    while True:
        try:
            print(f"[Wikimedia] Connecting to {url} …")
            resp = requests.get(
                url, stream=True,
                headers={"Accept": "text/event-stream", "User-Agent": "CloudPulse/1.0"},
                timeout=30,
            )
            resp.raise_for_status()
            print("[Wikimedia] Connected — streaming live Wikipedia edit events")

            event_data = ""
            for raw_line in resp.iter_lines(decode_unicode=True):
                if raw_line is None:
                    continue
                if raw_line.startswith("data:"):
                    event_data = raw_line[5:].strip()
                elif raw_line == "" and event_data:
                    try:
                        raw = json.loads(event_data)
                        record = wikimedia_event_to_probe(raw)
                        if record:
                            yield record
                    except (json.JSONDecodeError, Exception):
                        pass
                    event_data = ""

        except Exception as exc:
            print(f"[Wikimedia] Stream error: {exc} — reconnecting in {reconnect_delay}s")
            time.sleep(reconnect_delay)


# ─── RIPE Atlas (optional, real public data) ───────────────────────────────────
def fetch_ripe_atlas_measurements(limit: int = 100) -> list[dict]:
    """
    Fetch recent public RIPE Atlas ping measurements.
    Converts results to CloudPulse probe record format.
    No API key required for public measurements.
    """
    try:
        url = "https://atlas.ripe.net/api/v2/measurements/"
        params = {"type": "ping", "status": 2, "is_public": True, "limit": limit}
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        measurements = resp.json().get("results", [])
        records = []
        for m in measurements:
            result_url = f"https://atlas.ripe.net/api/v2/measurements/{m['id']}/results/"
            rr = requests.get(result_url, params={"limit": 1}, timeout=5)
            if rr.status_code != 200:
                continue
            for result in rr.json():
                avg_rtt = result.get("avg") or 999
                loss    = result.get("loss") or 0
                records.append({
                    "event_id":        str(uuid.uuid4()),
                    "timestamp":       datetime.now(timezone.utc).isoformat(),
                    "unix_ts":         int(time.time()),
                    "probe_id":        f"ripe-{result.get('prb_id', 0)}",
                    "region_id":       "ripe-atlas",
                    "region_name":     "RIPE Atlas",
                    "country_code":    "XX",
                    "country_name":    "Unknown",
                    "avg_rtt_ms":      round(float(avg_rtt), 2),
                    "packet_loss_pct": round(float(loss), 2),
                    "is_reachable":    avg_rtt < 999,
                    "outage_detected": avg_rtt > 500 or loss > 30,
                    "health_score":    _compute_health_score(float(avg_rtt), float(loss), avg_rtt < 999),
                    "measurement_type": "ping",
                    "source": "ripe-atlas",
                })
        return records
    except Exception as exc:
        print(f"[RIPE Atlas] Warning: {exc}")
        return []
