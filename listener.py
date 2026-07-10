#!/usr/bin/env python3
"""
Weather Station Forwarder — Local Push Listener

Receives Wunderground-protocol pushes from an Ambient Weather console's
"Customized" server slot (local network, no cloud round-trip) and forwards
to Wunderground, CWOP, PWSWeather, OpenWeatherMap, Windy, and WeatherCloud.

Every sender runs on its own thread, fully decoupled from the console's
push cadence -- a slow or hung service can never block another, and can
never delay the next console push from being accepted. Per-service status
(last attempt, last success, last message) is tracked and exposed via a
small built-in web UI at http://<host>:<port>/.

healthchecks.io (or self-hosted) is pinged on every console push,
unconditionally -- it answers "is the console reaching the listener at
all", not "did every downstream service succeed". Per-service health is
a web UI concern, not a paging concern.
"""

import json
import logging
import os
import sys
import time
import threading
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).parent.resolve()
STATE_FILE = Path(os.environ.get("STATE_FILE", str(BASE_DIR / "state.json")))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("listener")

# ---------------------------------------------------------------------------
# Config -- from environment variables
# ---------------------------------------------------------------------------

def require_env(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        log.error("Required environment variable %s is not set", name)
        sys.exit(1)
    return val

def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()

LISTEN_PORT = int(env("LISTEN_PORT", "8090"))
LAT = env("STATION_LATITUDE")
LON = env("STATION_LONGITUDE")

CWOP_STATION_ID = require_env("CWOP_STATION_ID")
CWOP_VALIDATION_CODE = env("CWOP_VALIDATION_CODE")

WU_STATION_ID = require_env("WUNDERGROUND_STATION_ID")
WU_STATION_KEY = require_env("WUNDERGROUND_STATION_KEY")

PWS_STATION_ID = require_env("PWSWEATHER_STATION_ID")
PWS_API_KEY = require_env("PWSWEATHER_API_KEY")

OWM_API_KEY = require_env("OPENWEATHERMAP_API_KEY")
OWM_STATION_ID = require_env("OPENWEATHERMAP_STATION_ID")

WINDY_STATION_ID = env("WINDY_STATION_ID")
WINDY_STATION_PASSWORD = env("WINDY_STATION_PASSWORD")

WEATHERCLOUD_ID = env("WEATHERCLOUD_ID")
WEATHERCLOUD_KEY = env("WEATHERCLOUD_KEY")

HEALTHCHECKS_URL = env("HEALTHCHECKS_URL")

# ---------------------------------------------------------------------------
# State (persistent JSON) -- protected by STATE_LOCK
# ---------------------------------------------------------------------------

STATE_LOCK = threading.Lock()

# Services registered for status tracking / the web UI. Order here is the
# display order on the status page.
SERVICE_NAMES = ["wunderground", "cwop", "pwsweather", "openweathermap", "windy", "weathercloud"]

def default_service_status() -> dict:
    return {
        "enabled": True,
        "last_attempt_ts": 0,
        "last_success_ts": 0,
        "last_status": "never run",   # "ok" | "error" | "skipped" | "never run"
        "last_message": "",
    }

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text())
            state.setdefault("precip_history", [])
            state.setdefault("last_cwop_ts", 0)
            state.setdefault("last_pws_ts", 0)
            state.setdefault("last_owm_ts", 0)
            state.setdefault("last_windy_ts", 0)
            state.setdefault("last_weathercloud_ts", 0)
            state.setdefault("last_cwop_obs_time", None)
            state.setdefault("services", {})
            for name in SERVICE_NAMES:
                state["services"].setdefault(name, default_service_status())
            state.setdefault("last_push_ts", 0)
            state.setdefault("last_conditions", {})
            return state
        except Exception as e:
            log.warning("Could not read state file, starting fresh: %s", e)
    return {
        "precip_history": [],
        "last_cwop_ts": 0,
        "last_pws_ts": 0,
        "last_owm_ts": 0,
        "last_windy_ts": 0,
        "last_weathercloud_ts": 0,
        "last_cwop_obs_time": None,
        "services": {name: default_service_status() for name in SERVICE_NAMES},
        "last_push_ts": 0,
        "last_conditions": {},
    }

def save_state(state: dict) -> None:
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(STATE_FILE)

def record_service_result(service: str, status: str, message: str = "") -> None:
    """Thread-safe update of a single service's status. Safe to call from
    any sender thread without holding STATE_LOCK for the whole send."""
    with STATE_LOCK:
        state = load_state()
        svc = state["services"].setdefault(service, default_service_status())
        now = int(time.time() * 1000)
        svc["last_attempt_ts"] = now
        svc["last_status"] = status
        svc["last_message"] = message[:300]
        if status == "ok":
            svc["last_success_ts"] = now
            if service == "cwop":
                state["last_cwop_ts"] = now
            elif service == "pwsweather":
                state["last_pws_ts"] = now
            elif service == "openweathermap":
                state["last_owm_ts"] = now
            elif service == "windy":
                state["last_windy_ts"] = now
            elif service == "weathercloud":
                state["last_weathercloud_ts"] = now
        save_state(state)

# ---------------------------------------------------------------------------
# Unit conversions
# ---------------------------------------------------------------------------

def f_to_c(f): return (f - 32) * 5 / 9
def mph_to_mps(mph): return mph * 0.44704
def inh_to_hpa(inh): return inh * 33.86389
def in_to_mm(inches): return inches * 25.4

# ---------------------------------------------------------------------------
# HTTP helpers (outbound)
# ---------------------------------------------------------------------------

DEFAULT_TIMEOUT = 15
OUTBOUND_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; weather-listener/1.0)"}

def http_get(url: str, retries: int = 2, timeout: int = DEFAULT_TIMEOUT) -> str:
    req = urllib.request.Request(url, headers=OUTBOUND_HEADERS)
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode()
        except Exception as e:
            if attempt < retries - 1:
                log.warning("GET attempt %d failed (%s), retrying in 5s...", attempt + 1, e)
                time.sleep(5)
            else:
                raise
    raise RuntimeError(f"GET failed after {retries} attempts: {url}")

def http_post(url: str, payload: bytes, headers: dict, retries: int = 2, timeout: int = DEFAULT_TIMEOUT) -> tuple[int, str]:
    h = {**OUTBOUND_HEADERS, **headers}
    req = urllib.request.Request(url, data=payload, headers=h, method="POST")
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.status, r.read().decode()
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            if attempt < retries - 1:
                log.warning("POST attempt %d failed %s (%s), retrying in 5s...", attempt + 1, e.code, body)
                time.sleep(5)
            else:
                raise
        except Exception as e:
            if attempt < retries - 1:
                log.warning("POST attempt %d failed (%s), retrying in 5s...", attempt + 1, e)
                time.sleep(5)
            else:
                raise
    raise RuntimeError(f"POST failed after {retries} attempts: {url}")

def http_get_json(url: str, timeout: int = DEFAULT_TIMEOUT) -> dict:
    return json.loads(http_get(url, timeout=timeout))

# ---------------------------------------------------------------------------
# Precip accumulation (rolling 60-min window from rate)
# ---------------------------------------------------------------------------

ONE_HOUR_MS = 3_600_000

def update_precip_history(state: dict, rate_in: float) -> float | None:
    now_ms = int(time.time() * 1000)
    history = state["precip_history"]
    history.append({"rate_in": rate_in, "ts_ms": now_ms})
    history[:] = [e for e in history if e["ts_ms"] >= now_ms - ONE_HOUR_MS]
    state["precip_history"] = history

    if len(history) < 2:
        return None
    span = now_ms - history[0]["ts_ms"]
    if span < ONE_HOUR_MS * 0.95:
        return None

    total = 0.0
    for i in range(1, len(history)):
        prev = history[i - 1]
        curr = history[i]
        frac = (curr["ts_ms"] - prev["ts_ms"]) / ONE_HOUR_MS
        total += prev["rate_in"] * frac
    return round(total, 3)

# ---------------------------------------------------------------------------
# Parse incoming Wunderground-protocol push from the console
# ---------------------------------------------------------------------------

def parse_console_push(query: dict) -> dict:
    def f(key):
        v = query.get(key, [None])[0]
        return float(v) if v not in (None, "") else None

    def i(key):
        v = query.get(key, [None])[0]
        return int(float(v)) if v not in (None, "") else None

    c = {}
    c["obs_time_ms"] = int(time.time() * 1000)
    c["lat"] = LAT
    c["lon"] = LON

    tempf = f("tempf")
    if tempf is not None:
        c["temp_f"] = round(tempf, 2)
        c["temp_c"] = round(f_to_c(tempf), 2)

    dewptf = f("dewptf")
    if dewptf is not None:
        c["dewpt_f"] = round(dewptf, 2)
        c["dewpt_c"] = round(f_to_c(dewptf), 2)

    wind = f("windspeedmph")
    if wind is not None:
        c["wind_mph"] = round(wind, 2)
        c["wind_mps"] = round(mph_to_mps(wind), 2)

    gust = f("windgustmph")
    if gust is not None:
        c["gust_mph"] = round(gust, 2)
        c["gust_mps"] = round(mph_to_mps(gust), 2)

    winddir = i("winddir")
    if winddir is not None:
        c["winddir"] = winddir

    baromin = f("baromin")
    if baromin is not None:
        c["pressure_inhg"] = round(baromin, 3)
        c["pressure_hpa"] = round(inh_to_hpa(baromin), 1)

    humidity = i("humidity")
    if humidity is not None:
        c["humidity"] = humidity

    uv = f("UV")
    if uv is not None:
        c["uv"] = uv

    solarradiation = f("solarradiation")
    if solarradiation is not None:
        c["solar_radiation"] = solarradiation

    rainin = f("rainin")
    if rainin is not None:
        c["precip_rate_in"] = round(rainin, 3)
        c["precip_rate_mm"] = round(in_to_mm(rainin), 2)

    dailyrainin = f("dailyrainin")
    if dailyrainin is not None:
        c["precip_since_midnight_in"] = round(dailyrainin, 3)
        c["precip_since_midnight_mm"] = round(in_to_mm(dailyrainin), 2)

    return c

# ---------------------------------------------------------------------------
# Senders -- each is self-contained: reads what it needs from `c`, does its
# own OWM-internal-id-style bootstrap if needed, and returns nothing. All
# status/timestamp bookkeeping happens via record_service_result(), called
# by the thread wrapper, not by the sender itself.
# ---------------------------------------------------------------------------

def send_wunderground(c: dict) -> str:
    dt = datetime.fromtimestamp(c["obs_time_ms"] / 1000, tz=timezone.utc)
    dateutc = dt.strftime("%Y-%m-%d %H:%M:%S")

    optional = {}
    if "temp_f" in c: optional["tempf"] = c["temp_f"]
    if "dewpt_f" in c: optional["dewptf"] = c["dewpt_f"]
    if "wind_mph" in c: optional["windspeedmph"] = c["wind_mph"]
    if "gust_mph" in c: optional["windgustmph"] = c["gust_mph"]
    if "winddir" in c: optional["winddir"] = c["winddir"]
    if "pressure_inhg" in c: optional["baromin"] = c["pressure_inhg"]
    if "humidity" in c: optional["humidity"] = c["humidity"]
    if "uv" in c: optional["uv"] = c["uv"]
    if "solar_radiation" in c: optional["solarradiation"] = c["solar_radiation"]
    if "precip_last_hour_in" in c: optional["rainin"] = c["precip_last_hour_in"]
    if "precip_since_midnight_in" in c: optional["dailyrainin"] = c["precip_since_midnight_in"]

    params = {
        "ID": WU_STATION_ID,
        "PASSWORD": WU_STATION_KEY,
        "dateutc": dateutc,
        "softwaretype": "python-listener-v2",
        "action": "updateraw",
        "realtime": "1",
        "rtfreq": "30",
    }
    params.update(optional)

    url = "https://rtupdate.wunderground.com/weatherstation/updateweatherstation.php?" + urllib.parse.urlencode(params)
    response = http_get(url).strip()
    return response

def send_cwop(c: dict) -> str:
    for field in ("temp_f", "wind_mph", "gust_mph", "winddir"):
        if field not in c:
            raise RuntimeError(f"missing required field {field}, skipped")

    params = {
        "id": CWOP_STATION_ID,
        "lat": c["lat"],
        "long": c["lon"],
        "time": c["obs_time_ms"],
        "tempf": c["temp_f"],
        "windspeedmph": c["wind_mph"],
        "windgustmph": c["gust_mph"],
        "winddir": c["winddir"],
        "software": "python-listener-v2",
    }
    if CWOP_VALIDATION_CODE:
        params["validation"] = CWOP_VALIDATION_CODE
    if "pressure_hpa" in c: params["pressure"] = c["pressure_hpa"]
    if "humidity" in c: params["humidity"] = c["humidity"]
    if "solar_radiation" in c: params["solarradiation"] = c["solar_radiation"]
    if "precip_last_hour_in" in c: params["rainin"] = c["precip_last_hour_in"]
    if "precip_since_midnight_in" in c: params["dailyrainin"] = c["precip_since_midnight_in"]

    url = "https://send.cwop.rest/?" + urllib.parse.urlencode(params)
    response = http_get(url).strip()
    return response

def send_pwsweather(c: dict) -> str:
    dt = datetime.fromtimestamp(c["obs_time_ms"] / 1000, tz=timezone.utc)
    dateutc = dt.strftime("%Y-%m-%d+%H:%M:%S")

    optional = {}
    if "temp_f" in c: optional["tempf"] = c["temp_f"]
    if "dewpt_f" in c: optional["dewptf"] = c["dewpt_f"]
    if "wind_mph" in c: optional["windspeedmph"] = c["wind_mph"]
    if "gust_mph" in c: optional["windgustmph"] = c["gust_mph"]
    if "winddir" in c: optional["winddir"] = c["winddir"]
    if "pressure_inhg" in c: optional["baromin"] = c["pressure_inhg"]
    if "humidity" in c: optional["humidity"] = c["humidity"]
    if "uv" in c: optional["uv"] = c["uv"]
    if "solar_radiation" in c: optional["solarradiation"] = c["solar_radiation"]
    if "precip_last_hour_in" in c: optional["rainin"] = c["precip_last_hour_in"]
    if "precip_since_midnight_in" in c: optional["dailyrainin"] = c["precip_since_midnight_in"]

    url = (
        f"https://pwsupdate.pwsweather.com/api/v1/submitwx"
        f"?ID={PWS_STATION_ID}&PASSWORD={PWS_API_KEY}&dateutc={dateutc}"
        f"&softwaretype=python-listener-v2&action=updateraw"
        f"&{urllib.parse.urlencode(optional)}"
    )
    response = http_get(url).strip()
    return response

_owm_internal_id_lock = threading.Lock()
_owm_internal_id_cache = {"id": None}

def _resolve_owm_internal_id() -> str:
    with _owm_internal_id_lock:
        if _owm_internal_id_cache["id"]:
            return _owm_internal_id_cache["id"]
        stations_url = f"https://api.openweathermap.org/data/3.0/stations?APPID={OWM_API_KEY}"
        stations = http_get_json(stations_url)
        match = next((s for s in stations if str(s.get("external_id")) == str(OWM_STATION_ID)), None)
        if not match:
            raise RuntimeError(f"station with external_id {OWM_STATION_ID} not found")
        _owm_internal_id_cache["id"] = match["id"]
        return match["id"]

def send_openweathermap(c: dict) -> str:
    internal_id = _resolve_owm_internal_id()

    measurement = {"station_id": internal_id, "dt": int(c["obs_time_ms"] / 1000)}
    if "temp_c" in c: measurement["temperature"] = c["temp_c"]
    if "dewpt_c" in c: measurement["dew_point"] = c["dewpt_c"]
    if "wind_mps" in c: measurement["wind_speed"] = c["wind_mps"]
    if "gust_mps" in c: measurement["wind_gust"] = c["gust_mps"]
    if "winddir" in c: measurement["wind_deg"] = c["winddir"]
    if "pressure_hpa" in c: measurement["pressure"] = c["pressure_hpa"]
    if "humidity" in c: measurement["humidity"] = c["humidity"]
    if "precip_last_hour_mm" in c: measurement["rain_1h"] = c["precip_last_hour_mm"]

    payload = json.dumps([measurement]).encode()
    url = f"https://api.openweathermap.org/data/3.0/measurements?APPID={OWM_API_KEY}"
    status, body = http_post(url, payload, {"Content-Type": "application/json"})
    return f"{status} {body.strip() or '(empty)'}"

def send_windy(c: dict) -> str:
    params = {
        "id": WINDY_STATION_ID,
        "PASSWORD": WINDY_STATION_PASSWORD,
        "ts": int(c["obs_time_ms"] / 1000),
    }
    if "temp_c" in c: params["temp"] = c["temp_c"]
    if "dewpt_c" in c: params["dewpoint"] = c["dewpt_c"]
    if "wind_mps" in c: params["wind"] = c["wind_mps"]
    if "gust_mps" in c: params["gust"] = c["gust_mps"]
    if "winddir" in c: params["winddir"] = c["winddir"]
    if "humidity" in c: params["humidity"] = c["humidity"]
    if "pressure_hpa" in c: params["pressure"] = round(c["pressure_hpa"] * 100)  # hPa -> Pa
    if "uv" in c: params["uv"] = c["uv"]
    if "solar_radiation" in c: params["solarradiation"] = c["solar_radiation"]
    if "precip_since_midnight_mm" in c: params["precip"] = c["precip_since_midnight_mm"]

    url = "https://stations.windy.com/api/v2/observation/update?" + urllib.parse.urlencode(params)
    response = http_get(url).strip()
    return response or "(empty -- treated as success)"

def send_weathercloud(c: dict) -> str:
    """WeatherCloud's backend has proven unreliable -- observed slow/hanging
    connections up to ~1 minute, plus intermittent 502/500/429. Called with
    a generous 2-minute timeout and zero retries (retrying just compounds
    rate-limit pressure on their side)."""
    dt = datetime.fromtimestamp(c["obs_time_ms"] / 1000, tz=timezone.utc)

    params = {
        "wid": WEATHERCLOUD_ID,
        "key": WEATHERCLOUD_KEY,
        "date": dt.strftime("%Y%m%d"),
        "time": dt.strftime("%H%M"),
        "software": "python-listener-v2",
    }
    if "temp_c" in c: params["temp"] = round(c["temp_c"] * 10)
    if "dewpt_c" in c: params["dew"] = round(c["dewpt_c"] * 10)
    if "wind_mps" in c: params["wspd"] = round(c["wind_mps"] * 10)
    if "gust_mps" in c: params["wspdhi"] = round(c["gust_mps"] * 10)
    if "winddir" in c: params["wdir"] = c["winddir"]
    if "pressure_hpa" in c: params["bar"] = round(c["pressure_hpa"] * 10)
    if "humidity" in c: params["hum"] = c["humidity"]
    if "uv" in c: params["uvi"] = round(c["uv"] * 10)
    if "solar_radiation" in c: params["solarrad"] = round(c["solar_radiation"] * 10)
    if "precip_rate_mm" in c: params["rainrate"] = round(c["precip_rate_mm"] * 10)
    if "precip_since_midnight_mm" in c: params["rain"] = round(c["precip_since_midnight_mm"] * 10)

    url = "https://api.weathercloud.net/v01/set?" + urllib.parse.urlencode(params)
    result = http_get(url, retries=1, timeout=120).strip()
    if result != "200":
        raise RuntimeError(f"rejected: {result}")
    return result

SENDERS = {
    "wunderground": (send_wunderground, None),
    "cwop": (send_cwop, 5 * 60 * 1000),
    "pwsweather": (send_pwsweather, 5 * 60 * 1000),
    "openweathermap": (send_openweathermap, 1 * 60 * 1000),
    "windy": (send_windy, 5 * 60 * 1000),
    "weathercloud": (send_weathercloud, 10 * 60 * 1000),
}

# Guards against overlapping sends of the *same* service if one push's
# send is still in flight when the next push arrives.
_INFLIGHT_LOCKS = {name: threading.Lock() for name in SENDERS}

def elapsed_ms(ts_ms: int) -> int:
    return int(time.time() * 1000) - ts_ms

def dispatch_sender(name: str, c: dict) -> None:
    """Runs entirely on its own thread. Never touches STATE_LOCK except
    briefly via record_service_result(). A hung or slow sender here can
    never block another sender or the next console push."""
    lock = _INFLIGHT_LOCKS[name]
    if not lock.acquire(blocking=False):
        log.info("%s: previous send still in flight, skipping this push", name)
        return
    try:
        fn, _ = SENDERS[name]
        try:
            result = fn(c)
            log.info("%s response: %s", name, result)
            record_service_result(name, "ok", result)
        except Exception as e:
            log.warning("%s send failed: %s", name, e)
            record_service_result(name, "error", str(e))
    finally:
        lock.release()

# ---------------------------------------------------------------------------
# Healthchecks.io ping
# ---------------------------------------------------------------------------

def ping_healthcheck(success: bool = True) -> None:
    url = HEALTHCHECKS_URL
    if not url:
        return
    ping_url = url if success else url + "/fail"
    try:
        http_get(ping_url)
    except Exception as e:
        log.warning("Healthcheck ping failed: %s", e)

# ---------------------------------------------------------------------------
# Core handler logic — called on every console push
# ---------------------------------------------------------------------------

def handle_push(query: dict) -> None:
    # Brief lock: parse, update precip history, decide what needs sending,
    # snapshot everything, then release before any network I/O happens.
    with STATE_LOCK:
        state = load_state()
        conditions = parse_console_push(query)

        if "precip_rate_in" in conditions:
            precip_last_hour = update_precip_history(state, conditions["precip_rate_in"])
            if precip_last_hour is not None:
                conditions["precip_last_hour_in"] = precip_last_hour
                conditions["precip_last_hour_mm"] = round(in_to_mm(precip_last_hour), 2)
        else:
            update_precip_history(state, 0.0)

        state["last_push_ts"] = int(time.time() * 1000)
        state["last_conditions"] = conditions

        interval_state_key = {
            "cwop": "last_cwop_ts",
            "pwsweather": "last_pws_ts",
            "openweathermap": "last_owm_ts",
            "windy": "last_windy_ts",
            "weathercloud": "last_weathercloud_ts",
        }

        due = []
        for name, (_, interval_ms) in SENDERS.items():
            if interval_ms is None:
                due.append(name)  # e.g. wunderground -- sends every push
                continue
            last_ts = state.get(interval_state_key[name], 0)
            if elapsed_ms(last_ts) >= interval_ms:
                due.append(name)

        save_state(state)

    log.info(
        "Push received: temp=%s°F wind=%s mph gust=%s mph dir=%s°",
        conditions.get("temp_f", "?"),
        conditions.get("wind_mph", "?"),
        conditions.get("gust_mph", "?"),
        conditions.get("winddir", "?"),
    )

    # Dispatch every due sender on its own thread -- fully decoupled from
    # this handler and from each other.
    for name in due:
        if name in ("windy",) and not (WINDY_STATION_ID and WINDY_STATION_PASSWORD):
            continue
        if name in ("weathercloud",) and not (WEATHERCLOUD_ID and WEATHERCLOUD_KEY):
            continue
        threading.Thread(target=dispatch_sender, args=(name, conditions), daemon=True).start()

    # healthchecks is a pure dead-man's-switch: "is the console reaching
    # the listener" -- always pinged on success here, independent of
    # whether any individual downstream sender fails.
    ping_healthcheck(success=True)

# ---------------------------------------------------------------------------
# Web UI
# ---------------------------------------------------------------------------

STATUS_PAGE_HTML = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="15">
<title>Weather Listener Status</title>
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #0f1115; color: #e2e4e9; margin: 0; padding: 2rem; }
  h1 { font-size: 1.4rem; margin-bottom: 0.25rem; }
  .subtitle { color: #8b8f9a; margin-bottom: 1.5rem; font-size: 0.9rem; }
  table { border-collapse: collapse; width: 100%; max-width: 720px; }
  th, td { text-align: left; padding: 0.5rem 0.75rem; border-bottom: 1px solid #24262e; }
  th { color: #8b8f9a; font-weight: 600; font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.03em; }
  .status-ok { color: #4ade80; }
  .status-error { color: #f87171; }
  .status-skipped { color: #fbbf24; }
  .status-never { color: #6b7280; }
  .conditions { max-width: 720px; margin-bottom: 2rem; background: #171920; border-radius: 8px; padding: 1rem 1.5rem; }
  .conditions dl { display: grid; grid-template-columns: auto auto; gap: 0.25rem 1.5rem; margin: 0; }
  .conditions dt { color: #8b8f9a; }
  .conditions dd { margin: 0; }
  code { color: #93c5fd; }
</style>
</head>
<body>
<h1>Weather Listener</h1>
<div class="subtitle">Auto-refreshes every 15s &middot; last push: <span id="last-push">__LAST_PUSH__</span></div>

<div class="conditions">
<dl>
__CONDITIONS_ROWS__
</dl>
</div>

<table>
<thead><tr><th>Service</th><th>Status</th><th>Last Success</th><th>Message</th></tr></thead>
<tbody>
__SERVICE_ROWS__
</tbody>
</table>

</body>
</html>
"""

def _fmt_ago(ts_ms: int) -> str:
    if not ts_ms:
        return "never"
    secs = int((time.time() * 1000 - ts_ms) / 1000)
    if secs < 60:
        return f"{secs}s ago"
    if secs < 3600:
        return f"{secs // 60}m ago"
    return f"{secs // 3600}h {(secs % 3600) // 60}m ago"

def render_status_page() -> str:
    with STATE_LOCK:
        state = load_state()

    conditions = state.get("last_conditions", {})
    cond_pairs = [
        ("Temp", f'{conditions.get("temp_f", "?")}°F'),
        ("Wind", f'{conditions.get("wind_mph", "?")} mph'),
        ("Gust", f'{conditions.get("gust_mph", "?")} mph'),
        ("Direction", f'{conditions.get("winddir", "?")}°'),
        ("Humidity", f'{conditions.get("humidity", "?")}%'),
        ("Pressure", f'{conditions.get("pressure_inhg", "?")} inHg'),
    ]
    conditions_rows = "\n".join(f"<dt>{k}</dt><dd>{v}</dd>" for k, v in cond_pairs)

    rows = []
    for name in SERVICE_NAMES:
        svc = state["services"].get(name, default_service_status())
        status = svc["last_status"]
        css = {"ok": "status-ok", "error": "status-error", "skipped": "status-skipped"}.get(status, "status-never")
        rows.append(
            f"<tr><td>{name}</td>"
            f'<td class="{css}">{status}</td>'
            f"<td>{_fmt_ago(svc['last_success_ts'])}</td>"
            f"<td><code>{svc['last_message'][:120]}</code></td></tr>"
        )
    service_rows = "\n".join(rows)

    return (
        STATUS_PAGE_HTML
        .replace("__LAST_PUSH__", _fmt_ago(state.get("last_push_ts", 0)))
        .replace("__CONDITIONS_ROWS__", conditions_rows)
        .replace("__SERVICE_ROWS__", service_rows)
    )

def render_status_json() -> dict:
    with STATE_LOCK:
        state = load_state()
    return {
        "last_push_ts": state.get("last_push_ts", 0),
        "last_conditions": state.get("last_conditions", {}),
        "services": state.get("services", {}),
    }

# ---------------------------------------------------------------------------
# HTTP server
# ---------------------------------------------------------------------------

class PushHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # suppress default noisy access logging

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path == "/":
            body = render_status_page().encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if parsed.path == "/api/status":
            body = json.dumps(render_status_json(), indent=2).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if parsed.path == "/weatherstation/updateweatherstation.php":
            query = urllib.parse.parse_qs(parsed.query)
            log.info("Raw request: %s", self.path)

            # Respond immediately so the console doesn't retry
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"success")

            try:
                handle_push(query)
            except Exception:
                log.exception("Error handling push")
            return

        self.send_response(404)
        self.end_headers()

def main():
    server = ThreadingHTTPServer(("0.0.0.0", LISTEN_PORT), PushHandler)
    log.info("Listening on 0.0.0.0:%d — waiting for console pushes, web UI at /...", LISTEN_PORT)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Shutting down.")

if __name__ == "__main__":
    main()