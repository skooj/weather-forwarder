#!/usr/bin/env python3
"""
Weather Station Forwarder — Local Push Listener

Receives Wunderground-protocol pushes from an Ambient Weather console's
"Customized" server slot (local network, no cloud round-trip) and forwards
to CWOP, PWSWeather, and OpenWeatherMap.
"""

import json
import logging
import os
import sys
import time
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
# State (persistent JSON) — thread-safe-ish since ThreadingHTTPServer
# ---------------------------------------------------------------------------

import threading
STATE_LOCK = threading.Lock()

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text())
            state.setdefault("last_wu_ts", 0)
            state.setdefault("last_windy_ts", 0)
            state.setdefault("last_weathercloud_ts", 0)
            state.setdefault("precip_history", [])
            state.setdefault("last_cwop_ts", 0)
            state.setdefault("last_pws_ts", 0)
            state.setdefault("last_owm_ts", 0)
            state.setdefault("last_cwop_obs_time", None)
            return state
        except Exception as e:
            log.warning("Could not read state file, starting fresh: %s", e)
    return {
        "precip_history": [],
        "last_wu_ts": 0,
        "last_windy_ts": 0,
        "last_weathercloud_ts": 0,
        "last_cwop_ts": 0,
        "last_pws_ts": 0,
        "last_owm_ts": 0,
        "last_cwop_obs_time": None,
    }

def save_state(state: dict) -> None:
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(STATE_FILE)

# ---------------------------------------------------------------------------
# Unit conversions
# ---------------------------------------------------------------------------

def f_to_c(f): return (f - 32) * 5 / 9
def mph_to_mps(mph): return mph * 0.44704
def inh_to_hpa(inh): return inh * 33.86389
def in_to_mm(inches): return inches * 25.4

# ---------------------------------------------------------------------------
# HTTP helpers (outbound, to CWOP/PWSWeather/OWM)
# ---------------------------------------------------------------------------

TIMEOUT = 15
OUTBOUND_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; weather-listener/1.0)"}

def http_get(url: str, retries: int = 2) -> str:
    req = urllib.request.Request(url, headers=OUTBOUND_HEADERS)
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
                return r.read().decode()
        except Exception as e:
            if attempt < retries - 1:
                log.warning("GET attempt %d failed (%s), retrying in 5s...", attempt + 1, e)
                time.sleep(5)
            else:
                raise
    raise RuntimeError(f"GET failed after {retries} attempts: {url}")

def http_post(url: str, payload: bytes, headers: dict, retries: int = 2) -> tuple[int, str]:
    h = {**OUTBOUND_HEADERS, **headers}
    req = urllib.request.Request(url, data=payload, headers=h, method="POST")
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
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

def http_get_json(url: str) -> dict:
    return json.loads(http_get(url))

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
    """
    The console sends standard Wunderground PWS protocol query params:
    tempf, dewptf, windspeedmph, windgustmph, winddir, baromin, humidity,
    UV, solarradiation, rainin, dailyrainin, dateutc, etc.
    """
    def f(key):
        v = query.get(key, [None])[0]
        return float(v) if v not in (None, "") else None

    def i(key):
        v = query.get(key, [None])[0]
        return int(float(v)) if v not in (None, "") else None

    c = {}
    c["obs_time_ms"] = int(time.time() * 1000)  # console doesn't reliably send usable dateutc locally; stamp on receipt
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
# Senders
# ---------------------------------------------------------------------------

def send_wunderground(c: dict, state: dict) -> bool:
    station_id = WU_STATION_ID
    station_key = WU_STATION_KEY

    dt = datetime.fromtimestamp(c["obs_time_ms"] / 1000, tz=timezone.utc)
    dateutc = dt.strftime("%Y-%m-%d %H:%M:%S")

    optional = {}
    if "temp_f" in c:
        optional["tempf"] = c["temp_f"]
    if "dewpt_f" in c:
        optional["dewptf"] = c["dewpt_f"]
    if "wind_mph" in c:
        optional["windspeedmph"] = c["wind_mph"]
    if "gust_mph" in c:
        optional["windgustmph"] = c["gust_mph"]
    if "winddir" in c:
        optional["winddir"] = c["winddir"]
    if "pressure_inhg" in c:
        optional["baromin"] = c["pressure_inhg"]
    if "humidity" in c:
        optional["humidity"] = c["humidity"]
    if "uv" in c:
        optional["uv"] = c["uv"]
    if "solar_radiation" in c:
        optional["solarradiation"] = c["solar_radiation"]
    if "precip_last_hour_in" in c:
        optional["rainin"] = c["precip_last_hour_in"]
    if "precip_since_midnight_in" in c:
        optional["dailyrainin"] = c["precip_since_midnight_in"]

    params = {
        "ID": station_id,
        "PASSWORD": station_key,
        "dateutc": dateutc,
        "softwaretype": "python-listener-v1",
        "action": "updateraw",
        "realtime": "1",
        "rtfreq": "30",
    }
    params.update(optional)

    url = "https://rtupdate.wunderground.com/weatherstation/updateweatherstation.php?" + urllib.parse.urlencode(params)
    response = http_get(url)
    log.info("Wunderground response: %s", response.strip())

    state["last_wu_ts"] = int(time.time() * 1000)
    return True

def send_cwop(c: dict, state: dict) -> bool:
    station_id = CWOP_STATION_ID
    validation = CWOP_VALIDATION_CODE or None

    for field in ("temp_f", "wind_mph", "gust_mph", "winddir"):
        if field not in c:
            log.warning("CWOP: missing required field %s, skipping this push", field)
            return False

    params = {
        "id": station_id,
        "lat": c["lat"],
        "long": c["lon"],
        "time": c["obs_time_ms"],
        "tempf": c["temp_f"],
        "windspeedmph": c["wind_mph"],
        "windgustmph": c["gust_mph"],
        "winddir": c["winddir"],
        "software": "python-listener-v1",
    }
    if validation:
        params["validation"] = validation
    if "pressure_hpa" in c:
        params["pressure"] = c["pressure_hpa"]
    if "humidity" in c:
        params["humidity"] = c["humidity"]
    if "solar_radiation" in c:
        params["solarradiation"] = c["solar_radiation"]
    if "precip_last_hour_in" in c:
        params["rainin"] = c["precip_last_hour_in"]
    if "precip_since_midnight_in" in c:
        params["dailyrainin"] = c["precip_since_midnight_in"]

    url = "https://send.cwop.rest/?" + urllib.parse.urlencode(params)
    response = http_get(url)
    log.info("CWOP response: %s", response.strip())

    state["last_cwop_ts"] = int(time.time() * 1000)
    return True

def send_pwsweather(c: dict, state: dict) -> bool:
    station_id = PWS_STATION_ID
    api_key = PWS_API_KEY

    dt = datetime.fromtimestamp(c["obs_time_ms"] / 1000, tz=timezone.utc)
    dateutc = dt.strftime("%Y-%m-%d+%H:%M:%S")

    optional = {}
    if "temp_f" in c:
        optional["tempf"] = c["temp_f"]
    if "dewpt_f" in c:
        optional["dewptf"] = c["dewpt_f"]
    if "wind_mph" in c:
        optional["windspeedmph"] = c["wind_mph"]
    if "gust_mph" in c:
        optional["windgustmph"] = c["gust_mph"]
    if "winddir" in c:
        optional["winddir"] = c["winddir"]
    if "pressure_inhg" in c:
        optional["baromin"] = c["pressure_inhg"]
    if "humidity" in c:
        optional["humidity"] = c["humidity"]
    if "uv" in c:
        optional["uv"] = c["uv"]
    if "solar_radiation" in c:
        optional["solarradiation"] = c["solar_radiation"]
    if "precip_last_hour_in" in c:
        optional["rainin"] = c["precip_last_hour_in"]
    if "precip_since_midnight_in" in c:
        optional["dailyrainin"] = c["precip_since_midnight_in"]

    url = (
        f"https://pwsupdate.pwsweather.com/api/v1/submitwx"
        f"?ID={station_id}&PASSWORD={api_key}&dateutc={dateutc}"
        f"&softwaretype=python-listener-v1&action=updateraw"
        f"&{urllib.parse.urlencode(optional)}"
    )
    response = http_get(url)
    log.info("PWSWeather response: %s", response.strip())

    state["last_pws_ts"] = int(time.time() * 1000)
    return True

def send_openweathermap(c: dict, state: dict) -> bool:
    api_key = OWM_API_KEY
    external_id = OWM_STATION_ID

    if "owm_internal_id" not in state:
        stations_url = f"https://api.openweathermap.org/data/3.0/stations?APPID={api_key}"
        stations = http_get_json(stations_url)
        match = next((s for s in stations if str(s.get("external_id")) == str(external_id)), None)
        if not match:
            log.error("OWM: station with external_id %s not found", external_id)
            return False
        state["owm_internal_id"] = match["id"]
        log.info("OWM: resolved internal station ID: %s", state["owm_internal_id"])

    measurement = {"station_id": state["owm_internal_id"]}
    measurement["dt"] = int(c["obs_time_ms"] / 1000)
    if "temp_c" in c:
        measurement["temperature"] = c["temp_c"]
    if "dewpt_c" in c:
        measurement["dew_point"] = c["dewpt_c"]
    if "wind_mps" in c:
        measurement["wind_speed"] = c["wind_mps"]
    if "gust_mps" in c:
        measurement["wind_gust"] = c["gust_mps"]
    if "winddir" in c:
        measurement["wind_deg"] = c["winddir"]
    if "pressure_hpa" in c:
        measurement["pressure"] = c["pressure_hpa"]
    if "humidity" in c:
        measurement["humidity"] = c["humidity"]
    if "precip_last_hour_mm" in c:
        measurement["rain_1h"] = c["precip_last_hour_mm"]

    payload = json.dumps([measurement]).encode()
    url = f"https://api.openweathermap.org/data/3.0/measurements?APPID={api_key}"
    status, body = http_post(url, payload, {"Content-Type": "application/json"})
    log.info("OWM response: %s %s", status, body.strip() or "(empty)")

    state["last_owm_ts"] = int(time.time() * 1000)
    return True

def send_windy(c: dict, state: dict) -> bool:
    params = {
        "id": WINDY_STATION_ID,
        "PASSWORD": WINDY_STATION_PASSWORD,
        "ts": int(c["obs_time_ms"] / 1000),
    }
    if "temp_c" in c:
        params["temp"] = c["temp_c"]
    if "dewpt_c" in c:
        params["dewpoint"] = c["dewpt_c"]
    if "wind_mps" in c:
        params["wind"] = c["wind_mps"]
    if "gust_mps" in c:
        params["gust"] = c["gust_mps"]
    if "winddir" in c:
        params["winddir"] = c["winddir"]
    if "humidity" in c:
        params["humidity"] = c["humidity"]
    if "pressure_hpa" in c:
        params["pressure"] = round(c["pressure_hpa"] * 100)  # hPa -> Pa
    if "uv" in c:
        params["uv"] = c["uv"]
    if "solar_radiation" in c:
        params["solarradiation"] = c["solar_radiation"]
    if "precip_since_midnight_mm" in c:
        params["precip"] = c["precip_since_midnight_mm"]
 
    url = "https://stations.windy.com/api/v2/observation/update?" + urllib.parse.urlencode(params)
    response = http_get(url)
    log.info("Windy response: %s", response.strip())
 
    state["last_windy_ts"] = int(time.time() * 1000)
    return True

def send_weathercloud(c: dict, state: dict) -> bool:
    dt = datetime.fromtimestamp(c["obs_time_ms"] / 1000, tz=timezone.utc)

    params = {
        "wid": WEATHERCLOUD_ID,
        "key": WEATHERCLOUD_KEY,
        "date": dt.strftime("%Y%m%d"),
        "time": dt.strftime("%H%M"),
        "software": "python-listener-v1",
    }
    # WeatherCloud expects most values as integers scaled by 10
    if "temp_c" in c:
        params["temp"] = round(c["temp_c"] * 10)
    if "dewpt_c" in c:
        params["dew"] = round(c["dewpt_c"] * 10)
    if "wind_mps" in c:
        params["wspd"] = round(c["wind_mps"] * 10)
    if "gust_mps" in c:
        params["wspdhi"] = round(c["gust_mps"] * 10)
    if "winddir" in c:
        params["wdir"] = c["winddir"]
    if "pressure_hpa" in c:
        params["bar"] = round(c["pressure_hpa"] * 10)
    if "humidity" in c:
        params["hum"] = c["humidity"]
    if "uv" in c:
        params["uvi"] = round(c["uv"] * 10)
    if "solar_radiation" in c:
        params["solarrad"] = round(c["solar_radiation"] * 10)
    if "precip_rate_mm" in c:
        params["rainrate"] = round(c["precip_rate_mm"] * 10)
    if "precip_since_midnight_mm" in c:
        params["rain"] = round(c["precip_since_midnight_mm"] * 10)

    url = "http://api.weathercloud.net/v01/set?" + urllib.parse.urlencode(params)
    response = http_get(url)
    log.info("WeatherCloud response: %s", response.strip())

    state["last_weathercloud_ts"] = int(time.time() * 1000)
    return True

# ---------------------------------------------------------------------------
# Healthchecks.io ping
# ---------------------------------------------------------------------------

def ping_healthcheck(success: bool) -> None:
    url = HEALTHCHECKS_URL
    if not url:
        return
    ping_url = url if success else url + "/fail"
    try:
        http_get(ping_url)
    except Exception as e:
        log.warning("Healthcheck ping failed: %s", e)

# ---------------------------------------------------------------------------
# Rate gate
# ---------------------------------------------------------------------------

def elapsed_ms(ts_ms: int) -> int:
    return int(time.time() * 1000) - ts_ms

CWOP_INTERVAL_MS = 5 * 60 * 1000
PWS_INTERVAL_MS  = 5 * 60 * 1000
OWM_INTERVAL_MS  = 1 * 60 * 1000
WINDY_INTERVAL_MS       = 5 * 60 * 1000
WEATHERCLOUD_INTERVAL_MS = 10 * 60 * 1000

# ---------------------------------------------------------------------------
# Core handler logic — called on every console push
# ---------------------------------------------------------------------------

def handle_push(query: dict) -> None:
    with STATE_LOCK:
        state = load_state()
        conditions = parse_console_push(query)

        log.info(
            "Push received: temp=%s°F wind=%s mph gust=%s mph dir=%s°",
            conditions.get("temp_f", "?"),
            conditions.get("wind_mph", "?"),
            conditions.get("gust_mph", "?"),
            conditions.get("winddir", "?"),
        )

        if "precip_rate_in" in conditions:
            precip_last_hour = update_precip_history(state, conditions["precip_rate_in"])
            if precip_last_hour is not None:
                conditions["precip_last_hour_in"] = precip_last_hour
                conditions["precip_last_hour_mm"] = round(in_to_mm(precip_last_hour), 2)
        else:
            update_precip_history(state, 0.0)

        success = True

        try:
            send_wunderground(conditions, state)
        except Exception as e:
            log.error("Wunderground send failed: %s", e)
            success = False

        if elapsed_ms(state["last_cwop_ts"]) >= CWOP_INTERVAL_MS:
            try:
                send_cwop(conditions, state)
            except Exception as e:
                log.error("CWOP send failed: %s", e)
                success = False

        if elapsed_ms(state["last_pws_ts"]) >= PWS_INTERVAL_MS:
            try:
                send_pwsweather(conditions, state)
            except Exception as e:
                log.error("PWSWeather send failed: %s", e)
                success = False

        if elapsed_ms(state["last_owm_ts"]) >= OWM_INTERVAL_MS:
            try:
                send_openweathermap(conditions, state)
            except Exception as e:
                log.error("OWM send failed: %s", e)
                success = False

        if WINDY_STATION_ID and WINDY_STATION_PASSWORD:
            if elapsed_ms(state["last_windy_ts"]) >= WINDY_INTERVAL_MS:
                try:
                    send_windy(conditions, state)
                except Exception as e:
                    log.error("Windy send failed: %s", e)
                    success = False

        if WEATHERCLOUD_ID and WEATHERCLOUD_KEY:
            if elapsed_ms(state["last_weathercloud_ts"]) >= WEATHERCLOUD_INTERVAL_MS:
                try:
                    send_weathercloud(conditions, state)
                except Exception as e:
                    log.error("WeatherCloud send failed: %s", e)
                    success = False

        save_state(state)
        ping_healthcheck(success)

# ---------------------------------------------------------------------------
# HTTP server
# ---------------------------------------------------------------------------

class PushHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # suppress default noisy access logging

    def do_GET(self):
        # Log exactly what the console sent
        log.info("Raw request: %s", self.path)

        parsed = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed.query)

        # Log the parsed query parameters
        log.info("Parsed query: %s", query)

        # Respond immediately so the console doesn't retry
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"success")

        try:
            handle_push(query)
        except Exception as e:
            log.exception("Error handling push")

def main():
    server = ThreadingHTTPServer(("0.0.0.0", LISTEN_PORT), PushHandler)
    log.info("Listening on 0.0.0.0:%d — waiting for console pushes...", LISTEN_PORT)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Shutting down.")

if __name__ == "__main__":
    main()
