"""
Open-Meteo weather API fetcher.

Open-Meteo is completely free and requires no API key.
Docs: https://open-meteo.com/en/docs
Geocoding: https://open-meteo.com/en/docs/geocoding-api
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Generator

import httpx

import config as cfg
from models import WeatherRecord
from utils import rate_limit, retry

log = logging.getLogger(__name__)

WEATHER_URL = cfg.weather.base_url + "/forecast"
GEO_URL = cfg.weather.geocoding_url + "/search"

# WMO Weather interpretation codes → human-readable label
WMO_CODES: dict[int, str] = {
    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Depositing rime fog",
    51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
    61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow",
    77: "Snow grains",
    80: "Slight showers", 81: "Moderate showers", 82: "Violent showers",
    85: "Slight snow showers", 86: "Heavy snow showers",
    95: "Thunderstorm", 96: "Thunderstorm w/ hail", 99: "Thunderstorm w/ heavy hail",
}


# ---------------------------------------------------------------------------
# Geocoding
# ---------------------------------------------------------------------------

@retry()
@rate_limit(seconds=0.5)
def _geocode(client: httpx.Client, city: str) -> dict[str, Any] | None:
    """Resolve city name to lat/lon. Returns None if not found."""
    resp = client.get(GEO_URL, params={"name": city, "count": 1, "language": "en", "format": "json"}, timeout=10)
    resp.raise_for_status()
    results = resp.json().get("results", [])
    if not results:
        log.warning("Geocoding: city '%s' not found", city)
        return None
    return results[0]


# ---------------------------------------------------------------------------
# Weather fetch
# ---------------------------------------------------------------------------

@retry()
@rate_limit(seconds=0.5)
def _fetch_weather(
    client: httpx.Client,
    lat: float,
    lon: float,
    days_forecast: int,
    days_history: int,
) -> dict[str, Any]:
    """Fetch hourly weather data for the given coordinates."""
    params: dict[str, Any] = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,apparent_temperature,precipitation,wind_speed_10m,wind_direction_10m,weather_code",
        "wind_speed_unit": "kmh",
        "timezone": "UTC",
    }

    # Forecast
    if days_forecast > 0:
        params["forecast_days"] = days_forecast

    # Historical (past days)
    if days_history > 0:
        end = datetime.utcnow().date()
        start = end - timedelta(days=days_history)
        params["start_date"] = start.isoformat()
        params["end_date"] = end.isoformat()

    log.debug("Fetching weather for lat=%.4f lon=%.4f", lat, lon)
    resp = client.get(WEATHER_URL, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def _parse_hourly(city: str, geo: dict[str, Any], data: dict[str, Any]) -> list[WeatherRecord]:
    """Convert raw Open-Meteo hourly response to a list of WeatherRecord."""
    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    records: list[WeatherRecord] = []

    now = datetime.utcnow()

    for i, t in enumerate(times):
        try:
            obs_time = datetime.fromisoformat(t)
        except ValueError:
            continue

        record = WeatherRecord(
            city=city,
            latitude=geo["latitude"],
            longitude=geo["longitude"],
            observation_time=obs_time,
            temperature_c=hourly.get("temperature_2m", [None])[i] if i < len(hourly.get("temperature_2m", [])) else None,
            apparent_temperature_c=hourly.get("apparent_temperature", [None])[i] if i < len(hourly.get("apparent_temperature", [])) else None,
            precipitation_mm=hourly.get("precipitation", [None])[i] if i < len(hourly.get("precipitation", [])) else None,
            wind_speed_kmh=hourly.get("wind_speed_10m", [None])[i] if i < len(hourly.get("wind_speed_10m", [])) else None,
            wind_direction_deg=hourly.get("wind_direction_10m", [None])[i] if i < len(hourly.get("wind_direction_10m", [])) else None,
            weather_code=hourly.get("weather_code", [None])[i] if i < len(hourly.get("weather_code", [])) else None,
            is_forecast=obs_time > now,
        )
        records.append(record)

    return records


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch(
    cities: list[str] | None = None,
    days_forecast: int | None = None,
    days_history: int | None = None,
) -> Generator[WeatherRecord, None, None]:
    """
    Fetch weather data for a list of cities.

    Args:
        cities: List of city names. Defaults to config value.
        days_forecast: Number of forecast days. Defaults to config value.
        days_history: Number of historical days. Defaults to config value.

    Yields:
        WeatherRecord instances (one per city per hour).
    """
    city_list = cities or cfg.weather.cities
    fcast = days_forecast if days_forecast is not None else cfg.weather.days_forecast
    hist = days_history if days_history is not None else cfg.weather.days_history

    log.info("Weather: fetching %d cities, %d forecast days, %d history days", len(city_list), fcast, hist)
    total = 0

    with httpx.Client(headers={"User-Agent": "data-pipeline-demo/1.0"}) as client:
        for city in city_list:
            geo = _geocode(client, city)
            if geo is None:
                continue

            try:
                raw = _fetch_weather(client, geo["latitude"], geo["longitude"], fcast, hist)
            except Exception as exc:
                log.error("Weather fetch failed for '%s': %s", city, exc)
                continue

            records = _parse_hourly(city, geo, raw)
            for rec in records:
                yield rec
                total += 1

    log.info("Weather: fetched %d records across %d cities", total, len(city_list))
