"""Poll the OpenSky live state-vector API (Phase 2, streaming path entry point).

OpenSky changed in March 2026: basic auth is gone, it is OAuth2 client
credentials only. Tokens last 30 minutes, calls are rate-limited against a
credit budget, and you get 429s if you push too hard. This module handles all
three. Wiring the yielded records into Kafka is the remaining Phase 2 TODO
(see the docstring at the bottom).

Get client_id / client_secret from your OpenSky account page, set them as
OPENSKY_CLIENT_ID / OPENSKY_CLIENT_SECRET.
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass

import requests

TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network/"
    "protocol/openid-connect/token"
)
STATES_URL = "https://opensky-network.org/api/states/all"

# OpenSky state-vector array positions (see their REST docs).
FIELDS = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
    "spi", "position_source",
]


@dataclass
class _Token:
    value: str
    expires_at: float


class OpenSkyClient:
    def __init__(self, client_id: str | None = None, client_secret: str | None = None):
        self.client_id = client_id or os.environ["OPENSKY_CLIENT_ID"]
        self.client_secret = client_secret or os.environ["OPENSKY_CLIENT_SECRET"]
        self._token: _Token | None = None
        self._session = requests.Session()

    def _get_token(self) -> str:
        now = time.time()
        # Refresh 5 minutes before the 30-minute expiry.
        if self._token and now < self._token.expires_at - 300:
            return self._token.value
        resp = self._session.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=30,
        )
        resp.raise_for_status()
        body = resp.json()
        self._token = _Token(body["access_token"], now + body.get("expires_in", 1800))
        return self._token.value

    def get_states(self, bbox: tuple[float, float, float, float] | None = None,
                   max_retries: int = 4) -> list[dict]:
        """Fetch current state vectors, optionally within (lamin,lomin,lamax,lomax).

        Bounding the request to a region is the polite move: it costs fewer
        credits than the global feed and is plenty for a demo.
        """
        params: dict = {}
        if bbox:
            params = dict(zip(("lamin", "lomin", "lamax", "lomax"), bbox))

        for attempt in range(max_retries):
            resp = self._session.get(
                STATES_URL,
                params=params,
                headers={"Authorization": f"Bearer {self._get_token()}"},
                timeout=30,
            )
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 2 ** attempt))
                time.sleep(wait)
                continue
            resp.raise_for_status()
            payload = resp.json()
            ts = payload.get("time")
            states = payload.get("states") or []
            return [
                {**dict(zip(FIELDS, s)), "snapshot_time": ts}
                for s in states
            ]
        raise RuntimeError("rate limited: exhausted retries")


def poll_loop(bbox=None, interval_s: int = 15):
    """Yield batches of state vectors forever. interval_s respects rate limits."""
    client = OpenSkyClient()
    while True:
        batch = client.get_states(bbox=bbox)
        yield batch
        time.sleep(interval_s)


if __name__ == "__main__":
    # California-ish bounding box.
    CALIFORNIA = (32.5, -124.5, 42.0, -114.0)
    client = OpenSkyClient()
    rows = client.get_states(bbox=CALIFORNIA)
    print(f"{len(rows)} aircraft in view")
    for r in rows[:3]:
        print(r["callsign"], r["latitude"], r["longitude"], r["baro_altitude"])

# Phase 2 TODO: replace the print loop with a Kafka producer.
#   from kafka import KafkaProducer            # confluent-kafka also fine
#   producer = KafkaProducer(bootstrap_servers=..., value_serializer=json.dumps)
#   for batch in poll_loop(bbox=CALIFORNIA):
#       for rec in batch:
#           producer.send("opensky.states", rec)
# A separate consumer drains "opensky.states" and reuses snowflake_loader-style
# COPY logic to land RAW.OPENSKY_STATES. Kafka here buys you decoupling and
# replay, not throughput. Be ready to say that in the interview.
