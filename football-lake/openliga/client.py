"""HTTP client cho OpenLigaDB: rate limit cục bộ + retry/backoff (429/5xx/timeout)."""
import logging
import threading
import time
from urllib.parse import quote

import requests

from . import config

log = logging.getLogger("openliga.client")


class NotFound(Exception):
    pass


class OpenLigaClient:
    def __init__(self, base_url=None, max_per_min=None, max_retries=5, session=None):
        self.base = (base_url or config.BASE_URL).rstrip("/")
        self.min_interval = 60.0 / (max_per_min or config.MAX_REQ_PER_MIN)
        self.max_retries = max_retries
        self.s = session or requests.Session()
        self.s.headers.update({"User-Agent": config.USER_AGENT, "Accept": "application/json"})
        self._last = 0.0
        self._lock = threading.Lock()
        self.requests_made = 0

    # --- rate limit -------------------------------------------------------
    def _throttle(self):
        with self._lock:
            wait = self._last + self.min_interval - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()

    def get(self, path):
        url = f"{self.base}{path}"
        delay = 2.0
        for attempt in range(1, self.max_retries + 1):
            self._throttle()
            self.requests_made += 1
            try:
                r = self.s.get(url, timeout=(10, 60))
            except (requests.ConnectionError, requests.Timeout) as e:
                log.warning("GET %s lỗi mạng (%s) lần %d/%d", path, e, attempt, self.max_retries)
                time.sleep(delay); delay *= 2
                continue
            if r.status_code == 404:
                raise NotFound(path)
            if r.status_code == 429 or r.status_code >= 500:
                ra = r.headers.get("Retry-After")
                sleep_for = float(ra) if ra and ra.replace(".", "", 1).isdigit() else delay
                log.warning("GET %s -> %s, chờ %.0fs (lần %d/%d)", path, r.status_code, sleep_for,
                            attempt, self.max_retries)
                time.sleep(sleep_for); delay *= 2
                continue
            r.raise_for_status()
            if not r.content or r.content.strip() in (b"", b"null"):
                return None
            return r.json()
        raise RuntimeError(f"GET {path} thất bại sau {self.max_retries} lần")

    # --- endpoints --------------------------------------------------------
    def sports(self):                      return self.get("/getavailablesports")
    def result_types(self):                return self.get("/getresulttypes")
    def leagues(self, season):             return self.get(f"/getavailableleagues/{int(season)}")
    def result_infos(self, league_id):     return self.get(f"/getresultinfos/{int(league_id)}")

    def season_matches(self, shortcut, season):
        return self.get(f"/getmatchdata/{quote(shortcut, safe='')}/{int(season)}")

    def group_matches(self, shortcut, season, group_order):
        return self.get(f"/getmatchdata/{quote(shortcut, safe='')}/{int(season)}/{int(group_order)}")

    def last_change(self, shortcut, season, group_order):
        v = self.get(f"/getlastchangedate/{quote(shortcut, safe='')}/{int(season)}/{int(group_order)}")
        return None if v is None else str(v)

    def table(self, shortcut, season):
        return self.get(f"/getbltable/{quote(shortcut, safe='')}/{int(season)}")

    def available_teams(self, shortcut, season):
        return self.get(f"/getavailableteams/{quote(shortcut, safe='')}/{int(season)}")

    def available_groups(self, shortcut, season):
        return self.get(f"/getavailablegroups/{quote(shortcut, safe='')}/{int(season)}")

    def goal_getters(self, shortcut, season):
        return self.get(f"/getgoalgetters/{quote(shortcut, safe='')}/{int(season)}")
