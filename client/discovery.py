"""
client/discovery.py  -  Broker discovery, leader caching, and HTTP communication helpers.
"""

from __future__ import annotations

import json
import logging
import random
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class LeaderDiscovery:
    """
    Discovers and caches the cluster leader address from a seed list of brokers.
    Reuses cached leader address until invalidated by network error, 307 redirect, or 503.
    """

    def __init__(self, seed_brokers: List[str], client_id: str = "client"):
        if not seed_brokers:
            raise ValueError("Seed brokers list cannot be empty")
        # Ensure clean host:port formatting without http:// prefix
        self.seeds = [s.replace("http://", "").replace("https://", "").strip() for s in seed_brokers if s.strip()]
        self.client_id = client_id
        self._cached_leader: Optional[str] = None
        self.metadata_call_count = 0

    @property
    def cached_leader(self) -> Optional[str]:
        return self._cached_leader

    def invalidate_cache(self, reason: str = "unspecified"):
        if self._cached_leader is not None:
            logger.info("[%s] Leader cache invalidated (%s): was %s", self.client_id, reason, self._cached_leader)
            self._cached_leader = None

    def set_leader(self, leader_address: str, source: str = "direct"):
        clean_addr = leader_address.replace("http://", "").replace("https://", "").strip()
        if clean_addr != self._cached_leader:
            logger.info("[%s] Leader cache updated (%s): %s", self.client_id, source, clean_addr)
            self._cached_leader = clean_addr

    def get_leader(self, max_attempts: int = 5, base_delay: float = 0.2) -> str:
        """
        Return cached leader, or query seed brokers until one returns the leader.
        Uses exponential backoff with jitter.
        """
        if self._cached_leader is not None:
            return self._cached_leader

        attempt = 0
        while attempt < max_attempts:
            attempt += 1
            # Shuffle seeds for random distribution
            candidate_seeds = list(self.seeds)
            random.shuffle(candidate_seeds)

            for seed in candidate_seeds:
                url = f"http://{seed}/metadata/leader"
                self.metadata_call_count += 1
                try:
                    req = urllib.request.Request(url, headers={"Connection": "close"})
                    with urllib.request.urlopen(req, timeout=3.0) as resp:
                        if resp.status == 200:
                            data = json.loads(resp.read().decode())
                            leader_addr = data.get("leaderAddress") or data.get("leader_address")
                            if leader_addr:
                                self.set_leader(leader_addr, source=f"metadata from {seed}")
                                return self._cached_leader
                except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError) as exc:
                    logger.debug("[%s] Failed querying %s: %s", self.client_id, url, exc)
                    continue

            # Exponential backoff with jitter
            delay = min(2.0, base_delay * (2 ** (attempt - 1))) + random.uniform(0.05, 0.2)
            logger.info("[%s] No leader found on attempt %d; sleeping %.2fs...", self.client_id, attempt, delay)
            time.sleep(delay)

        raise RuntimeError(f"[{self.client_id}] Failed to discover cluster leader after {max_attempts} attempts across seeds: {self.seeds}")

    def send_request(
        self,
        endpoint: str,
        method: str = "GET",
        body: Optional[Dict[str, Any]] = None,
        timeout: float = 6.0,
        max_retries: int = 5,
    ) -> Tuple[int, Dict[str, Any]]:
        """
        Send HTTP request to leader with automatic leader discovery, redirect following,
        cache invalidation, and exponential backoff retry.
        """
        attempt = 0
        payload_bytes = json.dumps(body).encode("utf-8") if body is not None else None

        while attempt < max_retries:
            attempt += 1
            try:
                leader = self.get_leader()
            except RuntimeError as exc:
                if attempt >= max_retries:
                    raise
                time.sleep(0.3)
                continue

            target_url = f"http://{leader}{endpoint}"
            req = urllib.request.Request(
                target_url,
                data=payload_bytes,
                headers={"Content-Type": "application/json", "Connection": "close"} if body is not None else {"Connection": "close"},
                method=method,
            )

            # Use custom opener that does not auto-follow redirects so we can inspect 307
            class NoRedirectHandler(urllib.request.HTTPRedirectHandler):
                def redirect_request(self, req, fp, code, msg, headers, newurl):
                    return None

            opener = urllib.request.build_opener(NoRedirectHandler)

            try:
                with opener.open(req, timeout=timeout) as resp:
                    resp_data = json.loads(resp.read().decode()) if resp.length != 0 else {}
                    return resp.status, resp_data

            except urllib.error.HTTPError as exc:
                status = exc.code
                try:
                    resp_data = json.loads(exc.read().decode())
                except Exception:
                    resp_data = {"error": exc.reason}

                # 307 Temporary Redirect: Follow leader address directly
                if status == 307:
                    new_leader = resp_data.get("leaderAddress") or resp_data.get("leader_address")
                    if not new_leader:
                        # Inspect Location header
                        loc = exc.headers.get("Location")
                        if loc:
                            new_leader = loc.replace("http://", "").split("/")[0]

                    if new_leader:
                        self.set_leader(new_leader, source="307 redirect")
                        logger.info("[%s] Redirected 307 -> %s", self.client_id, new_leader)
                        continue
                    else:
                        self.invalidate_cache("307 redirect with no leader address")

                elif status in (503, 504):
                    self.invalidate_cache(f"HTTP {status} {resp_data.get('error', '')}")

                elif "not the leader" in str(resp_data).lower():
                    new_leader = resp_data.get("leaderAddress") or resp_data.get("leader_address")
                    if new_leader:
                        self.set_leader(new_leader, source="not leader error body")
                        continue
                    self.invalidate_cache("not the leader error")

                else:
                    # Client errors (400, 404, etc.) are final
                    return status, resp_data

            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                self.invalidate_cache(f"connection error/timeout ({type(exc).__name__}: {exc})")

            # Backoff before retry
            if attempt < max_retries:
                delay = min(2.0, 0.2 * (2 ** (attempt - 1))) + random.uniform(0.05, 0.2)
                logger.info("[%s] Send attempt %d failed; retrying in %.2fs...", self.client_id, attempt, delay)
                time.sleep(delay)

        raise RuntimeError(f"[{self.client_id}] Request to {endpoint} exceeded max retries ({max_retries})")
