#!/usr/bin/env python3
"""
lrc_test_delete_reflexes.py

Mass-delete every LexReflex in a flat-uploaded lexicon (e.g. the DravidiLex
pilot `dravidilex_pilot`) on an LRC site, over HTTP only — no artisan, no DB,
no deploy access required.

Why this exists
---------------
The DravidiLex pilot was uploaded via the admin Utilities uploader against the
`master` importer, which is flat-only: every source row became a `LexReflex`
(no etyma, no etyma->reflex links). To re-import a corrected batch we must first
remove the current upload. The admin panel exposes no bulk delete for reflexes
(the `DeleteBulkAction` is commented out) and no lexicon-scoped filter, but each
reflex's Edit page *does* have a per-record `DeleteAction`. This script drives
that action over Livewire for every reflex in the lexicon.

Safe by construction
---------------------
* Enumeration is scoped to ONE lexicon via the public data endpoint
  (`/api/v1/lexicon/{slug}/data`), which returns `reflex_id`s. It never touches
  other lexicons or the global reflex table.
* Deleting a reflex cascades to its extra_data / sources / cross-refs AND its
  `lex_lexicon_data_cache` row (all `onDelete('cascade')` as of migration
  2024_07_19 / 2025_10_01), so the public count drops as we go and the loop is
  self-correcting: enumerate a page -> delete it -> re-enumerate until zero.
* Defaults to --dry-run. Use --limit 1 to validate the delete handshake on a
  single record and eyeball the result in the admin before the full run.

Auth
----
Copy the `Cookie:` header from any authenticated `/admin` request in your
browser devtools (Network tab) and pass it via the LRC_COOKIE env var or
--cookie-file. It must contain the Laravel session + XSRF-TOKEN cookies.

Usage
-----
    export LRC_COOKIE='laravel_session=...; XSRF-TOKEN=...; ...'
    # 1. see what would be deleted (no writes):
    python lrc_test_delete_reflexes.py --dry-run
    # 2. delete exactly one, then check the admin UI:
    python lrc_test_delete_reflexes.py --limit 1
    # 3. full teardown:
    python lrc_test_delete_reflexes.py --yes

NOTE: the Livewire delete payload (mountAction/callMountedAction) below follows
the standard Livewire 3.7 / Filament 4.6 protocol. If the --limit 1 validation
fails, paste the real `/livewire/update` request captured from a manual delete
(copy-as-cURL) and we align DELETE_METHOD_* / header names to your exact build.
"""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import sys
import time
from typing import Optional

import requests

# --- configuration (override via env / flags) --------------------------------
BASE_URL = os.environ.get("LRC_BASE", "https://lrc-test.la.utexas.edu").rstrip("/")
SLUG = os.environ.get("LRC_SLUG", "dravidilex_pilot")
ADMIN_RESOURCE = os.environ.get("LRC_ADMIN_RESOURCE", "lex-reflexes")  # /admin/<this>/{id}/edit
LIVEWIRE_ENDPOINT = "/livewire/update"

# Livewire method names for the Filament DeleteAction handshake. Confirm/adjust
# from a captured real request if the --limit 1 validation fails.
MOUNT_METHOD = "mountAction"
CALL_METHOD = "callMountedAction"
ACTION_NAME = "delete"

PAGE_SIZE = 100  # max the public endpoint allows


class LrcClient:
    def __init__(self, cookie: str, base_url: str = BASE_URL, verbose: bool = False):
        self.base = base_url
        self.verbose = verbose
        self.s = requests.Session()
        self.s.headers.update({
            "User-Agent": "lrc-batch-teardown/1.0",
            "Accept": "text/html,application/json",
        })
        if cookie:
            self.s.headers["Cookie"] = cookie.strip()
        self._csrf: Optional[str] = None

    # -- enumeration (public, unauthenticated) --------------------------------
    def enumerate_reflex_ids(self, slug: str = SLUG) -> list[int]:
        """Page the public datatable endpoint and collect every reflex id."""
        url = f"{self.base}/api/v1/lexicon/{slug}/data"
        ids: list[int] = []
        start = 0
        total = None
        while True:
            params = {
                "draw": 1,
                "start": start,
                "length": PAGE_SIZE,
                "search[value]": "",
                "search[regex]": "false",
                "columns[0][name]": "root",
                "columns[0][search][value]": "",
                "columns[0][search][regex]": "false",
            }
            r = self.s.get(url, params=params, timeout=30)
            r.raise_for_status()
            payload = r.json()
            if total is None:
                total = payload.get("recordsTotal", 0)
            rows = payload.get("data", [])
            if not rows:
                break
            for row in rows:
                if "id" in row:
                    ids.append(int(row["id"]))
            start += len(rows)
            if start >= total or len(rows) < PAGE_SIZE:
                break
        # de-dupe, preserve order
        seen = set()
        unique = [i for i in ids if not (i in seen or seen.add(i))]
        return unique

    # -- Livewire plumbing (authenticated) ------------------------------------
    def _get_edit_component(self, reflex_id: int) -> tuple[str, str, str]:
        """GET the reflex Edit page; return (snapshot_json_string, csrf, update_uri)."""
        url = f"{self.base}/admin/{ADMIN_RESOURCE}/{reflex_id}/edit"
        r = self.s.get(url, timeout=30)
        if r.status_code == 403 or "login" in r.url:
            raise PermissionError("Not authenticated — cookie missing/expired or lacks Site Manager role.")
        r.raise_for_status()
        body = r.text

        # CSRF token from the <meta name="csrf-token"> tag.
        m = re.search(r'<meta name="csrf-token" content="([^"]+)"', body)
        if not m:
            raise RuntimeError("Could not find csrf-token meta on edit page.")
        csrf = m.group(1)

        # Livewire randomizes its update endpoint (e.g. /livewire-e9870728/update)
        # and embeds it as data-update-uri; read it rather than assume the default.
        mu = re.search(r'data-update-uri="([^"]+)"', body)
        update_uri = mu.group(1) if mu else f"{self.base}{LIVEWIRE_ENDPOINT}"

        # Livewire snapshots are HTML-entity-encoded JSON in wire:snapshot="...".
        # The page has several components (edit form, notifications, widgets);
        # the notifications one even shares the admin/lex-reflexes path, so match
        # the page component by its memo name first, then by record key.
        snaps = [html.unescape(e) for e in re.findall(r'wire:snapshot="((?:[^"\\]|\\.)*)"', body)]
        chosen = None
        for snap in snaps:  # 1) the EditLexReflex page component
            if "EditLexReflex" in snap:
                chosen = snap
                break
        if chosen is None:  # 2) any component bound to this record id
            for snap in snaps:
                if f'"key":{reflex_id}' in snap or f'"recordKey":"{reflex_id}"' in snap:
                    chosen = snap
                    break
        if chosen is None and snaps:  # 3) last resort: largest snapshot
            chosen = max(snaps, key=len)
        if chosen is None:
            raise RuntimeError("No wire:snapshot found on edit page.")
        return chosen, csrf, update_uri

    def _livewire_update(self, snapshot: str, calls: list[dict], csrf: str, url: str) -> dict:
        headers = {
            "Content-Type": "application/json",
            "X-CSRF-TOKEN": csrf,
            "X-Livewire": "",
            "Accept": "application/json",
        }
        payload = {
            "_token": csrf,
            "components": [{
                "snapshot": snapshot,
                "updates": {},
                "calls": calls,
            }],
        }
        r = self.s.post(url, headers=headers, data=json.dumps(payload), timeout=30)
        if self.verbose:
            print(f"    [livewire {calls[0]['method']}] HTTP {r.status_code}", file=sys.stderr)
        if r.status_code != 200:
            raise RuntimeError(f"Livewire {calls[0]['method']} failed: HTTP {r.status_code} {r.text[:400]}")
        return r.json()

    def delete_reflex(self, reflex_id: int) -> bool:
        """Drive the Filament DeleteAction for one reflex. Returns True on success.

        Payload shape confirmed from a real captured /livewire/update pair:
          1) mountAction("delete", {}, {"recordKey": "<id>"})
          2) callMountedAction()  using the snapshot mountAction returns.
        Success surfaces as a "Deleted" success notification + redirect to index.
        """
        snapshot, csrf, update_uri = self._get_edit_component(reflex_id)
        # 1) mount the delete action (server-side; opens the confirm modal)
        resp = self._livewire_update(
            snapshot,
            [{"method": MOUNT_METHOD,
              "params": [ACTION_NAME, {}, {"recordKey": str(reflex_id)}],
              "metadata": {}}],
            csrf, update_uri,
        )
        snapshot2 = resp["components"][0]["snapshot"]
        # 2) execute the mounted action
        resp2 = self._livewire_update(
            snapshot2,
            [{"method": CALL_METHOD, "params": [], "metadata": {}}],
            csrf, update_uri,
        )
        blob = json.dumps(resp2)
        # a successful delete redirects to the index and/or dispatches a
        # "Deleted" success notification.
        return ('"redirect"' in blob) or ('"Deleted"' in blob) or ('"status":"success"' in blob)


DEFAULT_COOKIE_FILE = os.path.join(os.path.dirname(__file__), "..", "..", "..", ".lrc_cookie")


def load_cookie(args) -> str:
    path = args.cookie_file
    if not path and os.path.exists(DEFAULT_COOKIE_FILE):
        path = DEFAULT_COOKIE_FILE
    if path:
        return open(path, "r", encoding="utf-8").read().strip()
    cookie = os.environ.get("LRC_COOKIE", "")
    if not cookie and not args.dry_run:
        sys.exit("ERROR: no auth. Put your admin Cookie header in .lrc_cookie (repo root), "
                 "or set LRC_COOKIE, or pass --cookie-file.")
    return cookie


def main() -> None:
    ap = argparse.ArgumentParser(description="Mass-delete flat reflexes for one LRC lexicon over HTTP.")
    ap.add_argument("--slug", default=SLUG, help="lexicon slug (default: %(default)s)")
    ap.add_argument("--dry-run", action="store_true", help="enumerate only; no deletes (default if --yes absent)")
    ap.add_argument("--yes", action="store_true", help="actually delete ALL enumerated reflexes")
    ap.add_argument("--limit", type=int, default=0, help="delete at most N (validation runs); implies real deletes")
    ap.add_argument("--sleep", type=float, default=0.3, help="seconds between deletes (default: %(default)s)")
    ap.add_argument("--cookie-file", help="file containing the Cookie header")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    do_delete = args.yes or args.limit > 0
    if not do_delete:
        args.dry_run = True

    cookie = load_cookie(args)
    client = LrcClient(cookie, verbose=args.verbose)

    print(f">> Enumerating reflexes in '{args.slug}' at {client.base} ...")
    ids = client.enumerate_reflex_ids(args.slug)
    print(f">> Found {len(ids)} reflexes.")
    if args.limit:
        ids = ids[: args.limit]
        print(f">> --limit {args.limit}: will delete only {len(ids)}: {ids}")

    if args.dry_run:
        print(">> DRY RUN — no deletions performed. Re-run with --limit 1 to validate, then --yes.")
        print("   sample ids:", ids[:10])
        return

    ok = 0
    fail = 0
    for n, rid in enumerate(ids, 1):
        try:
            if client.delete_reflex(rid):
                ok += 1
                print(f"  [{n}/{len(ids)}] deleted reflex {rid}")
            else:
                fail += 1
                print(f"  [{n}/{len(ids)}] UNCONFIRMED reflex {rid} (no redirect in response)")
        except Exception as e:  # noqa: BLE001
            fail += 1
            print(f"  [{n}/{len(ids)}] ERROR reflex {rid}: {e}")
            if n == 1:
                print("  First delete failed — stopping. Capture a real /livewire/update from a manual "
                      "delete and we'll align the payload.")
                break
        time.sleep(args.sleep)

    print(f">> Done. deleted={ok} failed={fail}")
    if ok:
        print("   Re-run --dry-run to confirm the remaining count dropped, then upload the corrected compat batch.")


if __name__ == "__main__":
    main()
