import argparse
from typing import Dict, Any, List, Optional, Set
import time
import requests
import os
from dotenv import load_dotenv

RETRY_STATUS = {429, 503}
MAX_RETRIES = 3
BASE_BACKOFF_SECONDS = 2.0

def request_with_retry(session: requests.Session, method: str, url: str, **kwargs) -> requests.Response:
    attempt = 0
    last_exc: Optional[Exception] = None
    last_retry_resp: Optional[requests.Response] = None
    while attempt <= MAX_RETRIES:
        try:
            resp = session.request(method, url, **kwargs)
            if resp.status_code in RETRY_STATUS:
                last_retry_resp = resp
                retry_after = resp.headers.get("Retry-After")
                if retry_after is not None:
                    try:
                        wait_s = float(retry_after)
                    except ValueError:
                        wait_s = BASE_BACKOFF_SECONDS * (2 ** attempt)
                else:
                    wait_s = BASE_BACKOFF_SECONDS * (2 ** attempt)
                time.sleep(wait_s)
                attempt += 1
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            last_exc = e
            if attempt >= MAX_RETRIES:
                break
            time.sleep(BASE_BACKOFF_SECONDS * (2 ** attempt))
            attempt += 1
    # Exhausted retries: raise the last retryable HTTP response if present, else the last exception
    if last_retry_resp is not None:
        last_retry_resp.raise_for_status()
    if last_exc is not None:
        raise last_exc
    raise requests.HTTPError(f"Request failed after {MAX_RETRIES} retries: {method} {url}")


def build_headers(api_key: str) -> Dict[str, str]:
    return {"X-Api-Key": api_key, "Content-Type": "application/json"}

def search_artist(session: requests.Session, base_url: str, headers: Dict[str, str], term: str) -> List[Dict[str, Any]]:
    url = f"{base_url}/api/v1/artist/lookup"
    params = {"term": term}
    response = request_with_retry(session, "GET", url, headers=headers, params=params, timeout=30)
    return response.json()

def add_artist(
    session: requests.Session,
    base_url: str,
    headers: Dict[str, str],
    artist: Dict[str, Any],
    quality_profile_id: int,
    metadata_profile_id: int,
    root_folder: str,
    monitor_option: str,
    search_missing: bool,
) -> Dict[str, Any]:
    url = f"{base_url}/api/v1/artist"
    payload = {
        "foreignArtistId": artist["foreignArtistId"],
        "artistName": artist.get("artistName", ""),
        "qualityProfileId": quality_profile_id,
        "metadataProfileId": metadata_profile_id,
        "images": artist.get("images", []),
        "monitored": True,
        "rootFolderPath": root_folder,
        "addOptions": {"monitor": monitor_option, "searchForMissingAlbums": search_missing},
        "tags": artist.get("tags", []),
    }
    response = request_with_retry(session, "POST", url, headers=headers, json=payload, timeout=30)
    return response.json()

def get_existing_foreign_ids(session: requests.Session, base_url: str, headers: Dict[str, str]) -> Set[str]:
    """Fetch all artists currently in Lidarr and return their foreignArtistId set."""
    url = f"{base_url}/api/v1/artist"
    resp = request_with_retry(session, "GET", url, headers=headers, timeout=30)
    try:
        items = resp.json()
    except ValueError:
        items = []
    existing: Set[str] = set()
    for it in items or []:
        fa = it.get("foreignArtistId")
        if fa:
            existing.add(fa)
    return existing

def get_root_folders(session: requests.Session, base_url: str, headers: Dict[str, str]) -> List[Dict[str, Any]]:
    url = f"{base_url}/api/v1/rootFolder"
    resp = request_with_retry(session, "GET", url, headers=headers, timeout=30)
    try:
        return resp.json() or []
    except ValueError:
        return []

def normalize_path(path: str) -> str:
    return path.rstrip("/")

def get_quality_profiles(session: requests.Session, base_url: str, headers: Dict[str, str]) -> List[Dict[str, Any]]:
    url = f"{base_url}/api/v1/qualityprofile"
    resp = request_with_retry(session, "GET", url, headers=headers, timeout=30)
    try:
        return resp.json() or []
    except ValueError:
        return []

def get_metadata_profiles(session: requests.Session, base_url: str, headers: Dict[str, str]) -> List[Dict[str, Any]]:
    url = f"{base_url}/api/v1/metadataprofile"
    resp = request_with_retry(session, "GET", url, headers=headers, timeout=30)
    try:
        return resp.json() or []
    except ValueError:
        return []

def parse_input_file(path: str) -> List[str]:
    mbids: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            s = raw.strip()
            if not s:
                continue
            if s.startswith("lidarr:"):
                s = s.split(":", 1)[1].strip()
            mbids.append(s)
    # de-dupe preserving order
    return list(dict.fromkeys(mbids))

def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Add/monitor artists in Lidarr from an MBID list.")
    parser.add_argument("--input", default="output/mbids.txt", help="Path to input file (lines of 'lidarr:<mbid>' or '<mbid>')")
    parser.add_argument("--root", default=os.getenv("ROOT_FOLDER", "/mnt/media/Music"), help="Root folder path in Lidarr")
    parser.add_argument("--quality-profile-id", type=int, default=0, help="Quality profile ID to use (0 = auto)")
    parser.add_argument("--metadata-profile-id", type=int, default=0, help="Metadata profile ID to use if lookup lacks one (0 = auto)")
    parser.add_argument("--limit", type=int, default=0, help="Process only first N entries")
    parser.add_argument("--dry-run", action="store_true", help="Lookup only; do not add")
    parser.add_argument("--report", default="output/lidarr_output.txt", help="Path to write a run report with statuses")
    parser.add_argument("--use-default-profiles", action="store_true", help="Fetch and use the first available quality/metadata profiles from Lidarr")
    parser.add_argument(
        "--monitor",
        default="all",
        choices=["all", "missing", "existing", "none", "future", "latest", "first"],
        help="Monitor option for addOptions.monitor (default: all)",
    )
    parser.add_argument(
        "--search-missing",
        action="store_true",
        help="If set, trigger searchForMissingAlbums on add (default: off)",
    )
    parser.add_argument("--lidarr-url", default=os.getenv("LIDARR_URL", "http://localhost:8686"), help="Base URL for Lidarr (no trailing slash)")
    parser.add_argument("--api-key", default=os.getenv("LIDARR_API_KEY", ""), help="Lidarr API key")
    args = parser.parse_args()

    mbids = parse_input_file(args.input)
    if args.limit and args.limit > 0:
        mbids = mbids[: args.limit]
    if not mbids:
        print("No MBIDs found in input file.")
        return

    base_url = args.lidarr_url.rstrip("/")
    api_key = args.api_key
    if not api_key:
        print("ERROR: Missing Lidarr API key. Set --api-key or LIDARR_API_KEY in .env")
        return
    headers = build_headers(api_key)
    session = requests.Session()
    # Validate root folder path against Lidarr configuration
    configured_roots = get_root_folders(session, base_url, headers)
    desired_root = normalize_path(args.root)
    available_roots = {normalize_path(r.get("path", "")) for r in configured_roots}
    if desired_root not in available_roots:
        print("ERROR: root folder not configured in Lidarr:")
        print(f"  requested: {args.root}")
        if available_roots:
            print("  available:")
            for p in sorted(available_roots):
                print(f"   - {p}")
        else:
            print("  (no root folders returned by Lidarr)")
        return
    # Resolve quality/metadata profile IDs
    effective_quality_id = args.quality_profile_id
    effective_metadata_id = args.metadata_profile_id
    if args.use_default_profiles or effective_quality_id <= 0 or effective_metadata_id <= 0:
        try:
            q_profiles = get_quality_profiles(session, base_url, headers)
        except requests.RequestException as e:
            q_profiles = []
        try:
            m_profiles = get_metadata_profiles(session, base_url, headers)
        except requests.RequestException as e:
            m_profiles = []
        def pick_default(profiles):
            if not profiles:
                return 0
            # Prefer a profile with 'Default' in the name; else first
            for p in profiles:
                if str(p.get("name", "")).lower().find("default") >= 0:
                    return int(p.get("id", 0) or 0)
            return int(profiles[0].get("id", 0) or 0)
        if effective_quality_id <= 0:
            effective_quality_id = pick_default(q_profiles)
        if effective_metadata_id <= 0:
            effective_metadata_id = pick_default(m_profiles)
    # Validate chosen profile IDs exist (best-effort)
    try:
        q_ids = {int(p.get("id")) for p in get_quality_profiles(session, base_url, headers)}
    except requests.RequestException:
        q_ids = set()
    try:
        m_ids = {int(p.get("id")) for p in get_metadata_profiles(session, base_url, headers)}
    except requests.RequestException:
        m_ids = set()
    if effective_quality_id <= 0 or (q_ids and effective_quality_id not in q_ids):
        print(f"ERROR: invalid quality profile id: {effective_quality_id}")
        if q_ids:
            print(f"  available quality profile ids: {sorted(q_ids)}")
        return
    if effective_metadata_id <= 0 or (m_ids and effective_metadata_id not in m_ids):
        print(f"ERROR: invalid metadata profile id: {effective_metadata_id}")
        if m_ids:
            print(f"  available metadata profile ids: {sorted(m_ids)}")
        return

    # Preload existing foreignArtistIds to avoid duplicate add attempts
    try:
        existing_ids = get_existing_foreign_ids(session, base_url, headers)
    except requests.RequestException as e:
        print(f"WARNING: could not load existing artists: {e}")
        existing_ids = set()
    stats = {"success": 0, "exists": 0, "lookup_error": 0, "add_error": 0, "dry_run": 0}

    # Fresh report each run
    try:
        os.makedirs(os.path.dirname(args.report), exist_ok=True)
        os.remove(args.report)
    except FileNotFoundError:
        pass

    def write_report_line(line: str) -> None:
        with open(args.report, "a", encoding="utf-8") as rf:
            rf.write(line + "\n")

    for mbid in mbids:
        # Skip if already present
        if mbid in existing_ids:
            write_report_line(f"{mbid}\tEXISTS\t-\tprecheck")
            stats["exists"] += 1
            print(f"{mbid}: already present (precheck)")
            continue
        term = f"lidarr:{mbid}"
        try:
            results = search_artist(session, base_url, headers, term)
        except requests.RequestException as e:
            msg = f"{mbid}\tLOOKUP_ERROR\t-\t{e}"
            print(f"{mbid}: LOOKUP ERROR {e}")
            write_report_line(msg)
            stats["lookup_error"] += 1
            continue
        if not results:
            msg = f"{mbid}\tNO_RESULTS\t-\tno lookup results"
            print(f"{mbid}: no lookup results")
            write_report_line(msg)
            stats["lookup_error"] += 1
            continue
        cand = None
        for r in results:
            if r.get("foreignArtistId") == mbid:
                cand = r
                break
        if cand is None:
            cand = results[0]
        name = cand.get("artistName", "<unknown>")
        dis = cand.get("disambiguation", "N/A")
        print(f"Found: {name} ({dis}) -> {mbid}")
        if args.dry_run:
            write_report_line(f"{mbid}\tDRY_RUN\t{name}\t-")
            stats["dry_run"] += 1
            continue
        # Prefer non-zero profile ids from lookup, else fallback to effective ids
        lookup_quality_id = int(cand.get("qualityProfileId") or 0)
        lookup_metadata_id = int(cand.get("metadataProfileId") or 0)
        chosen_quality_id = lookup_quality_id or effective_quality_id
        chosen_metadata_id = lookup_metadata_id or effective_metadata_id
        # Debug: print chosen profile IDs to help diagnose 400s
        print(f"Using profiles -> qualityProfileId={chosen_quality_id}, metadataProfileId={chosen_metadata_id}")
        try:
            added_item = add_artist(
                session,
                base_url,
                headers,
                cand,
                chosen_quality_id,
                chosen_metadata_id,
                args.root,
                args.monitor,
                args.search_missing,
            )
            print(f"Added: {added_item.get('artistName', name)}")
            write_report_line(f"{mbid}\tADDED\t{name}\t-")
            stats["success"] += 1
            existing_ids.add(mbid)
        except requests.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            try:
                detail = e.response.text
            except Exception:
                detail = str(e)
            # Re-check existence after error to avoid misclassifying validation errors
            try:
                # Refresh existing set minimally
                current_existing = get_existing_foreign_ids(session, base_url, headers)
            except requests.RequestException:
                current_existing = existing_ids
            if status in (400, 409) and mbid in current_existing:
                print(f"{name}: already exists (HTTP {status})")
                write_report_line(f"{mbid}\tEXISTS\t{name}\tHTTP {status}")
                stats["exists"] += 1
                existing_ids.add(mbid)
            else:
                print(f"{name}: ADD ERROR HTTP {status}: {detail}")
                write_report_line(f"{mbid}\tADD_ERROR\t{name}\tHTTP {status}: {detail}")
                stats["add_error"] += 1
        except requests.RequestException as e:
            print(f"{name}: ADD ERROR {e}")
            write_report_line(f"{mbid}\tADD_ERROR\t{name}\t{e}")
            stats["add_error"] += 1

    # Write summary
    summary = (
        f"SUMMARY\tADDED={stats['success']}\tEXISTS={stats['exists']}\t"
        f"LOOKUP_ERROR={stats['lookup_error']}\tADD_ERROR={stats['add_error']}\tDRY_RUN={stats['dry_run']}"
    )
    write_report_line(summary)

if __name__ == "__main__":
    main()
