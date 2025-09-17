# filename: get_musicbrainz_ids.py
import argparse
import os
import time
import requests
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv

MB_BASE = "https://musicbrainz.org/ws/2/artist/"
UA = os.getenv("MUSICBRAINZ_UA", "mbid_to_lidarr/1.0 (you@example.com)")
REQUEST_INTERVAL_SECONDS = float(os.getenv("MB_REQUEST_INTERVAL_SECONDS", "1.0"))

# Lucene metacharacters to escape inside query strings
_LUCENE_META = set('+ - && || ! ( ) { } [ ] ^ " ~ * ? : \\ /'.split())

def lucene_escape(s: str) -> str:
    out = []
    for ch in s:
        if ch in _LUCENE_META:
            out.append("\\" + ch)
        else:
            out.append(ch)
    return "".join(out)

def build_query(artist_name: str) -> str:
    # exact-phrase search in the artist field, falling back to general text
    esc = lucene_escape(artist_name)
    return f'artist:"{esc}" OR "{esc}"'

def select_best(candidates: List[Dict[str, Any]], artist_name: str) -> Optional[Dict[str, Any]]:
    if not candidates:
        return None
    # Prefer exact (casefold) name match or alias, else best score
    an = artist_name.casefold()
    def is_exact(a):
        if a.get("name", "").casefold() == an:
            return True
        for al in a.get("aliases", []):
            if al.get("name", "").casefold() == an:
                return True
        return False

    exacts = [a for a in candidates if is_exact(a)]
    pool = exacts if exacts else candidates

    # Highest score first
    pool.sort(key=lambda a: a.get("score", 0), reverse=True)

    # Optional: nudge groups/people if you care:
    # pool.sort(key=lambda a: (a.get("type") in {"Group","Person"}, a.get("score",0)), reverse=True)

    # If the top score is too low, bail out (tune threshold as needed)
    top = pool[0]
    if top.get("score", 0) < 80:
        return None
    return top

def get_artist(session: requests.Session, artist_name: str) -> Optional[Dict[str, Any]]:
    params = {
        "query": build_query(artist_name),
        "fmt": "json",
        "limit": 5,            # fetch a few so we can pick the best
        "inc": "aliases"       # include aliases to help exact matching
    }
    while True:
        resp = session.get(MB_BASE, params=params, timeout=15)
        # Handle polite backoff
        if resp.status_code in (429, 503):
            retry_after = int(resp.headers.get("Retry-After", "2"))
            time.sleep(retry_after)
            continue
        resp.raise_for_status()
        data = resp.json()
        return select_best(data.get("artists", []), artist_name)

def parse_artists_file(input_path: str) -> List[str]:
    """Parse an input text file of artists and return a de-duplicated list.

    The file is expected to contain one artist name per line. Empty lines are ignored.
    """
    parsed_names: List[str] = []
    with open(input_path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue
            parsed_names.append(line)
    # De-duplicate while preserving order
    return list(dict.fromkeys(parsed_names))

def lidarr_tag(mbid: str) -> str:
    return f"lidarr:{mbid}"

def resolve_artists_to_mbids(
    session: requests.Session,
    artist_names: List[str],
    output_path: str,
    append: bool = False,
    min_interval_seconds: float = 1.0,
) -> List[str]:
    """Resolve a list of artist names to MBIDs, write lidarr:MBID lines, and return MBID list.

    Writes only unique MBIDs; will append if requested. Returns all MBIDs written this run.
    """
    results: List[Dict[str, Any]] = []
    written_mbids: set[str] = set()
    if append and os.path.exists(output_path):
        try:
            with open(output_path, "r", encoding="utf-8") as existing:
                for line in existing:
                    s = line.strip()
                    if s.startswith("lidarr:") and len(s) > 7:
                        written_mbids.add(s[7:])
        except OSError:
            pass
    last_call = 0.0
    new_mbids: List[str] = []
    mode = "a" if append else "w"
    try:
        dirpath = os.path.dirname(output_path)
        if dirpath:
            os.makedirs(dirpath, exist_ok=True)
        with open(output_path, mode, encoding="utf-8") as out_f:
            for name in dict.fromkeys(artist_names):
                elapsed = time.time() - last_call
                if elapsed < min_interval_seconds:
                    time.sleep(min_interval_seconds - elapsed)
                try:
                    artist = get_artist(session, name)
                except requests.RequestException as e:
                    print(f"{name}: ERROR {e}")
                    artist = None
                finally:
                    last_call = time.time()
                row = {
                    "input_artist": name,
                    "musicbrainz_id": artist.get("id") if artist else "",
                    "matched_name": artist.get("name") if artist else "",
                    "score": artist.get("score") if artist else "",
                    "type": artist.get("type") if artist else "",
                    "country": artist.get("country") if artist else "",
                    "disambiguation": artist.get("disambiguation") if artist else "",
                }
                print(f"{name}: {row['musicbrainz_id']} ({row['matched_name']} • {row['score']})")
                results.append(row)
                mbid = row["musicbrainz_id"]
                if mbid and mbid not in written_mbids:
                    out_f.write(lidarr_tag(mbid) + "\n")
                    out_f.flush()
                    written_mbids.add(mbid)
                    new_mbids.append(mbid)
    except KeyboardInterrupt:
        print("\nInterrupted. Progress saved to output file.")
    return new_mbids

def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Resolve MusicBrainz IDs for artist names.")
    parser.add_argument("input_path", nargs="?", default="artists.txt", help="Path to input text file of artist names (default: artists.txt)")
    parser.add_argument("-o", "--output", dest="output_path", default="output/mbids.txt", help="Path to output text file (default: output/mbids.txt)")
    parser.add_argument("--limit", type=int, default=0, help="Only process the first N artists (0 = all)")
    parser.add_argument("--append", action="store_true", help="Append to the output file (resume mode)")
    parser.add_argument("--interval", type=float, default=float(os.getenv("MB_REQUEST_INTERVAL_SECONDS", str(REQUEST_INTERVAL_SECONDS))), help="Minimum seconds between MusicBrainz requests")
    args = parser.parse_args()

    artist_names = parse_artists_file(args.input_path)
    if args.limit and args.limit > 0:
        artist_names = artist_names[:args.limit]
    if not artist_names:
        print(f"No artist names found in {args.input_path}.")
        return

    session = requests.Session()
    session.headers.update({"User-Agent": UA})

    resolve_artists_to_mbids(
        session=session,
        artist_names=artist_names,
        output_path=args.output_path,
        append=bool(args.append),
        min_interval_seconds=float(args.interval),
    )

if __name__ == "__main__":
    main()
