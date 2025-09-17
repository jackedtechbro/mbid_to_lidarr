import argparse
import os
from typing import List
from dotenv import load_dotenv
import requests

from .get_musicbrainz_ids import parse_artists_file, resolve_artists_to_mbids
from .lidarr_add import main as lidarr_add_main


def run_bulk(
    artists_path: str,
    mbids_output: str,
    lidarr_root: str,
    lidarr_url: str,
    api_key: str,
    quality_profile_id: int,
    metadata_profile_id: int,
    monitor: str,
    search_missing: bool,
    report_path: str,
    limit: int,
    use_default_profiles: bool,
    mb_interval: float,
    mb_user_agent: str,
) -> None:
    session = requests.Session()
    session.headers.update({"User-Agent": mb_user_agent})

    artist_names: List[str] = parse_artists_file(artists_path)
    if limit and limit > 0:
        artist_names = artist_names[:limit]
    if not artist_names:
        print(f"No artist names found in {artists_path}.")
        return

    new_mbids = resolve_artists_to_mbids(
        session=session,
        artist_names=artist_names,
        output_path=mbids_output,
        append=False,
        min_interval_seconds=mb_interval,
    )

    # Chain into lidarr_add via CLI-style invocation by adjusting argv
    import sys
    saved_argv = sys.argv
    try:
        sys.argv = [
            "lidarr_add",
            "--input", mbids_output,
            "--root", lidarr_root,
            "--lidarr-url", lidarr_url,
            "--api-key", api_key,
            "--report", report_path,
            "--monitor", monitor,
        ] + (
            ["--search-missing"] if search_missing else []
        ) + (
            ["--use-default-profiles"] if use_default_profiles else []
        ) + (
            ["--quality-profile-id", str(quality_profile_id)] if quality_profile_id > 0 else []
        ) + (
            ["--metadata-profile-id", str(metadata_profile_id)] if metadata_profile_id > 0 else []
        )
        lidarr_add_main()
    finally:
        sys.argv = saved_argv


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Resolve MBIDs from artist list and add them to Lidarr in one step.")
    parser.add_argument("artists", nargs="?", default=os.getenv("ARTISTS_FILE", "artists.txt"), help="Path to input artist list (default: artists.txt)")
    parser.add_argument("--mbids-output", default=os.getenv("MBIDS_OUTPUT", "output/mbids.txt"), help="Output path for MBIDs (default: output/mbids.txt)")
    parser.add_argument("--lidarr-root", default=os.getenv("ROOT_FOLDER", "/mnt/media/Music"), help="Root folder path in Lidarr")
    parser.add_argument("--lidarr-url", default=os.getenv("LIDARR_URL", "http://localhost:8686"), help="Base URL for Lidarr")
    parser.add_argument("--api-key", default=os.getenv("LIDARR_API_KEY", ""), help="Lidarr API key")
    parser.add_argument("--quality-profile-id", type=int, default=int(os.getenv("QUALITY_PROFILE_ID", "0")), help="Quality profile ID (0 = auto)")
    parser.add_argument("--metadata-profile-id", type=int, default=int(os.getenv("METADATA_PROFILE_ID", "0")), help="Metadata profile ID (0 = auto)")
    parser.add_argument("--monitor", default=os.getenv("MONITOR_OPTION", "all"), choices=["all", "missing", "existing", "none", "future", "latest", "first"], help="Monitor option for addOptions.monitor")
    parser.add_argument("--search-missing", action="store_true", help="Search for missing albums after add")
    parser.add_argument("--report", default=os.getenv("LIDARR_REPORT", "output/lidarr_output.txt"), help="Report output path")
    parser.add_argument("--limit", type=int, default=int(os.getenv("LIMIT", "0")), help="Only process first N artists")
    parser.add_argument("--use-default-profiles", action="store_true", help="Use first/default quality and metadata profiles from Lidarr")
    parser.add_argument("--mb-interval", type=float, default=float(os.getenv("MB_REQUEST_INTERVAL_SECONDS", "1.0")), help="Minimum seconds between MusicBrainz requests")
    args = parser.parse_args()

    if not args.api_key:
        print("ERROR: Missing Lidarr API key. Set --api-key or LIDARR_API_KEY in .env")
        return

    # Ensure output directory exists for chosen outputs
    for p in [args.mbids_output, args.report]:
        d = os.path.dirname(p)
        if d:
            os.makedirs(d, exist_ok=True)

    run_bulk(
        artists_path=args.artists,
        mbids_output=args.mbids_output,
        lidarr_root=args.lidarr_root,
        lidarr_url=args.lidarr_url.rstrip("/"),
        api_key=args.api_key,
        quality_profile_id=args.quality_profile_id,
        metadata_profile_id=args.metadata_profile_id,
        monitor=args.monitor,
        search_missing=bool(args.search_missing),
        report_path=args.report,
        limit=int(args.limit),
        use_default_profiles=bool(args.use_default_profiles),
        mb_interval=float(args.mb_interval),
        mb_user_agent=os.getenv("MUSICBRAINZ_UA", "mbid_to_lidarr/1.0 (you@example.com)"),
    )


if __name__ == "__main__":
    main()


