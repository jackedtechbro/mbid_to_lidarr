import os
import argparse
from typing import Set, Any, Dict, Optional
import threading
import itertools
import sys
import time
from dotenv import load_dotenv
import spotipy  # type: ignore
from spotipy.oauth2 import SpotifyOAuth  # type: ignore

def get_env_var(key: str) -> str:
    value = os.getenv(key)
    if value is None:
        raise Exception(f"Missing {key} in .env.")
    return value

def get_repo_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))

def build_client(include_saved_albums: bool) -> Any:
    scope: str = "user-follow-read" + (" user-library-read" if include_saved_albums else "")
    spotify_client: Any = spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=get_env_var('SPOTIFY_CLIENT_ID'),
        client_secret=get_env_var('SPOTIFY_CLIENT_SECRET'),
        redirect_uri=get_env_var('SPOTIFY_REDIRECT_URI'),
        scope=scope,
        username=get_env_var('SPOTIFY_USERNAME')
    ))
    return spotify_client

def get_followed_artists(sp: Any) -> Set[str]:
    artists: Set[str] = set()
    results: Optional[Dict[str, Any]] = sp.current_user_followed_artists(limit=50)
    while results:
        for item in results['artists']['items']:
            artists.add(item['name'])
        if results['artists']['next']:
            results = sp.next(results['artists'])
        else:
            break
    return artists

def get_saved_albums_and_artists(sp: Any, artists: Set[str]) -> Set[str]:
    albums: Set[str] = set()
    album_results: Optional[Dict[str, Any]] = sp.current_user_saved_albums(limit=50)
    while album_results:
        for item in album_results['items']:
            album = item['album']
            albums.add(album['name'])
            for artist in album['artists']:
                artists.add(artist['name'])
        if album_results['next']:
            album_results = sp.next(album_results)
        else:
            break
    return albums

class Spinner:
    def __init__(self, message: str = "Processing") -> None:
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.message = message

    def start(self) -> None:
        def run() -> None:
            for c in itertools.cycle("|/-\\"):
                if self._stop.is_set():
                    break
                sys.stdout.write(f"\r{self.message}... {c}")
                sys.stdout.flush()
                time.sleep(0.1)
            sys.stdout.write("\r")
            sys.stdout.flush()

        self._thread = threading.Thread(target=run)
        self._thread.daemon = True
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join()


def main() -> None:
    load_dotenv()
    parser: argparse.ArgumentParser = argparse.ArgumentParser(description="Export Spotify artists to a file.")
    parser.add_argument('--dryrun', action='store_true', help='Only count and print, do not write to file.')
    parser.add_argument('--include-saved-albums', action='store_true', help='Also scan saved albums (requires user-library-read scope).')
    parser.add_argument('--out', default=os.getenv('ARTISTS_FILE', 'artists.txt'), help='Output path for artist list (default: artists.txt)')
    args: argparse.Namespace = parser.parse_args()

    sp: Any = build_client(include_saved_albums=bool(args.include_saved_albums))

    spinner = Spinner("Fetching Spotify data")
    try:
        spinner.start()
        artists: Set[str] = get_followed_artists(sp)
        albums: Set[str] = set()
        if args.include_saved_albums:
            albums = get_saved_albums_and_artists(sp, artists)
    finally:
        spinner.stop()

    if args.dryrun:
        print(f"[DRYRUN] Found {len(artists)} unique artists" + (f" and {len(albums)} unique albums" if args.include_saved_albums else "") + ".")
        return

    out_path: str = args.out
    with open(out_path, 'w', encoding='utf-8') as f:
        for artist in sorted(artists):
            f.write(f"{artist}\n")
    if args.include_saved_albums:
        print(f"Wrote {len(artists)} artists (and scanned {len(albums)} albums) to {out_path}")
    else:
        print(f"Wrote {len(artists)} artists to {out_path}")


if __name__ == "__main__":
    main()
