#!/usr/bin/env python3
"""
Radio playlist scraper → YouTube Music playlist creator.

Reads url_list.txt (onlineradiobox.com playlist URLs),
scrapes songs played in the last 24h per station, ranks each station's
songs by play count, then creates one public YouTube Music playlist per station.

Setup (one-time):
    pip install requests beautifulsoup4 ytmusicapi
    ytmusicapi browser --file browser.json
"""

import json
import logging
import os
import random
import sys
import time
from collections import defaultdict
from datetime import datetime

import requests
from bs4 import BeautifulSoup


URL_FILE = "url_list.txt"
CONFIG_FILE = "radio_config.json"
LOG_FILE = "radio_playlist.log"
# onlineradiobox.com rate-limits aggressive scrapers; randomised delays keep us under the radar
REQUEST_DELAY_MIN = 8.0
REQUEST_DELAY_MAX = 14.0


def read_config(path: str) -> dict:
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: {path} not found.", file=sys.stderr)
        sys.exit(1)


def read_urls(path: str) -> list[str]:
    with open(path) as f:
        return [line.strip() for line in f if line.strip() and line.strip().startswith("http")]


def fetch_playlist(url: str, session: requests.Session) -> tuple[str, list[tuple[str, str]]]:
    """
    Fetch one onlineradiobox.com playlist page.
    Returns (station_name, [(artist, title), ...]).
    Station name is taken from the page's <h1> with the trailing local word stripped.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        # Signals an AJAX-style request; required for the site to return full playlist HTML
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://onlineradiobox.com/",
    }
    try:
        resp = session.get(url, headers=headers, timeout=20)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  Warning: could not fetch {url}: {e}", file=sys.stderr)
        return _station_name_from_url(url), []

    soup = BeautifulSoup(resp.text, "html.parser")

    # Station name: strip the trailing "soittolista / playlist / spelllista / etc." word from h1
    station_name = _station_name_from_url(url)
    h1 = soup.find("h1")
    if h1:
        words = h1.get_text(strip=True).split()
        if len(words) > 1:
            station_name = " ".join(words[:-1])  # drop last word (local word for "playlist")

    # Disambiguate stations that share a name across countries (e.g. NRJ in SE and FR)
    if station_name.upper() == "NRJ":
        country = _country_from_url(url)
        if country:
            station_name = f"NRJ-{country}"

    # Build a set of tokens from the station name to detect jingles.
    # Use only the first hyphen-segment so "NRJ-SE" → {"nrj"} instead of {"nrj", "se"}.
    base_name = station_name.split("-")[0]
    station_tokens = {tok.lower() for tok in base_name.split() if len(tok) > 1}

    songs: list[tuple[str, str]] = []
    for cell in soup.select("td.track_history_item"):
        raw = cell.get_text(" ", strip=True)
        if " - " not in raw:
            continue
        artist, title = raw.split(" - ", 1)
        artist = artist.strip()
        title = title.strip()
        if not artist or not title:
            continue
        combined = (artist + " " + title).lower()
        if any(tok in combined for tok in station_tokens):
            continue
        songs.append((artist, title))

    return station_name, songs


def _station_name_from_url(url: str) -> str:
    """Fallback: extract and capitalise the station slug from the URL."""
    parts = [p for p in url.split("/") if p]
    # URL pattern: .../fi/suomipop/playlist/1 → slug is 2nd-to-last non-numeric segment
    slugs = [p for p in parts if not p.isdigit() and p not in ("playlist", "onlineradiobox.com")]
    return slugs[-1].capitalize() if slugs else "Unknown"


def _country_from_url(url: str) -> str:
    """Extract the two-letter country code from the URL path (e.g. 'se' from .../se/nrj/...)."""
    parts = [p for p in url.split("/") if p and p != "onlineradiobox.com" and ":" not in p]
    return parts[0].upper() if parts else ""


def rank_songs(songs: list[tuple[str, str]]) -> list[tuple[str, str, int]]:
    """
    Count plays per unique (artist, title) pair (case-insensitive key).
    Returns list of (artist, title, count) sorted by count descending.
    """
    counts: dict[tuple[str, str], int] = defaultdict(int)
    originals: dict[tuple[str, str], tuple[str, str]] = {}

    for artist, title in songs:
        key = (artist.lower(), title.lower())
        counts[key] += 1
        if key not in originals:
            originals[key] = (artist, title)

    ranked = sorted(counts.items(), key=lambda x: (-x[1], x[0][0], x[0][1]))
    return [(originals[k][0], originals[k][1], v) for k, v in ranked]


def yt_call(fn, *args, retries: int = 3, backoff: float = 15.0, **kwargs):
    """Call a ytmusicapi function, retrying on JSONDecodeError (rate-limit / empty response)."""
    for attempt in range(retries):
        try:
            return fn(*args, **kwargs)
        except json.JSONDecodeError:
            if attempt < retries - 1:
                wait = backoff * (attempt + 1)
                print(f"  Rate-limited, waiting {wait:.0f}s before retry {attempt + 1}/{retries - 1}...")
                time.sleep(wait)
            else:
                raise


def find_existing_playlist(yt, name: str) -> str | None:
    """Return playlist ID if a playlist with this exact name exists in the library."""
    playlists = yt_call(yt.get_library_playlists, limit=5000)
    for p in playlists:
        if p["title"] == name:
            return p["playlistId"]
    return None


def clear_playlist(yt, playlist_id: str) -> None:
    """Remove all tracks from an existing playlist."""
    playlist = yt_call(yt.get_playlist, playlist_id, limit=10000)
    tracks = playlist.get("tracks") or []
    if tracks:
        yt_call(yt.remove_playlist_items, playlist_id, tracks)
        print(f"  Cleared {len(tracks)} existing songs.")


def create_station_playlist(
    yt,
    station_name: str,
    ranked: list[tuple[str, str, int]],
    today: str,
) -> None:
    """Search each song on YouTube Music and add to a new or existing playlist for this station."""
    desc = f"Most played songs on {station_name} — {len(ranked)} songs, updated {today}, ordered by number of plays."

    existing_id = find_existing_playlist(yt, station_name)
    if existing_id:
        print(f"\nUpdating existing playlist: {station_name!r}")
        yt_call(yt.edit_playlist, existing_id, description=desc)
        clear_playlist(yt, existing_id)
        playlist_id = existing_id
    else:
        print(f"\nCreating playlist: {station_name!r}")
        result = yt_call(yt.create_playlist, station_name, desc, privacy_status="PUBLIC")
        if isinstance(result, str):
            playlist_id = result
        else:
            # 409 conflict or unexpected response — playlist likely exists but wasn't found
            print(f"  create_playlist returned unexpected response: {result!r}. Searching again...")
            playlist_id = find_existing_playlist(yt, station_name)
            if playlist_id is None:
                print(f"  Could not create or find playlist {station_name!r}. Skipping.", file=sys.stderr)
                return
            print(f"  Found existing playlist on retry.")
            yt_call(yt.edit_playlist, playlist_id, description=desc)
            clear_playlist(yt, playlist_id)
    print(f"  URL: https://music.youtube.com/playlist?list={playlist_id}")

    video_ids: list[str] = []
    not_found: list[str] = []

    print(f"  Searching {len(ranked)} songs...")
    for artist, title, count in ranked:
        query = f"{artist} {title}"
        results = None
        try:
            results = yt_call(yt.search, query, filter="songs", limit=1)
        except Exception as e:
            print(f"  [{count:2}x] {artist} - {title}  [SEARCH ERROR: {e}]")
        if results:
            video_ids.append(results[0]["videoId"])
            found_title = results[0].get("title", "?")
            found_artist = (results[0].get("artists") or [{}])[0].get("name", "?")
            print(f"  [{count:2}x] {artist} - {title} → {found_artist} - {found_title}")
        elif results is not None:
            print(f"  [{count:2}x] {artist} - {title}  [NOT FOUND]")
            not_found.append(f"{artist} - {title}")
        time.sleep(0.5)

    if video_ids:
        for i in range(0, len(video_ids), 200):
            yt_call(yt.add_playlist_items, playlist_id, video_ids[i : i + 200])
        print(f"  Added {len(video_ids)} songs.")
    else:
        print("  No songs found on YouTube Music.")

    if not_found:
        print(f"  {len(not_found)} not found: {', '.join(not_found[:5])}" +
              (" ..." if len(not_found) > 5 else ""))


def notify_telegram(msg: str, config: dict) -> None:
    token = config.get("telegram_bot_token") or os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = config.get("telegram_chat_id") or os.environ.get("TELEGRAM_CHAT_ID")
    if token and chat_id:
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": msg},
                timeout=10,
            )
            if resp.status_code != 200:
                print(f"Warning: Telegram API failed: {resp.status_code} {resp.text[:200]}", file=sys.stderr)
        except Exception as e:
            print(f"Warning: Telegram notify failed: {e}", file=sys.stderr)
    else:
        print(f"[Notification] {msg}")


def format_top10_message(station_name: str, ranked: list[tuple[str, str, int]], today: str) -> str:
    lines = [f"📻 {station_name} — top 10 ({today})"]
    for i, (artist, title, count) in enumerate(ranked[:10], 1):
        lines.append(f"{i:2}. [{count:2}x] {artist} - {title}")
    return "\n".join(lines)


class _Tee:
    """Write to both a file and the original stream."""
    def __init__(self, stream, filepath):
        self._stream = stream
        self._file = open(filepath, "a", encoding="utf-8")

    def write(self, data):
        self._stream.write(data)
        self._file.write(data)

    def flush(self):
        self._stream.flush()
        self._file.flush()

    def fileno(self):
        return self._stream.fileno()


def main() -> None:
    log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), LOG_FILE)
    sys.stdout = _Tee(sys.stdout, log_path)
    sys.stderr = _Tee(sys.stderr, log_path)
    print(f"\n{'='*60}")
    print(f"Run started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    config = read_config(CONFIG_FILE)

    try:
        urls = read_urls(URL_FILE)
    except FileNotFoundError:
        print(f"Error: {URL_FILE} not found.", file=sys.stderr)
        sys.exit(1)

    if not urls:
        print(f"Error: no URLs found in {URL_FILE}.", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(urls)} station URLs")

    # Scrape each station separately
    session = requests.Session()
    stations: list[tuple[str, list[tuple[str, str]]]] = []

    for i, url in enumerate(urls):
        if i > 0:
            delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
            print(f"  (waiting {delay:.1f}s…)")
            time.sleep(delay)
        print(f"\nFetching: {url}")
        station_name, songs = fetch_playlist(url, session)
        print(f"  {station_name}: {len(songs)} entries")
        if not songs:
            print("  (no playlist data — station may not be tracked, or temporary block)")
        stations.append((station_name, songs))

    stations_with_data = [(name, songs) for name, songs in stations if songs]
    if not stations_with_data:
        print("\nNo songs scraped from any station.", file=sys.stderr)
        sys.exit(1)

    # Print summary per station
    today = datetime.now().strftime("%Y-%m-%d")
    print(f"\n{'='*60}")
    for station_name, songs in stations_with_data:
        ranked = rank_songs(songs)
        print(f"\n{station_name} — top 10 (of {len(ranked)} unique tracks):")
        for j, (artist, title, count) in enumerate(ranked[:10], 1):
            print(f"  {j:2}. [{count:2}x] {artist} - {title}")
        msg = format_top10_message(station_name, ranked, today)
        notify_telegram(msg, config)

    # Connect to YouTube Music once for all stations
    try:
        from ytmusicapi import YTMusic
    except ImportError:
        print("\nError: ytmusicapi not installed. Run: pip install ytmusicapi", file=sys.stderr)
        sys.exit(1)

    browser_file = config.get("browser_auth_file")
    try:
        yt = YTMusic(browser_file)
        print("Authenticated via browser credentials.")
    except Exception as e:
        print(f"\nError: Could not authenticate with YouTube Music: {e}", file=sys.stderr)
        print("Run: ytmusicapi browser --file browser.json", file=sys.stderr)
        sys.exit(1)

    # Create one playlist per station
    print(f"\n{'='*60}")
    for station_name, songs in stations_with_data:
        ranked = rank_songs(songs)
        create_station_playlist(yt, station_name, ranked, today)

    print("\nDone!")


if __name__ == "__main__":
    main()
