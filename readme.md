# Xumo M3U Playlist and EPG Generator

This repository automatically generates an M3U playlist and XMLTV EPG file for Xumo TV channels available in the US.

## How It Works

1.  A GitHub Actions workflow runs every 8 hours.
2.  A Python script (`generate_xumo.py`) performs the following:
    *   Fetches dynamic `channelListId` and `geoId` from `xumo.tv`.
    *   Fetches the official master channel list using these IDs.
    *   Fetches "On Now / Next" data to identify currently live (`SIMULCAST`) channels and their current `assetId`.
    *   For each live channel, fetches the asset details using the `assetId` to retrieve the primary `master.m3u8` stream URL.
    *   Fetches EPG data for the next 24 hours (by default) for each live channel using the `/broadcast.json?hour=...` endpoint.
    *   Generates `playlists/xumo_playlist.m3u` (containing channels with valid stream URLs and an embedded `url-tvg` link).
    *   Generates `playlists/xumo_epg.xml.gz` (a gzipped XMLTV file with the fetched schedule).
3.  If the generated M3U or EPG files have changed, the workflow commits and pushes them back to this repository.

## How to Use

The generated files are located in the `playlists/` directory.

*   **Playlist URL:**
    ```
    https://raw.githubusercontent.com/BuddyChewChew/xumo-playlist-generator/main/playlists/xumo_playlist.m3u
    ```
    *(This URL already includes the correct EPG link via `url-tvg`)*
*   **EPG URL (usually not needed separately if player supports `url-tvg`):**
    ```
    https://raw.githubusercontent.com/BuddyChewChew/xumo-playlist-generator/main/playlists/xumo_epg.xml.gz
    ```

Add the **Playlist URL** to your IPTV player (TiviMate, OTT Navigator, IPTV Smarters, etc.). The player should automatically fetch the EPG using the link embedded in the playlist.

## Disclaimer

*   Uses official Xumo APIs. API endpoints or data structures may change without notice, breaking the script.
*   Stream availability depends on Xumo.
*   EPG data accuracy depends on the data returned by Xumo's `/broadcast.json` endpoint.
*   Use responsibly and according to Xumo's terms of service.
