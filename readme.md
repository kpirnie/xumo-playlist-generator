# Xumo M3U Playlist and EPG Generator

This repository automatically generates an M3U playlist and XMLTV EPG file for Xumo TV channels available in the US.

## How It Works

1.  A GitHub Actions workflow runs every 8 hours.
2.  A Python script (`generate_xumo.py`) fetches the official channel list and EPG data from Xumo's public APIs.
3.  It fetches a community-maintained M3U file from `iptv-org/iptv` to get working stream URLs.
4.  It combines the official channel/EPG data with the stream URLs, generating:
    *   `playlists/xumo_playlist.m3u`: The playlist file with `url-tvg` pointing to the EPG.
    *   `playlists/xumo_epg.xml.gz`: A gzipped XMLTV file containing the program schedule.
5.  If the generated files have changed, the workflow commits and pushes them back to this repository.

## How to Use

The generated files are located in the `playlists/` directory.

*   **Playlist URL:**
    ```
    https://raw.githubusercontent.com/BuddyChewChew/xumo-playlist-generator/main/playlists/xumo_playlist.m3u
    ```
    *(This URL already includes the correct EPG link)*
*   **EPG URL (usually not needed separately if player supports `url-tvg`):**
    ```
    https://raw.githubusercontent.com/BuddyChewChew/xumo-playlist-generator/main/playlists/xumo_epg.xml.gz
    ```

Add the **Playlist URL** to your IPTV player (TiviMate, OTT Navigator, IPTV Smarters, etc.). The player should automatically fetch the EPG using the link embedded in the playlist.

## Disclaimer

*   Uses official Xumo APIs for channel/EPG data and stream URLs from the `iptv-org` project.
*   Stream availability and EPG accuracy depend on Xumo and the `iptv-org` community maintenance. Streams or APIs may change or break without notice.
*   Use responsibly and according to Xumo's terms of service.
