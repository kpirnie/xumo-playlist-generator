# Xumo M3U Playlist and EPG Generator

This repository automatically generates an M3U playlist and XMLTV EPG file for Xumo TV channels available in the US.

## How to Use

The generated files are located in the `playlists/` directory.

*   **Playlist URL:**
    `https://raw.githubusercontent.com/BuddyChewChew/xumo-playlist-generator/main/playlists/xumo_playlist.m3u`
    
    
    *(This URL already includes the correct EPG link via `url-tvg`)*

    
*   **EPG URL:**https://raw.githubusercontent.com/BuddyChewChew/xumo-playlist-generator/main/playlists/xumo_epg.xml.gz`

     (usually not needed separately if player supports `url-tvg`)

Add the **Playlist URL** to your IPTV player (TiviMate, OTT Navigator, IPTV Smarters, etc.). The player should automatically fetch the EPG using the link embedded in the playlist.

## Disclaimer

*   Uses official Xumo APIs identified through community efforts. API endpoints or data structures may change without notice, breaking the script.
*   Stream availability depends on Xumo. Placeholder replacement logic for stream URLs might need adjustments if Xumo changes requirements.
*   EPG data accuracy depends on the data returned by Xumo's EPG endpoint.
*   Use responsibly and according to Xumo's terms of service.
