> [!WARNING]
> DISCLAIMER: The scripts and links provided on this GitHub page are for informational and educational purposes only. I do not claim responsibility for any issues, damages, or losses that may arise from the use of these scripts or links. Users are advised to use them at their own risk and discretion. Always review and test any code or links before implementing them in your projects.

All streams are from a FREE publicly available source no DRM content. No login required and no commercials cut from streams. I would suggest visit [Xuno](https://play.xumo.com/live-guide/alien-nation-by-dust) or download the app for more content like (Movies Ondemand).

# Xumo M3U Playlist and EPG Generator

This repository automatically generates an M3U playlist and XMLTV EPG file for Xumo TV channels available in the US.

- M3U: https://raw.githubusercontent.com/BuddyChewChew/xumo-playlist-generator/refs/heads/main/playlists/xumo_playlist.m3u

![Screenshot](https://github.com/BuddyChewChew/xumo-playlist-generator/blob/main/Screenshot%202025-04-16%20202330.jpg?raw=true)




## How to Use

The generated files are located in the `playlists/` directory.

*   **Playlist URL:**
    `https://raw.githubusercontent.com/BuddyChewChew/xumo-playlist-generator/main/playlists/xumo_playlist.m3u`
    
    
    *(This URL already includes the correct EPG link via `url-tvg`)*

    
*   **EPG URL:**
    `https://raw.githubusercontent.com/BuddyChewChew/xumo-playlist-generator/main/playlists/xumo_epg.xml.gz`
    

     (usually not needed separately if player supports `url-tvg`)

Add the **Playlist URL** to your IPTV player (TiviMate, OTT Navigator, IPTV Smarters, etc.). The player should automatically fetch the EPG using the link embedded in the playlist.

## Disclaimer

*   Data structures may change without notice, breaking the script.
*   Stream availability depends on Xumo. Placeholder replacement logic for stream URLs might need adjustments if Xumo changes requirements.
*   EPG data accuracy depends on the data returned by Xumo's EPG endpoint.
*   Use responsibly and according to Xumo's terms of service.
