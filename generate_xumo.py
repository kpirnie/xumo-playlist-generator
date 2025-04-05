import requests
import json
import os
import gzip
from datetime import datetime, timedelta, timezone
import xml.etree.ElementTree as ET
import logging
import time
import re

# --- Configuration ---
XUMO_CHANNELS_API_URL = "https://common.xumo.com/v1/channels.json?filter=live"
XUMO_GUIDE_API_URL_TEMPLATE = "https://common.xumo.com/v1/guide/channel/{channel_id}.json?durationHours={duration}"
XUMO_LOGO_URL_TEMPLATE = "https://image.xumo.com/v1/channel/{channel_id}/512x512.png?type=color_on_transparent"
IPTV_ORG_XUMO_M3U_URL = "https://raw.githubusercontent.com/iptv-org/iptv/master/streams/us_xumo.m3u" # Use master branch

EPG_DURATION_HOURS = 12 # How many hours of EPG data to fetch per channel
API_DELAY_SECONDS = 0.1 # Small delay between EPG API calls

OUTPUT_DIR = "playlists"
PLAYLIST_FILENAME = "xumo_playlist.m3u"
EPG_FILENAME = "xumo_epg.xml.gz"

# IMPORTANT: Update with YOUR details for the EPG URL!
GITHUB_USER = "BuddyChewChew" # Replace with your GitHub Username
GITHUB_REPO = "xumo-playlist-generator" # Replace with your GitHub Repo Name for this project
GITHUB_BRANCH = "main"
EPG_RAW_URL = f"https://raw.githubusercontent.com/{GITHUB_USER}/{GITHUB_REPO}/{GITHUB_BRANCH}/{OUTPUT_DIR}/{EPG_FILENAME}"

# Use a common browser User-Agent
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
    'Referer': 'https://play.xumo.com/' # Good practice
}
REQUEST_TIMEOUT = 30 # seconds

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Helper Functions ---

def fetch_data(url, params=None, is_json=True):
    """Fetches data from a URL, handles JSON parsing and errors."""
    logging.debug(f"Fetching {'JSON' if is_json else 'text'} from: {url} with params: {params}")
    try:
        response = requests.get(url, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        if is_json:
            return response.json()
        else:
            # Decode explicitly as UTF-8, ignore errors
            return response.content.decode('utf-8', errors='ignore')
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching {url}: {e}")
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON from {url}: {e}")
    return None

def parse_iptv_org_m3u(m3u_content):
    """Parses the iptv-org M3U to extract tvg-id and stream URL."""
    stream_map = {}
    if not m3u_content:
        return stream_map

    lines = m3u_content.strip().split('\n')
    current_tvg_id = None

    # Regex to find tvg-id in #EXTINF line
    # Handles variations in spacing and quotes
    extinf_regex = re.compile(r'#EXTINF:-1.*?tvg-id\s*=\s*["\']?([^"\']+)["\']?.*')

    for i, line in enumerate(lines):
        line = line.strip()
        if line.startswith('#EXTINF'):
            match = extinf_regex.search(line)
            if match:
                current_tvg_id = match.group(1).strip()
            else:
                current_tvg_id = None # Reset if tvg-id not found
                logging.warning(f"Could not find tvg-id in line: {line}")
        elif current_tvg_id and (line.startswith('http://') or line.startswith('https://')):
            stream_map[current_tvg_id] = line
            current_tvg_id = None # Reset after finding URL

    logging.info(f"Parsed {len(stream_map)} streams from iptv-org M3U.")
    return stream_map

def format_xmltv_time(iso_time_str):
    """Formats an ISO 8601 UTC string (ending in Z) into XMLTV time format."""
    if not iso_time_str or not iso_time_str.endswith('Z'):
        return "" # Return empty if invalid format
    try:
        # Handle potential milliseconds
        if '.' in iso_time_str:
             dt_obj = datetime.strptime(iso_time_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        else:
             dt_obj = datetime.strptime(iso_time_str, "%Y-%m-%dT%H:%M:%SZ")
        # Ensure timezone is set to UTC
        dt_obj = dt_obj.replace(tzinfo=timezone.utc)
        return dt_obj.strftime('%Y%m%d%H%M%S %z') # Format includes timezone offset
    except ValueError as e:
        logging.warning(f"Could not parse timestamp '{iso_time_str}': {e}")
        return ""

def ensure_output_dir():
    """Creates the output directory if it doesn't exist."""
    if not os.path.exists(OUTPUT_DIR):
        logging.info(f"Creating output directory: {OUTPUT_DIR}")
        os.makedirs(OUTPUT_DIR)

def save_gzipped_xml(tree, filepath):
    """Saves the ElementTree XML to a gzipped file."""
    try:
        xml_string = ET.tostring(tree.getroot(), encoding='UTF-8', xml_declaration=True)
        with gzip.open(filepath, 'wb') as f:
            f.write(xml_string)
        logging.info(f"Gzipped EPG XML file saved: {filepath}")
    except Exception as e:
        logging.error(f"Error writing gzipped EPG file {filepath}: {e}")

def save_m3u(content, filepath):
    """Saves the M3U playlist content to a file."""
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        logging.info(f"M3U playlist file saved: {filepath}")
    except Exception as e:
        logging.error(f"Error writing M3U file {filepath}: {e}")

# --- Core Functions ---

def get_xumo_channels():
    """Fetches the list of live Xumo channels from the official API."""
    data = fetch_data(XUMO_CHANNELS_API_URL, is_json=True)
    if not data:
        logging.error("Failed to fetch channel data from Xumo API.")
        return []

    channels = []
    # Parse based on observed structure or iptv-org hints
    for item in data.get('channels', []):
        try:
            channel_id = str(item.get('id'))
            name = item.get('name', 'Unknown Channel')
            genre = item.get('genres', ['General'])[0] if item.get('genres') else 'General'
            # Construct logo URL using the template
            logo_url = XUMO_LOGO_URL_TEMPLATE.replace("{channel_id}", channel_id)

            if channel_id and name:
                channels.append({
                    'id': channel_id,
                    'name': name,
                    'logo': logo_url,
                    'group': genre
                })
            else:
                 logging.warning(f"Skipping channel due to missing ID or name: {item}")
        except Exception as e:
             logging.warning(f"Error processing channel item {item}: {e}")

    logging.info(f"Fetched {len(channels)} channels from Xumo API.")
    return channels

def get_xumo_epg_for_channels(channel_list):
    """Fetches EPG data for each channel individually from the Xumo API."""
    if not channel_list:
        return {}

    all_programs = {}
    total_channels = len(channel_list)
    logging.info(f"Fetching EPG data for {total_channels} channels...")

    for i, channel in enumerate(channel_list):
        channel_id = channel['id']
        guide_url = XUMO_GUIDE_API_URL_TEMPLATE.format(
            channel_id=channel_id,
            duration=EPG_DURATION_HOURS
        )
        logging.debug(f"Fetching EPG for channel {channel_id} ({i+1}/{total_channels})")
        channel_guide_data = fetch_data(guide_url, is_json=True)

        if channel_guide_data and 'programs' in channel_guide_data:
            all_programs[channel_id] = channel_guide_data['programs']
        else:
            logging.warning(f"No EPG programs found for channel {channel_id}")
            all_programs[channel_id] = [] # Add empty list if fetch failed or no programs

        # Be nice to the API
        time.sleep(API_DELAY_SECONDS)

    logging.info("Finished fetching EPG data.")
    return all_programs

def generate_epg_xml(channel_list, epg_data):
    """Generates the XMLTV ElementTree object."""
    logging.info("Generating EPG XML structure...")
    tv_element = ET.Element('tv', attrib={'generator-info-name': f'{GITHUB_USER}-Xumo-Scraper'})

    programme_count = 0
    # Add channel elements
    for channel in channel_list:
        chan_el = ET.SubElement(tv_element, 'channel', attrib={'id': channel['id']})
        ET.SubElement(chan_el, 'display-name').text = channel['name']
        if channel['logo']:
            ET.SubElement(chan_el, 'icon', attrib={'src': channel['logo']})

    # Add programme elements
    for channel_id, programs in epg_data.items():
        for program in programs:
            try:
                start_time_str = program.get('startTime')
                end_time_str = program.get('endTime')
                title = program.get('title', 'Unknown Program')
                desc = program.get('description')
                # Optional fields from iptv-org parser:
                icon = program.get('poster')
                episode_title = program.get('episodeTitle')
                season = program.get('seasonNumber')
                episode = program.get('episodeNumber')
                genres = program.get('genres') # This is a list

                start_formatted = format_xmltv_time(start_time_str)
                stop_formatted = format_xmltv_time(end_time_str)

                if start_formatted and stop_formatted:
                    prog_el = ET.SubElement(tv_element, 'programme', attrib={
                        'start': start_formatted,
                        'stop': stop_formatted,
                        'channel': channel_id
                    })
                    ET.SubElement(prog_el, 'title', attrib={'lang': 'en'}).text = title
                    if desc:
                        ET.SubElement(prog_el, 'desc', attrib={'lang': 'en'}).text = desc
                    if episode_title and episode_title != title: # Add subtitle if different
                        ET.SubElement(prog_el, 'sub-title', attrib={'lang': 'en'}).text = episode_title
                    if icon:
                         ET.SubElement(prog_el, 'icon', attrib={'src': icon})
                    if genres and isinstance(genres, list):
                         for genre in genres:
                              ET.SubElement(prog_el, 'category', attrib={'lang': 'en'}).text = genre
                    # Episode numbering (common XMLTV format)
                    if season is not None and episode is not None:
                         # Format: S.E-1 (e.g., 0.14 means S01E15)
                         s_num = int(season)
                         e_num = int(episode)
                         ep_num_str = f"{s_num-1}.{e_num-1}." # Using 0-based index common in XMLTV NS format
                         ET.SubElement(prog_el, 'episode-num', attrib={'system': 'xmltv_ns'}).text = ep_num_str

                    programme_count += 1
                else:
                    logging.warning(f"Skipping program due to invalid time: {title} ({start_time_str} / {end_time_str})")

            except Exception as e:
                logging.warning(f"Error processing program item {program} for channel {channel_id}: {e}")

    logging.info(f"Generated XML structure with {len(channel_list)} channels and {programme_count} programmes.")
    return ET.ElementTree(tv_element)

def generate_m3u_playlist(channel_list, stream_map):
    """Generates the M3U playlist string."""
    logging.info("Generating M3U playlist...")
    playlist_parts = [f'#EXTM3U url-tvg="{EPG_RAW_URL}"\n']
    added_count = 0

    # Sort channels alphabetically by name for consistent output
    sorted_channels = sorted(channel_list, key=lambda x: x['name'].lower())

    for channel in sorted_channels:
        channel_id = channel['id']
        # Look up the stream URL from the parsed iptv-org map
        stream_url = stream_map.get(channel_id)

        if stream_url:
            # Ensure name doesn't contain problematic characters for M3U display name
            display_name = channel['name'].replace(',', '')
            group_title = channel['group'].replace(',', '') # Also sanitize group

            line1 = f'#EXTINF:-1 tvg-id="{channel_id}" tvg-name="{channel["name"]}" tvg-logo="{channel["logo"]}" group-title="{group_title}",{display_name}\n'
            line2 = f'{stream_url}\n'
            playlist_parts.append(line1)
            playlist_parts.append(line2)
            added_count += 1
        else:
            logging.warning(f"No stream URL found in iptv-org map for channel: {channel_id} ({channel['name']})")

    logging.info(f"Added {added_count} channels with stream URLs to M3U playlist.")
    return "".join(playlist_parts)

# --- Main Execution ---
if __name__ == "__main__":
    logging.info("--- Starting Xumo Scraper ---")
    ensure_output_dir()

    # 1. Get official channel list
    xumo_channels = get_xumo_channels()
    if not xumo_channels:
        logging.error("Failed to get Xumo channel list. Aborting.")
        exit(1) # Exit with error to fail workflow

    # 2. Get iptv-org M3U content
    iptv_org_m3u_content = fetch_data(IPTV_ORG_XUMO_M3U_URL, is_json=False)
    if not iptv_org_m3u_content:
        logging.error("Failed to get iptv-org M3U stream list. Aborting.")
        exit(1) # Exit with error

    # 3. Parse iptv-org M3U to map tvg-id -> stream_url
    stream_url_map = parse_iptv_org_m3u(iptv_org_m3u_content)
    if not stream_url_map:
        logging.warning("Parsing iptv-org M3U resulted in empty stream map. Playlist may be empty.")

    # 4. Get EPG data for the official channels
    epg_data = get_xumo_epg_for_channels(xumo_channels)
    # Note: epg_data can be {} if all fetches fail, script will continue

    # 5. Generate EPG XML
    epg_tree = generate_epg_xml(xumo_channels, epg_data)

    # 6. Generate M3U Playlist using official channels and mapped streams
    m3u_content = generate_m3u_playlist(xumo_channels, stream_url_map)

    # 7. Save Files
    save_m3u(m3u_content, os.path.join(OUTPUT_DIR, PLAYLIST_FILENAME))
    save_gzipped_xml(epg_tree, os.path.join(OUTPUT_DIR, EPG_FILENAME))

    logging.info("--- Xumo Scraper Finished Successfully ---")
