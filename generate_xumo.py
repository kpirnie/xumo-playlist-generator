import requests
import json
import os
import gzip
from datetime import datetime, timedelta, timezone
import xml.etree.ElementTree as ET
import logging
import time
import re
from math import ceil

# --- Configuration ---
API_ENDPOINT = "https://valencia-app-mds.xumo.com/v2"
CHANNEL_LIST_URL = f"{API_ENDPOINT}/channels/list/10006.json?sort=hybrid&geoId=unknown"
EPG_DISCOVERY_URL_TEMPLATE = f"{API_ENDPOINT}/epg/10006/19700101/0.json?limit=50&offset={{offset}}"
EPG_FETCH_URL_TEMPLATE = f"{API_ENDPOINT}/epg/10006/{{date_str}}/{{page}}.json?f=asset.title&f=asset.descriptions&limit=50&offset={{offset}}"
XUMO_LOGO_URL_TEMPLATE = "https://image.xumo.com/v1/channel/{channel_id}/512x512.png?type=color_on_transparent"
IPTV_ORG_XUMO_M3U_URL = "https://raw.githubusercontent.com/iptv-org/iptv/master/streams/us_xumo.m3u"

EPG_FETCH_DAYS = 2 # How many days of EPG data to fetch (today + tomorrow)
EPG_FETCH_PAGES = 4 # How many pages (0.json, 1.json...) to try fetching per date/offset based on JS example
API_DELAY_SECONDS = 0.1 # Small delay between EPG API calls
MAX_DISCOVERY_OFFSETS = 8 # Max number of offsets (0, 50..350) to check for active channels based on JS example

OUTPUT_DIR = "playlists"
PLAYLIST_FILENAME = "xumo_playlist.m3u"
EPG_FILENAME = "xumo_epg.xml.gz"

# IMPORTANT: Update with YOUR details for the EPG URL!
GITHUB_USER = "BuddyChewChew" # Replace with your GitHub Username
GITHUB_REPO = "xumo-playlist-generator" # Replace with your GitHub Repo Name for this project
GITHUB_BRANCH = "main"
EPG_RAW_URL = f"https://raw.githubusercontent.com/{GITHUB_USER}/{GITHUB_REPO}/{GITHUB_BRANCH}/{OUTPUT_DIR}/{EPG_FILENAME}"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
}
REQUEST_TIMEOUT = 45 # Increased timeout for potentially slower API calls

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Helper Functions --- (Keep fetch_data, parse_iptv_org_m3u, format_xmltv_time, ensure_output_dir, save_gzipped_xml, save_m3u from previous script)

def fetch_data(url, params=None, is_json=True, retries=2, delay=1):
    """Fetches data from a URL, handles JSON parsing and errors, includes retries."""
    logging.debug(f"Fetching {'JSON' if is_json else 'text'} from: {url} with params: {params}")
    for attempt in range(retries + 1):
        try:
            response = requests.get(url, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            if is_json:
                return response.json()
            else:
                return response.content.decode('utf-8', errors='ignore')
        except requests.exceptions.RequestException as e:
            logging.warning(f"Attempt {attempt+1} failed fetching {url}: {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                logging.error(f"Final attempt failed fetching {url}: {e}")
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON from {url}: {e}")
            break # Don't retry JSON errors
    return None

def parse_iptv_org_m3u(m3u_content):
    """Parses the iptv-org M3U to extract tvg-id and stream URL."""
    stream_map = {}
    if not m3u_content: return stream_map
    lines = m3u_content.strip().split('\n')
    current_tvg_id = None
    extinf_regex = re.compile(r'#EXTINF:-1.*?tvg-id\s*=\s*["\']?([^"\']+)["\']?.*')
    for i, line in enumerate(lines):
        line = line.strip()
        if line.startswith('#EXTINF'):
            match = extinf_regex.search(line)
            if match: current_tvg_id = match.group(1).strip()
            else: current_tvg_id = None; logging.warning(f"Could not find tvg-id in line: {line}")
        elif current_tvg_id and (line.startswith('http://') or line.startswith('https://')):
            stream_map[current_tvg_id] = line
            current_tvg_id = None
    logging.info(f"Parsed {len(stream_map)} streams from iptv-org M3U.")
    return stream_map

def format_xmltv_time(dt_obj):
    """Formats a datetime object into XMLTV time format (YYYYMMDDHHMMSS +ZZZZ)."""
    if not isinstance(dt_obj, datetime): return ""
    if not dt_obj.tzinfo: dt_obj = dt_obj.replace(tzinfo=timezone.utc) # Assume UTC
    return dt_obj.strftime('%Y%m%d%H%M%S %z')

def parse_iso_datetime(iso_time_str):
    """Parses ISO 8601 string (handles Z and potential milliseconds) to datetime object."""
    if not iso_time_str: return None
    try:
        # Remove 'Z' and add UTC offset for consistent parsing
        if iso_time_str.endswith('Z'):
            iso_time_str = iso_time_str[:-1] + '+00:00'
        # Handle milliseconds if present
        if '.' in iso_time_str:
            # Truncate microseconds for strptime
            time_part = iso_time_str.split('.')
            ms_part = time_part[1].split('+')[0]
            offset_part = time_part[1].split('+')[1]
            iso_time_str = f"{time_part[0]}.{ms_part[:6]}+{offset_part}" # Max 6 digits for microseconds
            dt_obj = datetime.fromisoformat(iso_time_str)
        else:
             dt_obj = datetime.fromisoformat(iso_time_str)
        return dt_obj
    except ValueError as e:
        logging.warning(f"Could not parse ISO timestamp '{iso_time_str}': {e}")
        return None

def ensure_output_dir():
    """Creates the output directory if it doesn't exist."""
    if not os.path.exists(OUTPUT_DIR):
        logging.info(f"Creating output directory: {OUTPUT_DIR}")
        os.makedirs(OUTPUT_DIR)

def save_gzipped_xml(tree, filepath):
    """Saves the ElementTree XML to a gzipped file."""
    try:
        xml_string = ET.tostring(tree.getroot(), encoding='UTF-8', xml_declaration=True)
        with gzip.open(filepath, 'wb') as f: f.write(xml_string)
        logging.info(f"Gzipped EPG XML file saved: {filepath}")
    except Exception as e: logging.error(f"Error writing gzipped EPG file {filepath}: {e}")

def save_m3u(content, filepath):
    """Saves the M3U playlist content to a file."""
    try:
        with open(filepath, 'w', encoding='utf-8') as f: f.write(content)
        logging.info(f"M3U playlist file saved: {filepath}")
    except Exception as e: logging.error(f"Error writing M3U file {filepath}: {e}")


# --- Core Functions ---

def get_full_channel_map():
    """Fetches the master channel list and creates a map ID -> info."""
    logging.info("Fetching master channel list...")
    data = fetch_data(CHANNEL_LIST_URL, is_json=True)
    if not data or 'channel' not in data or 'item' not in data['channel']:
        logging.error("Invalid or empty master channel list response.")
        return {}

    channel_map = {}
    for item in data['channel'].get('item', []):
        try:
            channel_id = item.get('guid', {}).get('value')
            title = item.get('title')
            # Genre/Category might need investigation in the actual JSON
            genre = item.get('category', {}).get('label', 'General')

            if channel_id and title:
                channel_id_str = str(channel_id)
                channel_map[channel_id_str] = {
                    'id': channel_id_str,
                    'name': title,
                    'logo': XUMO_LOGO_URL_TEMPLATE.replace("{channel_id}", channel_id_str),
                    'group': genre
                }
            else:
                 logging.warning(f"Skipping master channel item due to missing ID or title: {item}")
        except Exception as e:
             logging.warning(f"Error processing master channel item {item}: {e}")

    logging.info(f"Processed {len(channel_map)} channels from master list.")
    return channel_map

def discover_active_channels_with_offsets():
    """Finds channels with EPG data using discovery endpoints, returns dict {id: offset}."""
    logging.info("Discovering active channels with EPG data...")
    active_channel_offsets = {}
    processed_ids = set() # Avoid duplicates if channel appears in multiple offsets

    for i in range(MAX_DISCOVERY_OFFSETS):
        offset = i * 50
        discovery_url = EPG_DISCOVERY_URL_TEMPLATE.format(offset=offset)
        logging.debug(f"Checking discovery offset {offset}...")
        data = fetch_data(discovery_url, is_json=True)

        if data and 'channels' in data and isinstance(data['channels'], list):
            found_in_offset = 0
            for channel_data in data['channels']:
                channel_id = str(channel_data.get('channelId'))
                # Check if it has a schedule and hasn't been processed from another offset
                if channel_id and channel_data.get('schedule') and channel_id not in processed_ids:
                    active_channel_offsets[channel_id] = offset
                    processed_ids.add(channel_id)
                    found_in_offset += 1
            logging.debug(f"Offset {offset}: Found {found_in_offset} new active channels.")
        else:
            logging.warning(f"No valid channel data found for discovery offset {offset}. Stopping discovery.")
            break # Stop if an offset returns invalid data

        time.sleep(API_DELAY_SECONDS) # Be nice

    logging.info(f"Discovered {len(active_channel_offsets)} active channels across offsets.")
    return active_channel_offsets

def fetch_consolidated_epg_data(active_channel_offsets):
    """Fetches EPG data for active channels for the required dates and pages."""
    if not active_channel_offsets: return {}

    logging.info(f"Fetching EPG data for {len(active_channel_offsets)} active channels...")
    consolidated_epg = {} # Structure: { channel_id: [program_details, ...] }
    assets_cache = {} # Cache asset details: { asset_id: {details} }

    # Determine dates to fetch
    today = datetime.now(timezone.utc)
    dates_to_fetch = [today + timedelta(days=d) for d in range(EPG_FETCH_DAYS)]

    # Group channels by their offset for efficient fetching
    offsets_to_process = sorted(list(set(active_channel_offsets.values())))
    channels_by_offset = {offset: [] for offset in offsets_to_process}
    for channel_id, offset in active_channel_offsets.items():
        channels_by_offset[offset].append(channel_id)

    total_requests = len(dates_to_fetch) * len(offsets_to_process) * EPG_FETCH_PAGES
    request_count = 0
    logging.info(f"Estimated EPG fetch requests: {total_requests}")

    for date_obj in dates_to_fetch:
        date_str = date_obj.strftime('%Y%m%d')
        for offset in offsets_to_process:
            # Fetch multiple pages for this date/offset combo
            for page in range(EPG_FETCH_PAGES):
                request_count += 1
                logging.debug(f"Fetching EPG - Date: {date_str}, Offset: {offset}, Page: {page} ({request_count}/{total_requests})")
                fetch_url = EPG_FETCH_URL_TEMPLATE.format(date_str=date_str, page=page, offset=offset)
                page_data = fetch_data(fetch_url, is_json=True)

                if not page_data:
                    logging.warning(f"No data received for {fetch_url}. Skipping page.")
                    continue # Skip if fetch fails

                # Cache asset details from this page
                if 'assets' in page_data and isinstance(page_data['assets'], dict):
                    assets_cache.update(page_data['assets'])

                # Process schedules for channels in this offset group
                if 'channels' in page_data and isinstance(page_data['channels'], list):
                    for channel_schedule_data in page_data['channels']:
                        channel_id = str(channel_schedule_data.get('channelId'))
                        # Only process if this channel belongs to the current offset group
                        if channel_id in channels_by_offset.get(offset, []):
                            if channel_id not in consolidated_epg:
                                consolidated_epg[channel_id] = []

                            for program_schedule in channel_schedule_data.get('schedule', []):
                                asset_id = program_schedule.get('assetId')
                                asset_details = assets_cache.get(asset_id)
                                if asset_details:
                                    # Combine schedule timing with asset details
                                    program_info = {
                                        **program_schedule, # Contains start, end, assetId
                                        **asset_details     # Contains title, description, etc.
                                    }
                                    consolidated_epg[channel_id].append(program_info)
                                else:
                                    logging.warning(f"Asset details not found for assetId {asset_id} on channel {channel_id}")
                else:
                     logging.warning(f"No 'channels' list found in response for {fetch_url}")

                time.sleep(API_DELAY_SECONDS) # Be nice between pages/offsets

    logging.info("Finished consolidating EPG data.")
    return consolidated_epg


def generate_epg_xml(filtered_channel_list, consolidated_epg_data):
    """Generates the XMLTV ElementTree object from filtered channels and consolidated EPG."""
    logging.info("Generating EPG XML structure...")
    tv_element = ET.Element('tv', attrib={'generator-info-name': f'{GITHUB_USER}-Xumo-Scraper'})

    programme_count = 0
    # Add channel elements for channels that had EPG data or are in the filtered list
    for channel in filtered_channel_list:
        chan_el = ET.SubElement(tv_element, 'channel', attrib={'id': channel['id']})
        ET.SubElement(chan_el, 'display-name').text = channel['name']
        if channel['logo']: ET.SubElement(chan_el, 'icon', attrib={'src': channel['logo']})

    # Add programme elements
    for channel_id, programs in consolidated_epg_data.items():
        for program in programs:
            try:
                start_time = parse_iso_datetime(program.get('start'))
                end_time = parse_iso_datetime(program.get('end'))
                title = program.get('title', 'Unknown Program')
                desc = program.get('descriptions', {}).get('medium') or program.get('descriptions', {}).get('small') or program.get('descriptions', {}).get('tiny')
                episode_title = program.get('episodeTitle')
                # Add other fields if needed: icon, season, episode, genres

                start_formatted = format_xmltv_time(start_time)
                stop_formatted = format_xmltv_time(end_time)

                if start_formatted and stop_formatted:
                    prog_el = ET.SubElement(tv_element, 'programme', attrib={
                        'start': start_formatted,
                        'stop': stop_formatted,
                        'channel': channel_id
                    })
                    ET.SubElement(prog_el, 'title', attrib={'lang': 'en'}).text = title
                    if desc: ET.SubElement(prog_el, 'desc', attrib={'lang': 'en'}).text = desc
                    if episode_title and episode_title != title: ET.SubElement(prog_el, 'sub-title', attrib={'lang': 'en'}).text = episode_title
                    # Add icon, category, episode-num elements here if desired and available
                    programme_count += 1
                else:
                    logging.warning(f"Skipping program due to invalid time: {title} (Start: {program.get('start')}, End: {program.get('end')})")

            except Exception as e:
                logging.warning(f"Error processing program item {program} for channel {channel_id}: {e}")

    logging.info(f"Generated XML structure with {len(filtered_channel_list)} channels and {programme_count} programmes.")
    return ET.ElementTree(tv_element)

def generate_m3u_playlist(filtered_channel_list, stream_map):
    """Generates the M3U playlist string using filtered channels and mapped streams."""
    logging.info("Generating M3U playlist...")
    playlist_parts = [f'#EXTM3U url-tvg="{EPG_RAW_URL}"\n']
    added_count = 0

    # Sort channels alphabetically by name for consistent output
    sorted_channels = sorted(filtered_channel_list, key=lambda x: x['name'].lower())

    for channel in sorted_channels:
        channel_id = channel['id']
        stream_url = stream_map.get(channel_id)

        if stream_url:
            display_name = channel['name'].replace(',', '')
            group_title = channel['group'].replace(',', '')

            line1 = f'#EXTINF:-1 tvg-id="{channel_id}" tvg-name="{channel["name"]}" tvg-logo="{channel["logo"]}" group-title="{group_title}",{display_name}\n'
            line2 = f'{stream_url}\n'
            playlist_parts.append(line1)
            playlist_parts.append(line2)
            added_count += 1
        else:
            logging.warning(f"No stream URL found in iptv-org map for active channel: {channel_id} ({channel['name']})")

    logging.info(f"Added {added_count} channels with stream URLs to M3U playlist.")
    return "".join(playlist_parts)

# --- Main Execution ---
if __name__ == "__main__":
    logging.info("--- Starting Xumo Scraper (Using Valencia MDS API) ---")
    ensure_output_dir()

    # 1. Get full official channel map (ID -> {name, logo, group})
    full_channel_map = get_full_channel_map()
    if not full_channel_map:
        logging.error("Failed to get Xumo master channel list. Aborting.")
        exit(1)

    # 2. Discover active channels and their required EPG offset
    active_channel_offsets = discover_active_channels_with_offsets()
    if not active_channel_offsets:
        logging.warning("No active channels discovered. Output might be empty.")
        # Decide if we should exit or continue with just the channel map
        # exit(1) # Or allow empty files to be generated

    # 3. Create filtered list of channel details ONLY for active channels
    filtered_channel_list = []
    for channel_id in active_channel_offsets.keys():
        if channel_id in full_channel_map:
            # Add the offset to the channel info - might not be needed later but good for debug
            channel_info = full_channel_map[channel_id]
            channel_info['epg_offset'] = active_channel_offsets[channel_id]
            filtered_channel_list.append(channel_info)
        else:
            logging.warning(f"Active channel ID {channel_id} not found in master channel map.")

    logging.info(f"Proceeding with {len(filtered_channel_list)} active/mapped channels.")

    # 4. Get iptv-org M3U content
    iptv_org_m3u_content = fetch_data(IPTV_ORG_XUMO_M3U_URL, is_json=False)
    if not iptv_org_m3u_content:
        logging.error("Failed to get iptv-org M3U stream list. Aborting.")
        exit(1)

    # 5. Parse iptv-org M3U to map tvg-id -> stream_url
    stream_url_map = parse_iptv_org_m3u(iptv_org_m3u_content)
    if not stream_url_map:
        logging.warning("Parsing iptv-org M3U resulted in empty stream map.")

    # 6. Fetch consolidated EPG data for the active channels
    consolidated_epg = fetch_consolidated_epg_data(active_channel_offsets)

    # 7. Generate EPG XML using filtered channels and fetched EPG data
    epg_tree = generate_epg_xml(filtered_channel_list, consolidated_epg)

    # 8. Generate M3U Playlist using filtered channels and mapped streams
    m3u_content = generate_m3u_playlist(filtered_channel_list, stream_url_map)

    # 9. Save Files
    save_m3u(m3u_content, os.path.join(OUTPUT_DIR, PLAYLIST_FILENAME))
    save_gzipped_xml(epg_tree, os.path.join(OUTPUT_DIR, EPG_FILENAME))

    logging.info("--- Xumo Scraper Finished Successfully ---")
