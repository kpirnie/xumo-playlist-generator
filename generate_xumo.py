import requests
import json
import os
import gzip
from datetime import datetime, timedelta, timezone
import xml.etree.ElementTree as ET
import logging
import time
import re
import sys # Import sys to use sys.exit

# --- Configuration ---
XUMO_TV_URL = "http://www.xumo.tv" # For fetching dynamic IDs
API_ENDPOINT = "https://valencia-app-mds.xumo.com/v2"
CHANNEL_LIST_URL_TEMPLATE = f"{API_ENDPOINT}/channels/list/{{channel_list_id}}.json?geoId={{geo_id}}"
ON_NOW_URL_TEMPLATE = f"{API_ENDPOINT}/channels/list/{{channel_list_id}}/onnowandnext.json?f=asset.title&f=asset.descriptions.json"
ASSET_DETAILS_URL_TEMPLATE = f"{API_ENDPOINT}/assets/asset/{{asset_id}}.json?f=providers"
EPG_URL_TEMPLATE = f"{API_ENDPOINT}/channels/channel/{{channel_id}}/broadcast.json?hour={{hour_num}}" # E.g. hour=0..23
XUMO_LOGO_URL_TEMPLATE = "https://image.xumo.com/v1/channels/channel/{channel_id}/600x450.png?type=color_onWhite" # Adjusted based on PHP example

EPG_FETCH_HOURS = 24 # How many hours of EPG data to fetch from now
API_DELAY_SECONDS = 0.15 # Slightly increased delay between rapid API calls

OUTPUT_DIR = "playlists"
PLAYLIST_FILENAME = "xumo_playlist.m3u"
EPG_FILENAME = "xumo_epg.xml.gz"

# !!! IMPORTANT: Verify/Update these with YOUR details !!!
GITHUB_USER = "BuddyChewChew"
GITHUB_REPO = "xumo-playlist-generator" # The name of THIS repository
GITHUB_BRANCH = "main"
EPG_RAW_URL = f"https://raw.githubusercontent.com/{GITHUB_USER}/{GITHUB_REPO}/{GITHUB_BRANCH}/{OUTPUT_DIR}/{EPG_FILENAME}"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0', # Example modern UA
    'Referer': 'https://play.xumo.com/', # Good practice
    'Origin': 'https://play.xumo.com' # Added based on headers
}
REQUEST_TIMEOUT = 45

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Helper Functions ---

def fetch_data(url, params=None, is_json=True, retries=2, delay=2):
    """Fetches data from a URL, handles JSON parsing and errors, includes retries."""
    logging.debug(f"Fetching {'JSON' if is_json else 'text'} from: {url} with params: {params}")
    for attempt in range(retries + 1):
        try:
            response = requests.get(url, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            response.raise_for_status()
            if is_json:
                return response.json()
            else:
                return response.content.decode('utf-8', errors='ignore')
        except requests.exceptions.RequestException as e:
            logging.warning(f"Attempt {attempt+1}/{retries+1} failed fetching {url}: {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                logging.error(f"Final attempt failed fetching {url}: {e}")
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON from {url}: {e}")
            break # Don't retry JSON errors
    return None

def format_xmltv_time(dt_obj):
    """Formats a datetime object into XMLTV time format (YYYYMMDDHHMMSS +ZZZZ)."""
    if not isinstance(dt_obj, datetime): return ""
    if not dt_obj.tzinfo: dt_obj = dt_obj.replace(tzinfo=timezone.utc) # Assume UTC
    return dt_obj.strftime('%Y%m%d%H%M%S %z')

def parse_xumo_timestamp(ts_milliseconds):
    """Converts Xumo timestamp (milliseconds since epoch) to datetime object."""
    if ts_milliseconds is None: return None
    try:
        # Convert milliseconds to seconds
        timestamp_seconds = int(ts_milliseconds) / 1000.0
        return datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)
    except (ValueError, TypeError) as e:
        logging.warning(f"Could not parse timestamp '{ts_milliseconds}': {e}")
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

# --- Core Logic Functions ---

def get_dynamic_ids():
    """Fetches xumo.tv HTML to extract channelListId and geoId."""
    logging.info(f"Fetching dynamic IDs from {XUMO_TV_URL}...")
    html_content = fetch_data(XUMO_TV_URL, is_json=False)
    if not html_content:
        return None, None

    channel_list_id = None
    geo_id = None

    # Regex patterns based on PHP example
    list_match = re.search(r'"channelListId"\s*:\s*"(.*?)"', html_content)
    geo_match = re.search(r'"geoId"\s*:\s*"(.*?)"', html_content)

    if list_match:
        channel_list_id = list_match.group(1)
        logging.info(f"Found channelListId: {channel_list_id}")
    else:
        logging.error("Could not find channelListId in HTML content.")

    if geo_match:
        geo_id = geo_match.group(1)
        logging.info(f"Found geoId: {geo_id}")
    else:
        logging.error("Could not find geoId in HTML content.")

    return channel_list_id, geo_id

def get_master_channel_map(channel_list_id, geo_id):
    """Fetches the master channel list and creates a map ID -> info."""
    if not channel_list_id or not geo_id: return {}

    master_list_url = CHANNEL_LIST_URL_TEMPLATE.format(channel_list_id=channel_list_id, geo_id=geo_id)
    logging.info(f"Fetching master channel list from {master_list_url}...")
    data = fetch_data(master_list_url, is_json=True)

    if not data or 'channel' not in data or 'item' not in data['channel']:
        logging.error("Invalid or empty master channel list response.")
        return {}

    channel_map = {}
    for item in data['channel'].get('item', []):
        try:
            # guid.value seems to be the reliable channel ID
            channel_id = item.get('guid', {}).get('value')
            title = item.get('title')
            number_str = item.get('number') # Keep as string initially
            genre = item.get('genre', [{}])[0].get('value', 'General') if item.get('genre') else 'General'

            if channel_id and title:
                channel_id_str = str(channel_id)
                logo_url = XUMO_LOGO_URL_TEMPLATE.replace("{channel_id}", channel_id_str)
                channel_map[channel_id_str] = {
                    'id': channel_id_str,
                    'name': title,
                    'number': number_str, # Store original number string
                    'logo': logo_url,
                    'group': genre,
                    'current_asset_id': None, # To be filled later
                    'stream_url': None      # To be filled later
                }
            else:
                 logging.warning(f"Skipping master channel item due to missing ID or title: {item}")
        except Exception as e:
             logging.warning(f"Error processing master channel item {item}: {e}")

    logging.info(f"Processed {len(channel_map)} channels from master list.")
    return channel_map

def get_live_channel_asset_ids(channel_list_id):
    """Gets 'On Now' data, filters for live channels, returns map channelId -> assetId."""
    if not channel_list_id: return {}

    on_now_url = ON_NOW_URL_TEMPLATE.format(channel_list_id=channel_list_id)
    logging.info(f"Fetching 'On Now' data from {on_now_url}...")
    data = fetch_data(on_now_url, is_json=True)

    if not data or 'results' not in data or not isinstance(data['results'], list):
        logging.error("Invalid or empty 'On Now' response.")
        return {}

    live_asset_map = {}
    for item in data.get('results', []):
        try:
            content_type = item.get('contentType')
            channel_id = item.get('channelId')
            asset_id = item.get('id') # This 'id' from the onnow response *is* the asset ID

            # Filter for live simulcast channels based on PHP example
            if content_type == 'SIMULCAST' and channel_id and asset_id:
                live_asset_map[str(channel_id)] = str(asset_id)
            # else:
                # logging.debug(f"Skipping non-SIMULCAST or incomplete 'On Now' item: channel={channel_id}, type={content_type}")

        except Exception as e:
            logging.warning(f"Error processing 'On Now' item {item}: {e}")

    logging.info(f"Found {len(live_asset_map)} potentially live channels with current asset IDs.")
    return live_asset_map

def fetch_and_add_stream_urls(live_channels_list):
    """Fetches stream URL for each live channel using its current asset ID."""
    logging.info(f"Fetching stream URLs for {len(live_channels_list)} channels...")
    processed_count = 0
    for channel_info in live_channels_list:
        asset_id = channel_info.get('current_asset_id')
        channel_id = channel_info['id']
        if not asset_id:
            logging.warning(f"Skipping stream URL fetch for channel {channel_id} - Missing current asset ID.")
            continue

        asset_url = ASSET_DETAILS_URL_TEMPLATE.format(asset_id=asset_id)
        logging.debug(f"Fetching asset details for stream URL: {asset_url}")
        asset_data = fetch_data(asset_url, is_json=True)

        stream_url = None
        if (asset_data and 'providers' in asset_data and
                isinstance(asset_data['providers'], list) and len(asset_data['providers']) > 0 and
                'sources' in asset_data['providers'][0] and
                isinstance(asset_data['providers'][0]['sources'], list) and len(asset_data['providers'][0]['sources']) > 0 and
                'uri' in asset_data['providers'][0]['sources'][0]):
            stream_url = asset_data['providers'][0]['sources'][0]['uri']
            # Simple check for master.m3u8, might need refinement
            if stream_url and "master.m3u8" in stream_url:
                 channel_info['stream_url'] = stream_url
                 logging.debug(f"Found stream URL for channel {channel_id}: {stream_url}")
                 processed_count += 1
            else:
                 logging.warning(f"Could not find valid master.m3u8 URI in sources for asset {asset_id} (Channel {channel_id})")
                 stream_url = None # Ensure it's None if not found

        if not stream_url:
             logging.warning(f"Failed to get stream URL for asset {asset_id} (Channel {channel_id})")

        time.sleep(API_DELAY_SECONDS) # Delay between asset detail fetches

    logging.info(f"Successfully fetched stream URLs for {processed_count} channels.")

def fetch_epg_for_channels(live_channels_list):
    """Fetches EPG data using the broadcast.json?hour=... endpoint."""
    if not live_channels_list: return {}

    logging.info(f"Fetching EPG data for {len(live_channels_list)} live channels...")
    consolidated_epg = {channel['id']: [] for channel in live_channels_list} # Initialize structure

    now_utc = datetime.now(timezone.utc)
    total_requests = len(live_channels_list) * EPG_FETCH_HOURS
    request_count = 0

    for i, channel in enumerate(live_channels_list):
        channel_id = channel['id']
        logging.debug(f"Fetching EPG for channel {channel_id} ({i+1}/{len(live_channels_list)})...")

        # Calculate hours to fetch relative to current UTC hour
        hours_to_fetch = []
        try:
            current_hour_dt = now_utc.replace(minute=0, second=0, microsecond=0)
            for h in range(EPG_FETCH_HOURS):
                target_dt = current_hour_dt + timedelta(hours=h)
                # Format needed by API seems to be just the hour number (0-23) within a 24h cycle
                hour_num = target_dt.hour
                # We also need date context later, maybe fetch based on timestamp?
                # Let's try hour number as per fHDHR plugin
                hours_to_fetch.append(hour_num)
        except Exception as e:
            logging.error(f"Error calculating hours for channel {channel_id}: {e}")
            continue

        # This simple hour loop might miss date context, let's rethink based on fHDHR cache key
        # Cache key used timestamp: datetime.today().replace(hour=cache_key).timestamp()
        # Let's fetch based on target datetime timestamps for clarity

        fetched_program_ids_for_channel = set()

        for h_offset in range(EPG_FETCH_HOURS):
            request_count += 1
            target_dt = now_utc + timedelta(hours=h_offset)
            # Use the hour of the target time
            hour_num = target_dt.hour

            epg_url = EPG_URL_TEMPLATE.format(channel_id=channel_id, hour_num=hour_num)
            logging.debug(f"Fetching EPG - Channel: {channel_id}, Target Hour: {hour_num} ({request_count}/{total_requests})")
            epg_hour_data = fetch_data(epg_url, is_json=True)

            if epg_hour_data and 'assets' in epg_hour_data and isinstance(epg_hour_data['assets'], list):
                programs_in_hour = 0
                for asset in epg_hour_data['assets']:
                    program_id = asset.get('id') # Assuming 'id' in asset is unique program ID
                    start_ts = asset.get('timestamps', {}).get('start')
                    end_ts = asset.get('timestamps', {}).get('end')

                    # Avoid duplicates if a program spans across hour fetches
                    if program_id and program_id not in fetched_program_ids_for_channel and start_ts and end_ts:
                        consolidated_epg[channel_id].append(asset)
                        fetched_program_ids_for_channel.add(program_id)
                        programs_in_hour += 1

                logging.debug(f"Channel {channel_id}, Hour {hour_num}: Added {programs_in_hour} new programs.")
            else:
                logging.warning(f"No valid EPG assets found for channel {channel_id}, hour {hour_num}")

            time.sleep(API_DELAY_SECONDS) # Delay between hourly fetches for a channel

    logging.info("Finished fetching EPG data.")
    return consolidated_epg


def generate_epg_xml(channel_list, consolidated_epg_data):
    """Generates the XMLTV ElementTree object from filtered channels and consolidated EPG."""
    logging.info("Generating EPG XML structure...")
    tv_element = ET.Element('tv', attrib={'generator-info-name': f'{GITHUB_USER}-{GITHUB_REPO}'})

    programme_count = 0
    # Add channel elements
    for channel in channel_list:
        chan_el = ET.SubElement(tv_element, 'channel', attrib={'id': channel['id']})
        ET.SubElement(chan_el, 'display-name').text = channel['name']
        if channel.get('number'): # Add channel number if available
             ET.SubElement(chan_el, 'display-name').text = channel['number']
        if channel['logo']: ET.SubElement(chan_el, 'icon', attrib={'src': channel['logo']})

    # Add programme elements
    for channel_id, programs in consolidated_epg_data.items():
        for program in programs:
            try:
                # Parse timestamps using helper
                start_time = parse_xumo_timestamp(program.get('timestamps', {}).get('start'))
                end_time = parse_xumo_timestamp(program.get('timestamps', {}).get('end'))

                # Try fetching details from the asset itself
                title = program.get('title', 'Unknown Program')
                desc_obj = program.get('descriptions', {})
                desc = desc_obj.get('large') or desc_obj.get('medium') or desc_obj.get('small') or desc_obj.get('tiny')
                # Add other fields if needed: episodeTitle, seasonNumber, episodeNumber, genres, image/poster

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
                    # Add sub-title, icon, category, episode-num elements here if desired
                    programme_count += 1
                else:
                    logging.warning(f"Skipping program due to invalid time: {title} (Start TS: {program.get('timestamps', {}).get('start')}, End TS: {program.get('timestamps', {}).get('end')})")

            except Exception as e:
                logging.warning(f"Error processing EPG program item {program} for channel {channel_id}: {e}")

    logging.info(f"Generated XML structure with {len(channel_list)} channels and {programme_count} programmes.")
    return ET.ElementTree(tv_element)

def generate_m3u_playlist(channel_list):
    """Generates the M3U playlist string using channel list that includes stream URLs."""
    logging.info("Generating M3U playlist...")
    playlist_parts = [f'#EXTM3U url-tvg="{EPG_RAW_URL}"\n']
    added_count = 0

    # Sort channels numerically if possible, otherwise alphabetically
    def sort_key(channel):
        try: return int(channel.get('number', '99999')) # Try converting number to int
        except (ValueError, TypeError): return 99999 # Put non-numeric high
    sorted_channels = sorted(channel_list, key=lambda x: (sort_key(x), x['name'].lower()))

    for channel in sorted_channels:
        stream_url = channel.get('stream_url')
        channel_id = channel['id']

        if stream_url:
            display_name = channel['name'].replace(',', '') # M3U display name after comma
            group_title = channel['group'].replace(',', '') # Sanitize group

            line1 = f'#EXTINF:-1 tvg-id="{channel_id}" tvg-name="{channel["name"]}" tvg-logo="{channel["logo"]}" group-title="{group_title}",{display_name}\n'
            line2 = f'{stream_url}\n'
            playlist_parts.append(line1)
            playlist_parts.append(line2)
            added_count += 1
        # No warning here, filtering happens before calling this function

    logging.info(f"Added {added_count} channels with stream URLs to M3U playlist.")
    return "".join(playlist_parts)

# --- Main Execution ---
if __name__ == "__main__":
    logging.info("--- Starting Xumo Scraper (Combined Strategy) ---")
    ensure_output_dir()

    # Step 0: Get Dynamic IDs
    channel_list_id, geo_id = get_dynamic_ids()
    if not channel_list_id or not geo_id:
        logging.error("Failed to get dynamic channelListId or geoId. Aborting.")
        sys.exit(1) # Use sys.exit(1) for cleaner exit with error

    # Step 1: Get Master Channel Map
    master_channel_map = get_master_channel_map(channel_list_id, geo_id)
    if not master_channel_map:
        logging.error("Failed to get Xumo master channel list. Aborting.")
        sys.exit(1)

    # Step 2: Get "On Now" Asset IDs for Live Channels
    live_channel_asset_ids = get_live_channel_asset_ids(channel_list_id)
    if not live_channel_asset_ids:
        logging.warning("Could not find any live channels via 'On Now' endpoint. Output may be empty or incomplete.")
        # Continue, but M3U/EPG might be empty

    # Step 3: Create Final List of Live Channels with Details
    live_channels_final = []
    for channel_id, asset_id in live_channel_asset_ids.items():
        if channel_id in master_channel_map:
            channel_info = master_channel_map[channel_id]
            channel_info['current_asset_id'] = asset_id # Add asset ID needed for stream URL
            live_channels_final.append(channel_info)
        else:
            logging.warning(f"Live channel ID {channel_id} from 'On Now' not found in master channel map.")

    logging.info(f"Processing {len(live_channels_final)} live channels found in both lists.")

    if not live_channels_final:
         logging.warning("No live channels to process after filtering. Exiting.")
         # Create empty files to avoid commit errors if files existed before
         save_m3u(f'#EXTM3U url-tvg="{EPG_RAW_URL}"\n', os.path.join(OUTPUT_DIR, PLAYLIST_FILENAME))
         save_gzipped_xml(ET.ElementTree(ET.Element('tv')), os.path.join(OUTPUT_DIR, EPG_FILENAME))
         sys.exit(0) # Exit successfully with empty files

    # Step 4: Fetch Stream URLs
    fetch_and_add_stream_urls(live_channels_final)

    # Step 5: Filter list again for channels where stream URL fetch succeeded
    channels_with_streams = [ch for ch in live_channels_final if ch.get('stream_url')]
    logging.info(f"Proceeding with {len(channels_with_streams)} channels that have stream URLs.")

    if not channels_with_streams:
         logging.warning("No channels with successfully fetched stream URLs. Exiting.")
         # Create empty files
         save_m3u(f'#EXTM3U url-tvg="{EPG_RAW_URL}"\n', os.path.join(OUTPUT_DIR, PLAYLIST_FILENAME))
         save_gzipped_xml(ET.ElementTree(ET.Element('tv')), os.path.join(OUTPUT_DIR, EPG_FILENAME))
         sys.exit(0)

    # Step 6: Fetch EPG Data (using broadcast.json)
    epg_data = fetch_epg_for_channels(channels_with_streams) # Fetch only for channels with streams

    # Step 7: Generate EPG XML
    epg_tree = generate_epg_xml(channels_with_streams, epg_data) # Use list with streams for channel info

    # Step 8: Generate M3U Playlist
    m3u_content = generate_m3u_playlist(channels_with_streams) # Use list with streams

    # Step 9: Save Files
    save_m3u(m3u_content, os.path.join(OUTPUT_DIR, PLAYLIST_FILENAME))
    save_gzipped_xml(epg_tree, os.path.join(OUTPUT_DIR, EPG_FILENAME))

    logging.info("--- Xumo Scraper Finished Successfully ---")
