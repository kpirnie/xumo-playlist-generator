import requests
import json
import os
import gzip
from datetime import datetime, timedelta, timezone
import xml.etree.ElementTree as ET
import logging
import time
import re
import uuid # Needed for IFA placeholder
import sys

# --- Configuration ---
API_ENDPOINT = "https://android-tv-mds.xumo.com/v2" # Use Android TV endpoint
CHANNEL_LIST_ID = "10032" # Hardcoded based on the last script
GEO_ID = "us" # Assume US geo, can be adjusted if needed

CHANNEL_LIST_URL_TEMPLATE = f"{API_ENDPOINT}/channels/list/{CHANNEL_LIST_ID}.json?f=genreId&sort=hybrid&geoId={GEO_ID}"
BROADCAST_NOW_URL_TEMPLATE = f"{API_ENDPOINT}/channels/channel/{{channel_id}}/broadcast.json?hour={{hour_num}}" # For getting current asset ID
ASSET_DETAILS_URL_TEMPLATE = f"{API_ENDPOINT}/assets/asset/{{asset_id}}.json?f=providers" # For getting stream URI
EPG_FETCH_URL_TEMPLATE = f"{API_ENDPOINT}/epg/{CHANNEL_LIST_ID}/{{date_str}}/0.json?limit=50&offset={{offset}}&f=asset.title&f=asset.descriptions" # EPG Endpoint (only page 0)
XUMO_LOGO_URL_TEMPLATE = "https://image.xumo.com/v1/channels/channel/{channel_id}/168x168.png?type=color_onBlack" # From last script

EPG_FETCH_DAYS = 2 # How many days of EPG data to fetch (today + tomorrow)
MAX_EPG_OFFSET = 400 # Max offset to try for EPG fetching (limit 50 -> 8*50=400 channels approx)
API_DELAY_SECONDS = 0.15 # Delay between rapid API calls

OUTPUT_DIR = "playlists"
PLAYLIST_FILENAME = "xumo_playlist.m3u"
EPG_FILENAME = "xumo_epg.xml.gz"

# !!! IMPORTANT: VERIFY / UPDATE THESE !!!
GITHUB_USER = "BuddyChewChew"
GITHUB_REPO = "xumo-playlist-generator" # The name of THIS repository
GITHUB_BRANCH = "main"
EPG_RAW_URL = f"https://raw.githubusercontent.com/{GITHUB_USER}/{GITHUB_REPO}/{GITHUB_BRANCH}/{OUTPUT_DIR}/{EPG_FILENAME}"

HEADERS = {
    'User-Agent': 'okhttp/4.9.3', # From last script example
    # Add Referer/Origin if testing shows they are needed, start minimal
    # 'Referer': 'https://play.xumo.com/',
    # 'Origin': 'https://play.xumo.com'
}
REQUEST_TIMEOUT = 45

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# For more detail during debugging, change level to logging.DEBUG
# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Helper Functions --- (Copied from previous version, check if still needed/correct)

def fetch_data(url, params=None, is_json=True, retries=2, delay=2):
    """Fetches data from a URL, handles JSON parsing and errors, includes retries."""
    logging.debug(f"Fetching {'JSON' if is_json else 'text'} from: {url} with params: {params}")
    for attempt in range(retries + 1):
        try:
            # Add headers to every request
            response = requests.get(url, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            response.raise_for_status()
            if is_json:
                # Check for empty response before JSON decoding
                if not response.content:
                     logging.warning(f"Empty response content received from {url}")
                     return None
                return response.json()
            else:
                return response.content.decode('utf-8', errors='ignore')
        except requests.exceptions.RequestException as e:
            logging.warning(f"Attempt {attempt+1}/{retries+1} failed fetching {url}: {e}")
            if attempt < retries:
                time.sleep(delay)
            elif attempt == retries: # Only log error on final failed attempt
                logging.error(f"Final attempt failed fetching {url}: {e}")
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON from {url}, content: {response.text[:500]}... - {e}")
            break # Don't retry JSON errors
    return None

def format_xmltv_time(dt_obj):
    """Formats a datetime object into XMLTV time format (YYYYMMDDHHMMSS +ZZZZ)."""
    if not isinstance(dt_obj, datetime): return ""
    if not dt_obj.tzinfo: dt_obj = dt_obj.replace(tzinfo=timezone.utc) # Assume UTC
    return dt_obj.strftime('%Y%m%d%H%M%S %z')

def parse_iso_datetime(iso_time_str):
    """Parses ISO 8601 string (handles Z and potential milliseconds) to datetime object."""
    if not iso_time_str: return None
    try:
        # Remove 'Z' and add UTC offset for consistent parsing if Z exists
        if iso_time_str.endswith('Z'):
            iso_time_str = iso_time_str[:-1] + '+00:00'
        # Handle potential different timezone formats from API (e.g., +0000)
        # Try parsing directly first
        dt_obj = datetime.fromisoformat(iso_time_str)
        # Ensure it's UTC for consistency
        return dt_obj.astimezone(timezone.utc)
    except ValueError: # Handle potential milliseconds manually if needed, or other format issues
        try:
            # Example: Handle "2023-10-27T15:00:00.123+00:00" or just "2023-10-27T15:00:00+00:00"
             if '.' in iso_time_str:
                 time_part = iso_time_str.split('.')
                 zone_part = time_part[1].split('+')[-1] if '+' in time_part[1] else time_part[1].split('-')[-1] if '-' in time_part[1] else None
                 offset_sign = '+' if '+' in time_part[1] else '-' if '-' in time_part[1] else None

                 if offset_sign and zone_part:
                      # Pad zone part if needed (e.g., +00:00)
                      if ':' not in zone_part: zone_part = zone_part[:2] + ':' + zone_part[2:]
                      iso_time_str_fmt = f"{time_part[0]}.{time_part[1].split(offset_sign)[0][:6]}{offset_sign}{zone_part}"
                      dt_obj = datetime.fromisoformat(iso_time_str_fmt)
                      return dt_obj.astimezone(timezone.utc)
                 else: # Assume UTC if no offset found after ms
                      dt_obj = datetime.strptime(iso_time_str.split('.')[0], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
                      return dt_obj
             else: # No milliseconds
                 dt_obj = datetime.fromisoformat(iso_time_str)
                 return dt_obj.astimezone(timezone.utc)
        except Exception as e_inner:
            logging.warning(f"Could not parse ISO timestamp '{iso_time_str}': {e_inner}")
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

def process_stream_uri(uri):
    """Replaces placeholders in the stream URI."""
    if not uri: return None
    try:
        uri = uri.replace('[PLATFORM]', "androidtv")
        uri = uri.replace('[APP_VERSION]', "1.0.0") # Placeholder version
        uri = uri.replace('[timestamp]', str(int(time.time()*1000))) # Milliseconds timestamp
        uri = uri.replace('[app_bundle]', "com.xumo.xumo.tv") # From script
        uri = uri.replace('[device_make]', "GitHubAction") # Generic
        uri = uri.replace('[device_model]', "PythonScript") # Generic
        uri = uri.replace('[content_language]', "en")
        uri = uri.replace('[IS_LAT]', "0") # Limit Ad Tracking? Assume 0
        uri = uri.replace('[IFA]', str(uuid.uuid4())) # Random UUID for Identifier for Advertisers
        # Remove any remaining bracketed placeholders
        uri = re.sub(r'\[([^]]+)\]', '', uri)
        return uri
    except Exception as e:
        logging.error(f"Error processing stream URI '{uri[:50]}...': {e}")
        return None


# --- Core Logic Functions ---

def get_live_channels_list():
    """Fetches master list, filters for live/non-DRM channels."""
    logging.info(f"Fetching master channel list from {CHANNEL_LIST_URL_TEMPLATE.format(geo_id=GEO_ID)}...")
    data = fetch_data(CHANNEL_LIST_URL_TEMPLATE.format(geo_id=GEO_ID), is_json=True)

    if not data or 'channel' not in data or 'item' not in data['channel']:
        logging.error("Invalid or empty master channel list response.")
        return []

    live_channels = []
    for item in data['channel'].get('item', []):
        try:
            channel_id = item.get('guid', {}).get('value')
            title = item.get('title')
            callsign = item.get('callsign', '')
            is_live = item.get('properties', {}).get('is_live') == "true"
            number_str = item.get('number')
            genre = item.get('genre', [{}])[0].get('value', 'General') if item.get('genre') else 'General'

            # Filter based on last script's logic
            if callsign.endswith("-DRM") or callsign.endswith("DRM-CMS"):
                logging.debug(f"Skipping DRM channel: {channel_id} ({title})")
                continue
            if not is_live:
                logging.debug(f"Skipping non-live channel: {channel_id} ({title})")
                continue

            if channel_id and title:
                channel_id_str = str(channel_id)
                logo_url = XUMO_LOGO_URL_TEMPLATE.replace("{channel_id}", channel_id_str)
                live_channels.append({
                    'id': channel_id_str,
                    'name': title,
                    'number': number_str,
                    'callsign': callsign,
                    'logo': logo_url,
                    'group': genre,
                    'current_asset_id': None, # Will be fetched next
                    'stream_url': None      # Will be fetched later
                })
            else:
                 logging.warning(f"Skipping channel item due to missing ID or title: {item}")
        except Exception as e:
             logging.warning(f"Error processing channel item {item}: {e}")

    logging.info(f"Found {len(live_channels)} live, non-DRM channels from master list.")
    return live_channels


def fetch_and_add_stream_urls(live_channels_list):
    """Fetches current asset ID and then stream URL for each live channel."""
    logging.info(f"Fetching stream URLs for {len(live_channels_list)} channels...")
    processed_count = 0
    channels_with_streams = []

    for i, channel_info in enumerate(live_channels_list):
        channel_id = channel_info['id']
        logging.debug(f"Processing channel {channel_id} ({channel_info['name']}) ({i+1}/{len(live_channels_list)})")

        # 1. Get current asset ID from broadcast endpoint for current hour
        current_hour = datetime.now(timezone.utc).hour
        broadcast_url = BROADCAST_NOW_URL_TEMPLATE.format(channel_id=channel_id, hour_num=current_hour)
        logging.debug(f"Fetching broadcast info: {broadcast_url}")
        broadcast_data = fetch_data(broadcast_url, is_json=True, retries=1) # Fewer retries for this potentially volatile call

        asset_id = None
        if broadcast_data and 'assets' in broadcast_data and isinstance(broadcast_data['assets'], list) and len(broadcast_data['assets']) > 0:
            asset_id = broadcast_data['assets'][0].get('id')
            if asset_id:
                 channel_info['current_asset_id'] = str(asset_id)
                 logging.debug(f"Found current asset ID {asset_id} for channel {channel_id}")
            else:
                 logging.warning(f"First asset in broadcast data for channel {channel_id} has no ID.")
        else:
            logging.warning(f"Could not get valid broadcast data or assets for channel {channel_id} (Hour: {current_hour})")
            time.sleep(API_DELAY_SECONDS) # Still delay even if failed
            continue # Skip to next channel if we can't get asset ID

        # 2. Get asset details using the asset ID
        asset_details_url = ASSET_DETAILS_URL_TEMPLATE.format(asset_id=asset_id)
        logging.debug(f"Fetching asset details: {asset_details_url}")
        asset_data = fetch_data(asset_details_url, is_json=True)

        raw_stream_uri = None
        if (asset_data and 'providers' in asset_data and
                isinstance(asset_data['providers'], list) and len(asset_data['providers']) > 0 and
                'sources' in asset_data['providers'][0] and
                isinstance(asset_data['providers'][0]['sources'], list) and len(asset_data['providers'][0]['sources']) > 0 and
                'uri' in asset_data['providers'][0]['sources'][0]):
            raw_stream_uri = asset_data['providers'][0]['sources'][0]['uri']
        else:
             logging.warning(f"Could not find sources URI for asset {asset_id} (Channel {channel_id})")
             time.sleep(API_DELAY_SECONDS)
             continue # Skip to next channel

        # 3. Process the URI (replace placeholders)
        processed_stream_url = process_stream_uri(raw_stream_uri)

        if processed_stream_url:
            channel_info['stream_url'] = processed_stream_url
            logging.debug(f"Successfully processed stream URL for channel {channel_id}")
            channels_with_streams.append(channel_info) # Add to the final list only if stream URL is good
            processed_count += 1
        else:
            logging.warning(f"Failed to process stream URI for asset {asset_id} (Channel {channel_id})")

        time.sleep(API_DELAY_SECONDS) # Delay between channels

    logging.info(f"Successfully obtained stream URLs for {processed_count} channels.")
    return channels_with_streams


def fetch_epg_data(channel_list):
    """Fetches EPG data using the offset-based endpoint."""
    if not channel_list: return {}

    logging.info(f"Fetching EPG data for {len(channel_list)} channels...")
    consolidated_epg = {channel['id']: [] for channel in channel_list} # Initialize structure
    assets_cache = {} # Cache asset details found in EPG responses: { asset_id: {details} }
    channel_ids_in_list = {ch['id'] for ch in channel_list} # Faster lookups

    # Determine dates to fetch
    today = datetime.now(timezone.utc)
    dates_to_fetch = [today + timedelta(days=d) for d in range(EPG_FETCH_DAYS)]

    total_requests = 0 # Calculate dynamically

    for date_obj in dates_to_fetch:
        date_str = date_obj.strftime('%Y%m%d')
        offset = 0
        while offset <= MAX_EPG_OFFSET :
            total_requests += 1
            logging.debug(f"Fetching EPG - Date: {date_str}, Offset: {offset}")
            fetch_url = EPG_FETCH_URL_TEMPLATE.format(date_str=date_str, offset=offset)
            page_data = fetch_data(fetch_url, is_json=True)

            if not page_data or 'channels' not in page_data or not isinstance(page_data['channels'], list) or len(page_data['channels']) == 0:
                logging.info(f"No more channels found for date {date_str} at offset {offset}. Moving to next date or finishing.")
                break # Stop fetching offsets for this date

            # Cache asset details from this page
            if 'assets' in page_data and isinstance(page_data['assets'], dict):
                assets_cache.update(page_data['assets'])

            # Process schedules
            found_program_count = 0
            for channel_schedule_data in page_data['channels']:
                channel_id = str(channel_schedule_data.get('channelId'))

                # Only process if this channel is one we care about (in our live list)
                if channel_id in channel_ids_in_list:
                    if channel_id not in consolidated_epg: consolidated_epg[channel_id] = [] # Should exist, but safety check

                    for program_schedule in channel_schedule_data.get('schedule', []):
                        asset_id = program_schedule.get('assetId')
                        asset_details = assets_cache.get(asset_id)
                        if asset_details:
                            program_info = { **program_schedule, **asset_details }
                            consolidated_epg[channel_id].append(program_info)
                            found_program_count +=1
                        else:
                            logging.warning(f"Asset details not found for assetId {asset_id} on channel {channel_id} (Date: {date_str}, Offset: {offset})")

            logging.debug(f"Date: {date_str}, Offset: {offset}: Processed {found_program_count} program entries.")
            offset += 50 # Increment offset for next batch
            time.sleep(API_DELAY_SECONDS) # Delay between offset fetches

    logging.info(f"Finished fetching EPG data after {total_requests} requests.")
    return consolidated_epg

def generate_epg_xml(channel_list_with_streams, consolidated_epg_data):
    """Generates the XMLTV ElementTree object."""
    logging.info("Generating EPG XML structure...")
    tv_element = ET.Element('tv', attrib={'generator-info-name': f'{GITHUB_USER}-{GITHUB_REPO}'})

    programme_count = 0
    # Add channel elements for channels that have streams
    for channel in channel_list_with_streams:
        chan_el = ET.SubElement(tv_element, 'channel', attrib={'id': channel['id']})
        ET.SubElement(chan_el, 'display-name').text = channel['name']
        if channel.get('number'): # Add channel number if available
             ET.SubElement(chan_el, 'display-name').text = channel['number']
        if channel['logo']: ET.SubElement(chan_el, 'icon', attrib={'src': channel['logo']})

    # Add programme elements
    for channel_id, programs in consolidated_epg_data.items():
        # Find the channel name for logging/debugging (optional)
        # channel_name = next((ch['name'] for ch in channel_list_with_streams if ch['id'] == channel_id), channel_id)

        for program in programs:
            try:
                # Parse timestamps using helper
                start_time = parse_iso_datetime(program.get('start')) # Use ISO parser
                end_time = parse_iso_datetime(program.get('end')) # Use ISO parser

                # Details from asset cache
                title = program.get('title', 'Unknown Program')
                desc_obj = program.get('descriptions', {})
                desc = desc_obj.get('large') or desc_obj.get('medium') or desc_obj.get('small') or desc_obj.get('tiny')
                episode_title = program.get('episodeTitle')
                asset_id = program.get('assetId') # Get asset ID for episode num

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

                    # Episode Numbering using assetId based on last script's logic
                    if asset_id:
                        system_type = "dd_progid" if asset_id.startswith("EP") else "dd_seriesid"
                        ET.SubElement(prog_el, 'episode-num', attrib={'system': system_type}).text = asset_id

                    # Add icon, category elements here if desired and available in 'program' dict
                    # icon_url = program.get('some_image_key')
                    # if icon_url: ET.SubElement(prog_el, 'icon', attrib={'src': icon_url})
                    # genres = program.get('genres') # Assuming 'genres' is a list in asset details
                    # if genres and isinstance(genres, list):
                    #    for genre_item in genres: ET.SubElement(prog_el, 'category', attrib={'lang': 'en'}).text = genre_item

                    programme_count += 1
                else:
                    logging.warning(f"Skipping program due to invalid time: {title} (Start: {program.get('start')}, End: {program.get('end')})")

            except Exception as e:
                logging.exception(f"Error processing EPG program item {program} for channel {channel_id}: {e}") # Log full traceback

    logging.info(f"Generated XML structure with {len(channel_list_with_streams)} channels and {programme_count} programmes.")
    return ET.ElementTree(tv_element)

def generate_m3u_playlist(channel_list_with_streams):
    """Generates the M3U playlist string using channels that have stream URLs."""
    logging.info("Generating M3U playlist...")
    playlist_parts = [f'#EXTM3U url-tvg="{EPG_RAW_URL}"\n']
    added_count = 0

    # Sort channels numerically if possible, otherwise alphabetically
    def sort_key(channel):
        try: return int(channel.get('number', '99999'))
        except (ValueError, TypeError): return 99999
    sorted_channels = sorted(channel_list_with_streams, key=lambda x: (sort_key(x), x['name'].lower()))

    for channel in sorted_channels:
        stream_url = channel.get('stream_url') # Should exist if passed here
        channel_id = channel['id']
        display_name = channel['name'].replace(',', '')
        group_title = channel['group'].replace(',', '')

        if stream_url: # Double check stream_url is present
            line1 = f'#EXTINF:-1 tvg-id="{channel_id}" tvg-name="{channel["name"]}" tvg-logo="{channel["logo"]}" group-title="{group_title}",{display_name}\n'
            line2 = f'{stream_url}\n'
            playlist_parts.append(line1)
            playlist_parts.append(line2)
            added_count += 1
        else:
            # This shouldn't happen if list is filtered correctly, but log just in case
             logging.error(f"Channel {channel_id} made it to M3U generation without a stream URL!")

    logging.info(f"Added {added_count} channels with stream URLs to M3U playlist.")
    return "".join(playlist_parts)

# --- Main Execution ---
if __name__ == "__main__":
    logging.info("--- Starting Xumo Scraper (okhttp/Android Strategy) ---")
    ensure_output_dir()

    # Step 1: Get Master Channel List (using hardcoded ID)
    master_channel_map = get_live_channels_list() # Function now fetches and filters
    if not master_channel_map:
        logging.error("Failed to get live channels list. Aborting.")
        sys.exit(1)

    # Step 2: Fetch Stream URLs (requires getting asset ID via broadcast first)
    channels_with_streams = fetch_and_add_stream_urls(master_channel_map)

    if not channels_with_streams:
         logging.warning("No channels found with successfully fetched stream URLs. Generating empty files.")
         save_m3u(f'#EXTM3U url-tvg="{EPG_RAW_URL}"\n', os.path.join(OUTPUT_DIR, PLAYLIST_FILENAME))
         save_gzipped_xml(ET.ElementTree(ET.Element('tv')), os.path.join(OUTPUT_DIR, EPG_FILENAME))
         sys.exit(0) # Exit successfully with empty files

    # Step 3: Fetch EPG Data (using offset method)
    epg_data = fetch_epg_data(channels_with_streams) # Fetch only for channels with streams

    # Step 4: Generate EPG XML
    epg_tree = generate_epg_xml(channels_with_streams, epg_data) # Use list with streams for channel info

    # Step 5: Generate M3U Playlist
    m3u_content = generate_m3u_playlist(channels_with_streams) # Use list with streams

    # Step 6: Save Files
    save_m3u(m3u_content, os.path.join(OUTPUT_DIR, PLAYLIST_FILENAME))
    save_gzipped_xml(epg_tree, os.path.join(OUTPUT_DIR, EPG_FILENAME))

    logging.info("--- Xumo Scraper Finished Successfully ---")
