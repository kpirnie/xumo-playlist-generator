# -*- coding: utf-8 -*-
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
ANDROID_TV_ENDPOINT = "https://android-tv-mds.xumo.com/v2" # Original endpoint
VALENCIA_API_ENDPOINT = "https://valencia-app-mds.xumo.com/v2" # Base URL confirmed by Kodi addon
CHANNEL_LIST_ID = "10032" # Hardcoded (Used for Android TV fallback & EPG)
GEO_ID = "us" # Assume US geo, can be adjusted if needed

# --- Endpoint URLs ---
# Valencia Proxy Attempt (Primary) - Still guessing the exact path
PROXY_CHANNEL_URL = f"{VALENCIA_API_ENDPOINT}/proxy/channels/?geoId={GEO_ID}"

# Android TV Specific URLs (Fallback and EPG)
CHANNEL_LIST_URL_TEMPLATE = f"{ANDROID_TV_ENDPOINT}/channels/list/{CHANNEL_LIST_ID}.json?f=genreId&sort=hybrid&geoId={GEO_ID}"
BROADCAST_NOW_URL_TEMPLATE = f"{ANDROID_TV_ENDPOINT}/channels/channel/{{channel_id}}/broadcast.json?hour={{hour_num}}"
ASSET_DETAILS_URL_TEMPLATE = f"{ANDROID_TV_ENDPOINT}/assets/asset/{{asset_id}}.json?f=providers"
EPG_FETCH_URL_TEMPLATE = f"{ANDROID_TV_ENDPOINT}/epg/{CHANNEL_LIST_ID}/{{date_str}}/0.json?limit=50&offset={{offset}}&f=asset.title&f=asset.descriptions"

# Generic Logo URL
XUMO_LOGO_URL_TEMPLATE = "https://image.xumo.com/v1/channels/channel/{channel_id}/168x168.png?type=color_onBlack"

# --- Script Settings ---
EPG_FETCH_DAYS = 2
MAX_EPG_OFFSET = 400
API_DELAY_SECONDS = 0.15
OUTPUT_DIR = "playlists"
PLAYLIST_FILENAME = "xumo_playlist.m3u"
EPG_FILENAME = "xumo_epg.xml.gz"
REQUEST_TIMEOUT = 45

# !!! IMPORTANT: VERIFY / UPDATE THESE !!!
GITHUB_USER = "BuddyChewChew"
GITHUB_REPO = "xumo-playlist-generator"
GITHUB_BRANCH = "main"
EPG_RAW_URL = f"https://raw.githubusercontent.com/{GITHUB_USER}/{GITHUB_REPO}/{GITHUB_BRANCH}/{OUTPUT_DIR}/{EPG_FILENAME}"

# --- Headers ---
# Headers mimicking Web Browser for Valencia endpoint
WEB_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36', # Updated UA
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Origin': 'https://play.xumo.com', # From Kodi addon info
    'Referer': 'https://play.xumo.com/', # From Kodi addon info
}

# Headers for Android TV endpoint (Fallback/EPG)
ANDROID_TV_HEADERS = {
    'User-Agent': 'okhttp/4.9.3',
    # Keep minimal for now, add Origin/Referer if needed later
}

# --- Logging Setup ---
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# For more detail during debugging, change level to logging.DEBUG
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)


# --- Helper Functions ---

def fetch_data(url, params=None, is_json=True, retries=2, delay=2, headers=WEB_HEADERS): # Default to WEB_HEADERS
    """Fetches data from a URL, handles JSON parsing and errors, includes retries."""
    logging.debug(f"Fetching {'JSON' if is_json else 'text'} from: {url} with params: {params}")
    logging.debug(f"Using Headers: {json.dumps(headers)}") # Log headers being used
    for attempt in range(retries + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            logging.debug(f"Request URL: {response.url}")
            logging.debug(f"Response Status: {response.status_code}")
            # Log response headers if debugging network issues
            if logging.getLogger().level == logging.DEBUG:
                logging.debug(f"Response Headers: {json.dumps(dict(response.headers))}")

            response.raise_for_status() # Check for HTTP errors first

            if is_json:
                if not response.content:
                     logging.warning(f"Empty response content received from {url}")
                     return None
                # Log raw response for debugging the proxy endpoint
                if logging.getLogger().level == logging.DEBUG:
                     try:
                         # Attempt pretty printing JSON for readability
                         parsed_json = response.json()
                         logging.debug(f"Raw JSON Response from {url}:\n{json.dumps(parsed_json, indent=2)}")
                     except json.JSONDecodeError:
                         logging.debug(f"Raw (non-JSON?) Response Text from {url}:\n{response.text[:1500]}...") # Log more text if not JSON
                     except Exception as log_ex:
                         logging.error(f"Error logging raw response: {log_ex}")

                # Return the already parsed JSON to avoid parsing twice
                # Need to handle case where logging failed but original parsing might work
                try:
                    return response.json()
                except json.JSONDecodeError as e_final:
                    logging.error(f"Error decoding JSON from {url} after successful status code. Content: {response.text[:500]}... - {e_final}")
                    return None # Return None on final decode error
            else:
                # Decode text response
                 try:
                     decoded_text = response.content.decode('utf-8', errors='ignore')
                     if logging.getLogger().level == logging.DEBUG:
                          logging.debug(f"Raw Text Response from {url}:\n{decoded_text[:1500]}...")
                     return decoded_text
                 except Exception as decode_ex:
                     logging.error(f"Error decoding text response from {url}: {decode_ex}")
                     return None

        except requests.exceptions.HTTPError as e:
            logging.warning(f"Attempt {attempt+1}/{retries+1} HTTP Error fetching {url}: {e}")
            # Log content on error for debugging
            if response is not None: logging.warning(f"Error Response Content: {response.text[:500]}...")
            if attempt < retries and response is not None and response.status_code not in [401, 403, 404]: # Don't retry auth/not found errors immediately
                time.sleep(delay)
            elif attempt == retries:
                logging.error(f"Final attempt failed fetching {url} with HTTP Error: {e}")
                return None # Return None on final failure
            else: # e.g., 401/403/404 on first try
                break

        except requests.exceptions.RequestException as e:
            logging.warning(f"Attempt {attempt+1}/{retries+1} Network Error fetching {url}: {e}")
            if attempt < retries:
                time.sleep(delay)
            elif attempt == retries:
                logging.error(f"Final attempt failed fetching {url} with Network Error: {e}")
                return None # Return None on final failure

        # JSONDecodeError is handled within the `is_json` block now

    return None # Should only be reached if all retries fail

def format_xmltv_time(dt_obj):
    """Formats a datetime object into XMLTV time format (YYYYMMDDHHMMSS +ZZZZ)."""
    if not isinstance(dt_obj, datetime): return ""
    if not dt_obj.tzinfo: dt_obj = dt_obj.replace(tzinfo=timezone.utc) # Assume UTC
    return dt_obj.strftime('%Y%m%d%H%M%S %z')

def parse_iso_datetime(iso_time_str):
    """Parses ISO 8601 string (handles Z and potential milliseconds) to datetime object."""
    if not iso_time_str: return None
    try:
        # Most robust way: remove 'Z' if present, add '+00:00' if no timezone info
        iso_time_str = iso_time_str.replace('Z', '+00:00')
        if '+' not in iso_time_str and '-' not in iso_time_str[10:]: # Check if timezone offset exists after date part
             iso_time_str += '+00:00' # Assume UTC if no offset provided

        # Handle potential milliseconds by truncating
        if '.' in iso_time_str:
            time_parts = iso_time_str.split('.')
            ms_and_zone = time_parts[1].split('+')
            if len(ms_and_zone) == 1: # No '+', check for '-'
                ms_and_zone = time_parts[1].split('-')
                if len(ms_and_zone) == 1: # No timezone found after ms
                    iso_time_str = time_parts[0] + '+00:00' # Assume UTC
                else:
                     iso_time_str = f"{time_parts[0]}-{ms_and_zone[1]}" # Reconstruct with '-'
            else:
                iso_time_str = f"{time_parts[0]}+{ms_and_zone[1]}" # Reconstruct with '+'

        dt_obj = datetime.fromisoformat(iso_time_str)
        # Ensure it's UTC for consistency
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
        # Use web-like placeholders as we're likely getting URI from Valencia endpoint
        uri = uri.replace('[PLATFORM]', "web") # Changed to web
        uri = uri.replace('[APP_VERSION]', "1.0.0") # Placeholder version (might need updating)
        uri = uri.replace('[timestamp]', str(int(time.time()*1000))) # Milliseconds timestamp
        uri = uri.replace('[app_bundle]', "web.xumo.com") # Changed to web bundle
        uri = uri.replace('[device_make]', "GitHubAction") # Generic
        uri = uri.replace('[device_model]', "PythonScript") # Generic
        uri = uri.replace('[content_language]', "en")
        uri = uri.replace('[IS_LAT]', "0")
        uri = uri.replace('[IFA]', str(uuid.uuid4())) # Random UUID for IFA (or potentially empty for web?)
        # uri = uri.replace('[IFA]', "") # Alternative for web?

        # Add potentially new placeholders based on observation
        uri = uri.replace('[SESSION_ID]', str(uuid.uuid4())) # Add a session ID guess
        uri = uri.replace('[DEVICE_ID]', str(uuid.uuid4().hex)) # Add a device ID guess

        # Remove any remaining bracketed placeholders
        uri = re.sub(r'\[([^]]+)\]', '', uri)
        return uri
    except Exception as e:
        logging.error(f"Error processing stream URI '{uri[:50]}...': {e}")
        return None


# --- Core Logic Functions ---

# <<< Try Valencia Proxy Endpoint >>>
def get_channels_via_proxy():
    """Attempts to fetch channel list and stream URLs via the Valencia proxy endpoint."""
    logging.info(f"Attempting to fetch channel data from Valencia Proxy: {PROXY_CHANNEL_URL}")
    # Use WEB_HEADERS for this attempt
    data = fetch_data(PROXY_CHANNEL_URL, is_json=True, retries=1, headers=WEB_HEADERS)

    if not data:
        logging.warning("Failed to fetch data from Valencia proxy endpoint.")
        return None # Indicate failure

    # *** CRITICAL: Inspect the 'data' structure from DEBUG logs ***
    processed_channels = []
    channel_items = []

    # --- Dynamically find the list of channels ---
    if isinstance(data, list):
         channel_items = data
         logging.debug("Valencia proxy response is a root list.")
    elif isinstance(data, dict):
         # Common keys for lists: 'channels', 'items', 'data', 'list', 'results'
         possible_keys = ['channels', 'items', 'data', 'list', 'results']
         found_key = None
         for key in possible_keys:
             if key in data and isinstance(data[key], list):
                 channel_items = data[key]
                 found_key = key
                 logging.debug(f"Found channel list under key: '{found_key}'")
                 break
         if not found_key:
             logging.error(f"Could not find a channel list in Valencia proxy response dict. Top-level keys: {list(data.keys())}")
             return None
    else:
         logging.error("Valencia proxy response is not a list or a dictionary.")
         return None

    logging.info(f"Found {len(channel_items)} potential channel items in proxy response.")
    if not channel_items:
        logging.warning("Proxy response contained an empty channel list.")
        return None # Treat empty list as failure for fallback logic

    # --- PARSE EACH CHANNEL ITEM (ADAPT KEY NAMES BASED ON ACTUAL RESPONSE) ---
    for item in channel_items:
        if not isinstance(item, dict):
            logging.warning(f"Skipping non-dictionary item in channel list: {item}")
            continue
        try:
            # --- Extract data using multiple potential keys ---
            channel_id = item.get('id') or item.get('channelId') or item.get('guid')
            title = item.get('name') or item.get('title') or item.get('channelName')
            number_str = item.get('number') or item.get('channelNumber')
            callsign = item.get('callsign', '')
            logo_url = item.get('logo') or item.get('channelLogo') or item.get('icon') or item.get('thumbnail')

            # --- Stream URL Extraction (Crucial Part - Needs Debugging) ---
            raw_stream_uri = None
            stream_info = item.get('stream') or item.get('streams') or item.get('playback') or item.get('uris') or item.get('sources')

            if isinstance(stream_info, dict):
                # Look for HLS/M3U8 keys, then general 'url' or 'uri'
                raw_stream_uri = stream_info.get('hls') or stream_info.get('m3u8') or stream_info.get('live') or stream_info.get('url') or stream_info.get('uri')
            elif isinstance(stream_info, list) and len(stream_info) > 0:
                 # Check if list contains strings or dicts
                 first_source = stream_info[0]
                 if isinstance(first_source, str) and ('.m3u8' in first_source or 'manifest' in first_source):
                     raw_stream_uri = first_source
                 elif isinstance(first_source, dict):
                     # Look for uri/url within the first dict
                     raw_stream_uri = first_source.get('uri') or first_source.get('url') or first_source.get('src')
            elif isinstance(stream_info, str) and ('.m3u8' in stream_info or 'manifest' in stream_info):
                 raw_stream_uri = stream_info

            # If still no URI, check top-level keys common for direct links
            if not raw_stream_uri:
                 raw_stream_uri = item.get('streamUrl') or item.get('playbackUrl') or item.get('liveUrl')

            # --- Genre/Group ---
            genre_data = item.get('genre') or item.get('category') or item.get('genres')
            genre = 'General' # Default
            if isinstance(genre_data, str): genre = genre_data
            elif isinstance(genre_data, list) and len(genre_data) > 0:
                genre = genre_data[0] if isinstance(genre_data[0], str) else genre_data[0].get('name', 'General') if isinstance(genre_data[0], dict) else 'General'
            elif isinstance(genre_data, dict): genre = genre_data.get('name', 'General')

            # --- Filtering (Adapt based on proxy response flags) ---
            is_drm = item.get('drm') or item.get('drmProtected') or item.get('isDRM', False)
            if is_drm:
                 logging.debug(f"Skipping DRM channel from proxy: {channel_id} ({title})")
                 continue
            is_live_flag = item.get('is_live', True) # Assume live if not specified
            if not is_live_flag:
                 logging.debug(f"Skipping non-live channel from proxy: {channel_id} ({title})")
                 continue

            # --- Basic Validation ---
            if not channel_id or not title:
                logging.warning(f"Skipping proxy channel item due to missing ID or title: {item}")
                continue

            channel_id_str = str(channel_id)

            # --- Logo URL Processing ---
            final_logo_url = None
            if logo_url:
                 if logo_url.startswith('//'): final_logo_url = 'https:' + logo_url
                 elif logo_url.startswith('/'): final_logo_url = 'https://image.xumo.com' + logo_url # Guess base URL
                 else: final_logo_url = logo_url
            else:
                 # Fallback to standard template only if no logo found in data
                 final_logo_url = XUMO_LOGO_URL_TEMPLATE.replace("{channel_id}", channel_id_str)


            # --- Process Stream URL ---
            processed_stream_url = None
            if raw_stream_uri:
                processed_stream_url = process_stream_uri(raw_stream_uri)
                if not processed_stream_url:
                     logging.warning(f"Found raw stream URI for '{title}' ({channel_id_str}) but failed to process it: {raw_stream_uri[:100]}...")
            else:
                logging.debug(f"No direct stream URI found for channel '{title}' ({channel_id_str}) in proxy response.")

            # --- Store Channel Info ---
            processed_channels.append({
                'id': channel_id_str,
                'name': title,
                'number': str(number_str) if number_str else None,
                'callsign': callsign,
                'logo': final_logo_url,
                'group': genre,
                'stream_url': processed_stream_url, # Store None if not found/processed
            })

        except Exception as e:
             logging.warning(f"Error processing proxy channel item {item.get('id', 'N/A')}: {e}", exc_info=True)

    if not processed_channels:
        logging.warning("Valencia proxy endpoint returned data, but no channels could be successfully processed.")
        return None # Indicate failure

    streams_found = sum(1 for ch in processed_channels if ch['stream_url'])
    logging.info(f"Processed {len(processed_channels)} channels from proxy. Found direct stream URLs for {streams_found} channels.")

    if streams_found > 0: # Consider it successful if we got *any* streams
         logging.info("Using channel list obtained from Valencia proxy.")
         # Filter out channels where stream processing failed completely
         final_proxy_channels = [ch for ch in processed_channels if ch['stream_url']]
         if len(final_proxy_channels) < len(processed_channels):
             logging.warning(f"Filtered out {len(processed_channels) - len(final_proxy_channels)} channels from proxy list due to missing/unprocessed streams.")
         return final_proxy_channels
    else:
         logging.warning("Valencia proxy did not provide usable stream URLs for any channel. Will attempt fallback method.")
         return None # Indicate fallback needed


# --- Original Android TV Fetch Functions (Fallback) ---

def get_live_channels_list_android_tv():
    """Fetches master list from Android TV endpoint, filters for live/non-DRM channels."""
    logging.info(f"Fetching Android TV master channel list from {CHANNEL_LIST_URL_TEMPLATE}...")
    # Use ANDROID_TV_HEADERS for this specific endpoint
    data = fetch_data(CHANNEL_LIST_URL_TEMPLATE, is_json=True, headers=ANDROID_TV_HEADERS)

    if not data or 'channel' not in data or 'item' not in data['channel']:
        logging.error("Invalid or empty master channel list response from Android TV endpoint.")
        return []

    live_channels = []
    # ... (parsing logic remains the same) ...
    for item in data['channel'].get('item', []):
        try:
            channel_id = item.get('guid', {}).get('value')
            title = item.get('title')
            callsign = item.get('callsign', '')
            is_live = item.get('properties', {}).get('is_live') == "true"
            number_str = item.get('number')
            genre = item.get('genre', [{}])[0].get('value', 'General') if item.get('genre') else 'General'

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
                    'current_asset_id': None,
                    'stream_url': None
                })
            else:
                 logging.warning(f"Skipping Android channel item due to missing ID or title: {item}")
        except Exception as e:
             logging.warning(f"Error processing Android channel item {item}: {e}", exc_info=True)

    logging.info(f"Found {len(live_channels)} live, non-DRM channels from Android TV master list.")
    return live_channels


def fetch_and_add_stream_urls_android_tv(live_channels_list):
    """Fetches current asset ID and then stream URL for each live channel using Android TV endpoints."""
    logging.info(f"Fetching stream URLs via Android TV asset lookup for {len(live_channels_list)} channels...")
    processed_count = 0
    channels_with_streams = []

    for i, channel_info in enumerate(live_channels_list):
        channel_id = channel_info['id']
        logging.debug(f"Android TV Fetch: Processing channel {channel_id} ({channel_info['name']}) ({i+1}/{len(live_channels_list)})")

        # 1. Get current asset ID
        current_hour = datetime.now(timezone.utc).hour
        broadcast_url = BROADCAST_NOW_URL_TEMPLATE.format(channel_id=channel_id, hour_num=current_hour)
        logging.debug(f"Fetching broadcast info: {broadcast_url}")
        # Use ANDROID_TV_HEADERS
        broadcast_data = fetch_data(broadcast_url, is_json=True, retries=1, headers=ANDROID_TV_HEADERS)

        asset_id = None
        # ... (logic for finding current asset ID remains the same) ...
        if broadcast_data and 'assets' in broadcast_data and isinstance(broadcast_data['assets'], list) and len(broadcast_data['assets']) > 0:
            now_utc = datetime.now(timezone.utc)
            current_asset = None
            for asset in broadcast_data['assets']:
                start_time = parse_iso_datetime(asset.get('start'))
                end_time = parse_iso_datetime(asset.get('end'))
                if start_time and end_time and start_time <= now_utc < end_time:
                    current_asset = asset
                    break
            if not current_asset: current_asset = broadcast_data['assets'][0]

            asset_id = current_asset.get('id')
            if asset_id:
                 channel_info['current_asset_id'] = str(asset_id)
                 logging.debug(f"Found current asset ID {asset_id} for channel {channel_id}")
            else:
                 logging.warning(f"Relevant asset in broadcast data for channel {channel_id} has no ID.")
        else:
            logging.warning(f"Could not get valid broadcast data or assets for channel {channel_id} (Hour: {current_hour})")
            time.sleep(API_DELAY_SECONDS)
            continue

        if not asset_id:
             logging.warning(f"No asset ID found for channel {channel_id}, cannot get stream URL.")
             time.sleep(API_DELAY_SECONDS)
             continue

        # 2. Get asset details
        asset_details_url = ASSET_DETAILS_URL_TEMPLATE.format(asset_id=asset_id)
        logging.debug(f"Fetching asset details: {asset_details_url}")
        # Use ANDROID_TV_HEADERS
        asset_data = fetch_data(asset_details_url, is_json=True, headers=ANDROID_TV_HEADERS)

        raw_stream_uri = None
        # ... (logic for extracting URI from asset details remains the same) ...
        if asset_data and 'providers' in asset_data and isinstance(asset_data['providers'], list):
            for provider in asset_data['providers']:
                 if ('sources' in provider and isinstance(provider['sources'], list)):
                     for source in provider['sources']:
                         if source.get('uri') and (source.get('type') == 'application/x-mpegURL' or source.get('uri', '').endswith('.m3u8')):
                             raw_stream_uri = source['uri']
                             break
                         elif source.get('uri') and not raw_stream_uri:
                             raw_stream_uri = source['uri']
                     if raw_stream_uri: break
        else:
             logging.warning(f"Could not find providers or sources for asset {asset_id} (Channel {channel_id})")


        if not raw_stream_uri:
            logging.warning(f"No stream URI found in sources for asset {asset_id} (Channel {channel_id})")
            time.sleep(API_DELAY_SECONDS)
            continue

        # 3. Process the URI (Use generic processor, might need Android-specific one if placeholders differ)
        processed_stream_url = process_stream_uri(raw_stream_uri) # Assuming placeholders are similar enough for now

        if processed_stream_url:
            channel_info['stream_url'] = processed_stream_url
            logging.debug(f"Successfully processed stream URL for channel {channel_id} via Android TV method")
            channels_with_streams.append(channel_info)
            processed_count += 1
        else:
            logging.warning(f"Failed to process Android TV stream URI for asset {asset_id} (Channel {channel_id})")

        time.sleep(API_DELAY_SECONDS)

    logging.info(f"Successfully obtained stream URLs for {processed_count} channels via Android TV method.")
    return channels_with_streams


# --- EPG Functions (Still uses Android TV endpoint) ---

def fetch_epg_data(channel_list):
    """Fetches EPG data using the Android TV EPG endpoint."""
    if not channel_list: return {}

    logging.info(f"Fetching EPG data for {len(channel_list)} channels (using {ANDROID_TV_ENDPOINT})...")
    consolidated_epg = {channel['id']: [] for channel in channel_list}
    assets_cache = {}
    channel_ids_in_list = {ch['id'] for ch in channel_list}

    today = datetime.now(timezone.utc)
    dates_to_fetch = [today + timedelta(days=d) for d in range(EPG_FETCH_DAYS)]
    total_requests = 0

    for date_obj in dates_to_fetch:
        date_str = date_obj.strftime('%Y%m%d')
        offset = 0
        while offset <= MAX_EPG_OFFSET:
            total_requests += 1
            logging.debug(f"Fetching EPG - Date: {date_str}, Offset: {offset}")
            fetch_url = EPG_FETCH_URL_TEMPLATE.format(date_str=date_str, offset=offset)
            # Use ANDROID_TV_HEADERS for EPG endpoint
            page_data = fetch_data(fetch_url, is_json=True, headers=ANDROID_TV_HEADERS)

            if not page_data or 'channels' not in page_data or not isinstance(page_data['channels'], list) or len(page_data['channels']) == 0:
                logging.info(f"No more channels found for date {date_str} at offset {offset}.")
                break

            if 'assets' in page_data and isinstance(page_data['assets'], dict):
                assets_cache.update(page_data['assets'])

            found_program_count = 0
            processed_channel_count_this_page = 0
            for channel_schedule_data in page_data['channels']:
                channel_id = str(channel_schedule_data.get('channelId'))
                if channel_id in channel_ids_in_list:
                    processed_channel_count_this_page += 1
                    if channel_id not in consolidated_epg: consolidated_epg[channel_id] = []

                    for program_schedule in channel_schedule_data.get('schedule', []):
                        asset_id = program_schedule.get('assetId')
                        asset_details = assets_cache.get(asset_id)
                        if asset_details:
                            program_info = {
                                'start': program_schedule.get('start'),
                                'end': program_schedule.get('end'),
                                'assetId': asset_id,
                                'title': asset_details.get('title', 'Unknown Program'),
                                'descriptions': asset_details.get('descriptions',{}),
                                'episodeTitle': asset_details.get('episodeTitle'),
                            }
                            consolidated_epg[channel_id].append(program_info)
                            found_program_count +=1
                        else:
                            logging.warning(f"EPG: Asset details not found for assetId {asset_id} on channel {channel_id} (Date: {date_str}, Offset: {offset})")

            logging.debug(f"EPG - Date: {date_str}, Offset: {offset}: Processed {processed_channel_count_this_page} relevant channels, found {found_program_count} program entries.")
            offset += 50
            time.sleep(API_DELAY_SECONDS)

    logging.info(f"Finished fetching EPG data after {total_requests} requests.")
    return consolidated_epg

# --- Generate M3U and EPG XML (Unchanged logic) ---

def generate_epg_xml(channel_list_with_streams, consolidated_epg_data):
    # ... (function content remains the same)
    logging.info("Generating EPG XML structure...")
    tv_element = ET.Element('tv', attrib={'generator-info-name': f'{GITHUB_USER}-{GITHUB_REPO}'})
    programme_count = 0
    channel_ids_in_list = {c['id'] for c in channel_list_with_streams} # For checking validity

    # Add channel elements
    for channel in channel_list_with_streams:
        chan_el = ET.SubElement(tv_element, 'channel', attrib={'id': channel['id']})
        ET.SubElement(chan_el, 'display-name').text = channel['name']
        if channel.get('number'):
             ET.SubElement(chan_el, 'display-name').text = channel['number']
        if channel['logo']: ET.SubElement(chan_el, 'icon', attrib={'src': channel['logo']})

    # Add programme elements
    for channel_id, programs in consolidated_epg_data.items():
        if channel_id not in channel_ids_in_list:
             logging.debug(f"Skipping EPG programs for channel {channel_id} as it's not in the final M3U list.")
             continue

        for program in programs:
            try:
                start_time = parse_iso_datetime(program.get('start'))
                end_time = parse_iso_datetime(program.get('end'))
                title = program.get('title', 'Unknown Program')
                desc_obj = program.get('descriptions', {})
                desc = desc_obj.get('large') or desc_obj.get('medium') or desc_obj.get('small') or desc_obj.get('tiny')
                episode_title = program.get('episodeTitle')
                asset_id = program.get('assetId')

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
                    if asset_id:
                        system_type = "dd_progid" if asset_id.startswith("EP") else "dd_assetid"
                        ET.SubElement(prog_el, 'episode-num', attrib={'system': system_type}).text = asset_id
                    programme_count += 1
                else:
                    logging.warning(f"Skipping program due to invalid time: {title} (Start: {program.get('start')}, End: {program.get('end')})")
            except Exception as e:
                logging.exception(f"Error processing EPG program item {program.get('assetId', 'N/A')} for channel {channel_id}: {e}")

    logging.info(f"Generated XML structure with {len(channel_list_with_streams)} channels and {programme_count} programmes.")
    return ET.ElementTree(tv_element)


def generate_m3u_playlist(channel_list_with_streams):
    # ... (function content remains the same)
    logging.info("Generating M3U playlist...")
    playlist_parts = [f'#EXTM3U url-tvg="{EPG_RAW_URL}"\n']
    added_count = 0

    def sort_key(channel):
        try: return int(channel.get('number', '99999'))
        except (ValueError, TypeError): return 99999
    sorted_channels = sorted(channel_list_with_streams, key=lambda x: (sort_key(x), x['name'].lower()))

    for channel in sorted_channels:
        stream_url = channel.get('stream_url')
        channel_id = channel['id']
        display_name = channel['name'].replace(',', ';')
        group_title = channel.get('group', 'General').replace(',', ';')
        tvg_name = channel['name'].replace('"', "'")

        if stream_url:
            line1 = f'#EXTINF:-1 tvg-id="{channel_id}" tvg-name="{tvg_name}" tvg-logo="{channel["logo"]}" group-title="{group_title}",{display_name}\n'
            line2 = f'{stream_url}\n'
            playlist_parts.append(line1)
            playlist_parts.append(line2)
            added_count += 1
        else:
             logging.error(f"Channel {channel_id} ('{channel['name']}') made it to M3U generation without a stream URL!")

    logging.info(f"Added {added_count} channels with stream URLs to M3U playlist.")
    return "".join(playlist_parts)


# --- Main Execution ---
if __name__ == "__main__":
    logging.info("--- Starting Xumo Scraper ---")
    ensure_output_dir()

    final_channel_list = None

    # --- Strategy: Try Proxy First, Fallback to Android TV ---
    logging.info("Step 1: Attempting to get channels via Valencia Proxy endpoint...")
    proxy_channels = get_channels_via_proxy()

    if proxy_channels:
        # Proxy function now returns only channels with successfully processed streams
        logging.info(f"Successfully obtained {len(proxy_channels)} channels with streams via Valencia Proxy.")
        final_channel_list = proxy_channels
    else:
        logging.warning("Valencia proxy method failed or yielded no channels with streams.")

    # --- Fallback to Android TV Method if Proxy Failed ---
    if not final_channel_list:
        logging.info("--- Initiating Fallback: Android TV Method ---")
        logging.info("Step 1 (Fallback): Getting channel list via Android TV endpoint...")
        master_channel_map = get_live_channels_list_android_tv()
        if not master_channel_map:
            logging.error("Fallback Failed: Could not get master channel list. Aborting.")
            sys.exit(1)

        logging.info("Step 2 (Fallback): Fetching stream URLs via Android TV asset lookup...")
        channels_with_streams = fetch_and_add_stream_urls_android_tv(master_channel_map)
        final_channel_list = channels_with_streams # Use the result from the fallback

    # --- Process the final list ---
    if not final_channel_list:
         logging.warning("No channels with stream URLs found after trying all methods. Generating empty files.")
         # Create empty M3U
         save_m3u(f'#EXTM3U url-tvg="{EPG_RAW_URL}"\n', os.path.join(OUTPUT_DIR, PLAYLIST_FILENAME))
         # Create empty EPG XML Tree and save it gzipped
         empty_root = ET.Element('tv')
         empty_tree = ET.ElementTree(empty_root)
         save_gzipped_xml(empty_tree, os.path.join(OUTPUT_DIR, EPG_FILENAME))
         logging.info("Generated empty playlist and EPG files.")
         sys.exit(0)

    logging.info(f"Proceeding with {len(final_channel_list)} channels having stream URLs.")

    # Step 3: Fetch EPG Data (Still uses Android TV endpoint)
    epg_data = fetch_epg_data(final_channel_list)

    # Step 4: Generate EPG XML
    epg_tree = generate_epg_xml(final_channel_list, epg_data)

    # Step 5: Generate M3U Playlist
    m3u_content = generate_m3u_playlist(final_channel_list)

    # Step 6: Save Files
    save_m3u(m3u_content, os.path.join(OUTPUT_DIR, PLAYLIST_FILENAME))
    save_gzipped_xml(epg_tree, os.path.join(OUTPUT_DIR, EPG_FILENAME))

    logging.info("--- Xumo Scraper Finished Successfully ---")
