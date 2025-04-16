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
ANDROID_TV_ENDPOINT = "https://android-tv-mds.xumo.com/v2"
VALENCIA_API_ENDPOINT = "https://valencia-app-mds.xumo.com/v2"
GEO_ID = "us"

# --- List IDs ---
VALENCIA_LIST_ID = "10006"
ANDROID_TV_LIST_ID = "10032"

# --- Endpoint URLs ---
PROXY_CHANNEL_LIST_URL = f"{VALENCIA_API_ENDPOINT}/proxy/channels/list/{VALENCIA_LIST_ID}.json?geoId={GEO_ID}"
ANDROID_TV_CHANNEL_LIST_URL = f"{ANDROID_TV_ENDPOINT}/channels/list/{ANDROID_TV_LIST_ID}.json?f=genreId&sort=hybrid&geoId={GEO_ID}"
BROADCAST_NOW_URL_TEMPLATE = f"{ANDROID_TV_ENDPOINT}/channels/channel/{{channel_id}}/broadcast.json?hour={{hour_num}}"
ASSET_DETAILS_URL_TEMPLATE = f"{ANDROID_TV_ENDPOINT}/assets/asset/{{asset_id}}.json?f=providers"
EPG_FETCH_URL_TEMPLATE = f"{ANDROID_TV_ENDPOINT}/epg/{ANDROID_TV_LIST_ID}/{{date_str}}/0.json?limit=50&offset={{offset}}&f=asset.title&f=asset.descriptions"
XUMO_LOGO_URL_TEMPLATE = "https://image.xumo.com/v1/channels/channel/{channel_id}/168x168.png?type=color_onBlack"

# --- Script Settings ---
EPG_FETCH_DAYS = 2
MAX_EPG_OFFSET = 400
API_DELAY_SECONDS = 0.15
OUTPUT_DIR = "playlists"
PLAYLIST_FILENAME = "xumo_playlist.m3u"
EPG_FILENAME = "xumo_epg.xml.gz"
REQUEST_TIMEOUT = 45

# !!! GitHub Repo Info !!!
GITHUB_USER = "BuddyChewChew"
GITHUB_REPO = "xumo-playlist-generator"
GITHUB_BRANCH = "main"
EPG_RAW_URL = f"https://raw.githubusercontent.com/{GITHUB_USER}/{GITHUB_REPO}/{GITHUB_BRANCH}/{OUTPUT_DIR}/{EPG_FILENAME}"

# --- Headers ---
WEB_HEADERS = { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36', 'Accept': 'application/json, text/plain, */*', 'Accept-Language': 'en-US,en;q=0.9', 'Origin': 'https://play.xumo.com', 'Referer': 'https://play.xumo.com/', }
ANDROID_TV_HEADERS = { 'User-Agent': 'okhttp/4.9.3', }

# --- Logging Setup ---
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - [%(funcName)s] %(message)s', stream=sys.stdout)


# --- Helper Functions ---

# <<< CORRECTED fetch_data function >>>
def fetch_data(url, params=None, is_json=True, retries=2, delay=2, headers=WEB_HEADERS):
    """Fetches data from a URL, handles JSON parsing and errors, includes retries."""
    logging.debug(f"URL: {url}, Params: {params}")
    logging.debug(f"Headers: {json.dumps(headers)}")
    for attempt in range(retries + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            logging.debug(f"Request URL: {response.url}")
            logging.debug(f"Response Status: {response.status_code}")
            response.raise_for_status()

            if is_json:
                if not response.content:
                    logging.warning(f"Empty response content received from {url}")
                    return None
                try:
                    parsed_json = response.json()
                    return parsed_json
                except json.JSONDecodeError as e_final:
                    logging.error(f"Error decoding JSON. Content: {response.text[:500]}... - {e_final}")
                    if logging.getLogger().level == logging.DEBUG:
                        logging.debug(f"Full Text:\n{response.text}")
                    return None
            else:
                # --- CORRECTED BLOCK for non-JSON response ---
                 try:
                     decoded_text = response.content.decode('utf-8', errors='ignore')
                     if logging.getLogger().level == logging.DEBUG:
                         logging.debug(f"Raw Text Response:\n{decoded_text[:1500]}...")
                     return decoded_text
                 except Exception as decode_ex:
                     logging.error(f"Error decoding text response: {decode_ex}")
                     return None
                # --- END OF CORRECTION ---

        except requests.exceptions.HTTPError as e:
            logging.warning(f"Attempt {attempt+1}/{retries+1} HTTP Error: {e}")
            if response is not None: logging.warning(f"Error Response Content: {response.text[:500]}...")
            if attempt < retries and response is not None and response.status_code not in [401, 403, 404, 429]:
                time.sleep(delay * (attempt + 1))
            elif attempt == retries:
                logging.error(f"Final attempt failed with HTTP Error: {e}")
                return None
            else:
                break
        except requests.exceptions.RequestException as e:
            logging.warning(f"Attempt {attempt+1}/{retries+1} Network Error: {e}")
            if attempt < retries:
                time.sleep(delay * (attempt + 1))
            elif attempt == retries:
                logging.error(f"Final attempt failed with Network Error: {e}")
                return None
    return None
# <<< END OF CORRECTED fetch_data function >>>

def parse_iso_datetime(iso_time_str):
    """Parses ISO 8601 string, handling 'Z', milliseconds, and '+HHMM' timezone format."""
    if not iso_time_str: logging.debug("parse_iso_datetime received empty string."); return None
    try:
        original_str = iso_time_str
        if iso_time_str.endswith('Z'): iso_time_str = iso_time_str[:-1] + '+00:00'
        if '.' in iso_time_str:
            offset_str = ""; plus_index = iso_time_str.rfind('+'); minus_index = iso_time_str.rfind('-'); t_index = iso_time_str.find('T'); offset_index = -1
            if plus_index > t_index: offset_index = plus_index
            if minus_index > t_index: offset_index = max(offset_index, minus_index)
            if offset_index != -1: offset_str = iso_time_str[offset_index:]; iso_time_str = iso_time_str[:offset_index]
            iso_time_str = iso_time_str.split('.', 1)[0]; iso_time_str += offset_str
        if len(iso_time_str) >= 5 and iso_time_str[-5] in ['+', '-'] and iso_time_str[-4:].isdigit():
             if ':' not in iso_time_str[-5:]: iso_time_str = iso_time_str[:-2] + ':' + iso_time_str[-2:]; logging.debug(f"Inserted colon in timezone offset: {iso_time_str}")
        if '+' not in iso_time_str[10:] and '-' not in iso_time_str[10:]: logging.debug(f"Adding default +00:00 offset to '{iso_time_str}'"); iso_time_str += "+00:00"
        dt_obj = datetime.fromisoformat(iso_time_str)
        return dt_obj.astimezone(timezone.utc)
    except Exception as e: logging.warning(f"Parse failed for input '{original_str}' (processed as '{iso_time_str}'): {e}"); return None

# <<< FINAL MODIFIED format_xmltv_time function >>>
def format_xmltv_time(dt_obj):
    """Formats datetime object into XMLTV time (YYYYMMDDHHMMSS +HHMM)."""
    if not isinstance(dt_obj, datetime):
        logging.warning(f"format_xmltv_time received non-datetime object: {type(dt_obj)}")
        return ""
    if not dt_obj.tzinfo: dt_obj_utc = dt_obj.replace(tzinfo=timezone.utc); logging.debug(f"format_xmltv_time: Input was naive, assumed UTC: {dt_obj_utc}")
    else: dt_obj_utc = dt_obj.astimezone(timezone.utc); logging.debug(f"format_xmltv_time: Input had timezone, converted to UTC: {dt_obj_utc}")
    main_part = dt_obj_utc.strftime('%Y%m%d%H%M%S'); offset_part = dt_obj_utc.strftime('%z')
    offset_part_clean = offset_part.replace(':', '')
    full_time_str = f"{main_part} {offset_part_clean}"
    logging.debug(f"Formatted time: {full_time_str}"); return full_time_str
# <<< END OF FINAL MODIFIED format_xmltv_time function >>>

def ensure_output_dir():
    if not os.path.exists(OUTPUT_DIR):
        logging.info(f"Creating output directory: {OUTPUT_DIR}")
        try: os.makedirs(OUTPUT_DIR)
        except OSError as e: logging.error(f"Failed to create directory {OUTPUT_DIR}: {e}"); raise

# <<< MODIFIED save_gzipped_xml function >>>
ADD_XMLTV_DOCTYPE = True # Set to True to add <!DOCTYPE...>

def save_gzipped_xml(tree, filepath):
    """Saves the ElementTree XML to a gzipped file, optionally adding DOCTYPE."""
    try:
        if ADD_XMLTV_DOCTYPE:
            xml_partial_string = ET.tostring(tree.getroot(), encoding='unicode', method='xml')
            xml_full_string = f'''<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE tv SYSTEM "xmltv.dtd">
{xml_partial_string}'''
            xml_bytes = xml_full_string.encode('utf-8'); logging.debug("Adding DOCTYPE to XML output.")
        else:
            xml_bytes = ET.tostring(tree.getroot(), encoding='UTF-8', xml_declaration=True); logging.debug("Saving XML without DOCTYPE.")
        with gzip.open(filepath, 'wb') as f: f.write(xml_bytes)
        logging.info(f"Gzipped EPG XML file saved: {filepath}")
    except Exception as e: logging.error(f"Error writing gzipped EPG file {filepath}: {e}")
# <<< END OF MODIFIED save_gzipped_xml function >>>

def save_m3u(content, filepath):
    try:
        with open(filepath, 'w', encoding='utf-8') as f: f.write(content)
        logging.info(f"M3U playlist file saved: {filepath}")
    except Exception as e: logging.error(f"Error writing M3U file {filepath}: {e}")

def process_stream_uri(uri):
    if not uri: return None
    try:
        uri = uri.replace('[PLATFORM]', "web"); uri = uri.replace('[APP_VERSION]', "1.0.0"); uri = uri.replace('[timestamp]', str(int(time.time()*1000)))
        uri = uri.replace('[app_bundle]', "web.xumo.com"); uri = uri.replace('[device_make]', "GitHubAction"); uri = uri.replace('[device_model]', "PythonScript")
        uri = uri.replace('[content_language]', "en"); uri = uri.replace('[IS_LAT]', "0"); uri = uri.replace('[IFA]', str(uuid.uuid4()))
        uri = uri.replace('[SESSION_ID]', str(uuid.uuid4())); uri = uri.replace('[DEVICE_ID]', str(uuid.uuid4().hex))
        uri = re.sub(r'\[([^]]+)\]', '', uri)
        return uri
    except Exception as e: logging.error(f"Error processing stream URI '{uri[:50]}...': {e}"); return None

# --- Core Logic Functions ---
# ... (Keep get_channels_via_proxy_list, get_live_channels_list_android_tv, fetch_stream_urls_via_asset_lookup, fetch_epg_data functions the same as the last version) ...
def get_channels_via_proxy_list():
    logging.info(f"Attempting Valencia Proxy List: {PROXY_CHANNEL_LIST_URL}")
    data = fetch_data(PROXY_CHANNEL_LIST_URL, is_json=True, retries=1, headers=WEB_HEADERS)
    if not data or not isinstance(data, dict): logging.warning(f"Failed to fetch valid dictionary data from Valencia proxy list endpoint."); return None
    processed_channels = []; channel_items = []
    if 'channel' in data and isinstance(data['channel'], dict) and 'item' in data['channel'] and isinstance(data['channel']['item'], list):
        channel_items = data['channel']['item']; logging.debug("Found channel list under data['channel']['item']")
    elif 'items' in data and isinstance(data['items'], list):
        channel_items = data['items']; logging.debug("Found channel list under data['items']")
    else: logging.error(f"Could not find channel list in Valencia proxy list response. Top-level keys: {list(data.keys())}"); return None
    logging.info(f"Found {len(channel_items)} potential channel items in Valencia list response.")
    if not channel_items: logging.warning("Valencia list response contained an empty channel list."); return None
    for item in channel_items:
        if not isinstance(item, dict): logging.warning(f"Skipping non-dictionary item in channel list: {item}"); continue
        try:
            channel_id = item.get('guid', {}).get('value') or item.get('id'); title = item.get('title') or item.get('name')
            number_str = item.get('number'); callsign = item.get('callsign', ''); logo_url = item.get('images', {}).get('logo') or item.get('logo')
            genre_list = item.get('genre'); genre = 'General'
            if isinstance(genre_list, list) and len(genre_list) > 0 and isinstance(genre_list[0], dict): genre = genre_list[0].get('value', 'General')
            elif isinstance(genre_list, str): genre = genre_list
            raw_stream_uri = None; stream_info = item.get('stream') or item.get('streams') or item.get('playback') or item.get('providers')
            if isinstance(stream_info, dict): raw_stream_uri = stream_info.get('hls') or stream_info.get('m3u8') or stream_info.get('live') or stream_info.get('url') or stream_info.get('uri')
            elif isinstance(stream_info, list) and len(stream_info) > 0:
                 for provider in stream_info:
                     if isinstance(provider, dict) and 'sources' in provider and isinstance(provider['sources'], list):
                         for source in provider['sources']:
                             if isinstance(source, dict) and source.get('uri') and (source.get('type') == 'application/x-mpegURL' or source.get('uri','').endswith('.m3u8')): raw_stream_uri = source['uri']; break
                         if raw_stream_uri: break
            properties = item.get('properties', {}); is_live = properties.get('is_live') == "true"; is_drm = callsign.endswith("-DRM") or callsign.endswith("DRM-CMS")
            if is_drm: logging.debug(f"Skipping potential DRM channel: {channel_id} ({title})"); continue
            if not is_live: logging.debug(f"Skipping non-live channel: {channel_id} ({title})"); continue
            if not channel_id or not title: logging.warning(f"Skipping item due to missing ID or title: {item}"); continue
            channel_id_str = str(channel_id); final_logo_url = None
            if logo_url:
                 if logo_url.startswith('//'): final_logo_url = 'https:' + logo_url
                 elif logo_url.startswith('/'): final_logo_url = 'https://image.xumo.com' + logo_url
                 else: final_logo_url = logo_url
            else: final_logo_url = XUMO_LOGO_URL_TEMPLATE.replace("{channel_id}", channel_id_str)
            processed_stream_url = None
            if raw_stream_uri:
                processed_stream_url = process_stream_uri(raw_stream_uri)
                if not processed_stream_url: logging.warning(f"Found raw stream URI for '{title}' ({channel_id_str}) but failed to process it: {raw_stream_uri[:100]}...")
            else: logging.debug(f"No direct stream URI found for channel '{title}' ({channel_id_str}) in Valencia list item.")
            processed_channels.append({ 'id': channel_id_str, 'name': title, 'number': str(number_str) if number_str else None, 'callsign': callsign, 'logo': final_logo_url, 'group': genre, 'stream_url': processed_stream_url, })
        except Exception as e: logging.warning(f"Error processing Valencia list item {item.get('id', 'N/A')}: {e}", exc_info=True)
    if not processed_channels: logging.warning("Valencia list endpoint returned data, but no channels could be successfully processed."); return None
    logging.info(f"Successfully processed {len(processed_channels)} channels from Valencia list endpoint.")
    return processed_channels

def get_live_channels_list_android_tv():
    logging.info(f"Fetching Android TV Fallback List: {ANDROID_TV_CHANNEL_LIST_URL}")
    data = fetch_data(ANDROID_TV_CHANNEL_LIST_URL, is_json=True, headers=ANDROID_TV_HEADERS)
    if not data or 'channel' not in data or 'item' not in data['channel']: logging.error("Invalid or empty list response from Android TV endpoint."); return []
    live_channels = []
    for item in data['channel'].get('item', []):
        try:
            channel_id = item.get('guid', {}).get('value'); title = item.get('title'); callsign = item.get('callsign', '')
            properties = item.get('properties', {}); is_live = properties.get('is_live') == "true"; number_str = item.get('number')
            genre_list = item.get('genre'); genre = 'General'
            if isinstance(genre_list, list) and len(genre_list) > 0 and isinstance(genre_list[0], dict): genre = genre_list[0].get('value', 'General')
            if callsign.endswith("-DRM") or callsign.endswith("DRM-CMS"): logging.debug(f"Skipping DRM channel: {channel_id} ({title})"); continue
            if not is_live: logging.debug(f"Skipping non-live channel: {channel_id} ({title})"); continue
            if channel_id and title:
                channel_id_str = str(channel_id); logo_url = XUMO_LOGO_URL_TEMPLATE.replace("{channel_id}", channel_id_str)
                live_channels.append({ 'id': channel_id_str, 'name': title, 'number': number_str, 'callsign': callsign, 'logo': logo_url, 'group': genre, 'stream_url': None })
            else: logging.warning(f"Skipping Android channel item due to missing ID or title: {item}")
        except Exception as e: logging.warning(f"Error processing Android channel item {item}: {e}", exc_info=True)
    logging.info(f"Found {len(live_channels)} live, non-DRM channels from Android TV fallback list.")
    return live_channels

def fetch_stream_urls_via_asset_lookup(channels_list):
    logging.info(f"Attempting Android TV asset lookup for {len(channels_list)} channels...")
    processed_count = 0; channels_with_streams = []
    for i, channel_info in enumerate(channels_list):
        channel_id = channel_info['id']
        if channel_info.get('stream_url'): logging.debug(f"Stream URL already present for {channel_id}, skipping asset lookup."); channels_with_streams.append(channel_info); continue
        logging.debug(f"Asset Lookup: Processing {channel_id} ({channel_info['name']}) ({i+1}/{len(channels_list)})")
        current_hour = datetime.now(timezone.utc).hour; broadcast_url = BROADCAST_NOW_URL_TEMPLATE.format(channel_id=channel_id, hour_num=current_hour)
        logging.debug(f"Fetching broadcast info: {broadcast_url}"); broadcast_data = fetch_data(broadcast_url, is_json=True, retries=1, headers=ANDROID_TV_HEADERS)
        asset_id = None
        if broadcast_data and 'assets' in broadcast_data and isinstance(broadcast_data['assets'], list) and len(broadcast_data['assets']) > 0:
            now_utc = datetime.now(timezone.utc); current_asset = None
            for asset in broadcast_data['assets']:
                start_time = parse_iso_datetime(asset.get('start')); end_time = parse_iso_datetime(asset.get('end'))
                if start_time and end_time and start_time <= now_utc < end_time: current_asset = asset; break
            if not current_asset and broadcast_data['assets']: current_asset = broadcast_data['assets'][0]
            if current_asset: asset_id = current_asset.get('id')
            if asset_id: logging.debug(f"Found current asset ID {asset_id} for channel {channel_id}")
            else: logging.warning(f"Relevant asset in broadcast data for channel {channel_id} has no ID.")
        else: logging.warning(f"Could not get valid broadcast data or assets for channel {channel_id} (Hour: {current_hour})"); time.sleep(API_DELAY_SECONDS); continue
        if not asset_id: logging.warning(f"No asset ID found for channel {channel_id}, cannot get stream URL."); time.sleep(API_DELAY_SECONDS); continue
        asset_details_url = ASSET_DETAILS_URL_TEMPLATE.format(asset_id=asset_id); logging.debug(f"Fetching asset details: {asset_details_url}")
        asset_data = fetch_data(asset_details_url, is_json=True, headers=ANDROID_TV_HEADERS); raw_stream_uri = None
        if asset_data and 'providers' in asset_data and isinstance(asset_data['providers'], list):
            for provider in asset_data['providers']:
                 if ('sources' in provider and isinstance(provider['sources'], list)):
                     for source in provider['sources']:
                         if source.get('uri') and (source.get('type') == 'application/x-mpegURL' or source.get('uri', '').endswith('.m3u8')): raw_stream_uri = source['uri']; break
                         elif source.get('uri') and not raw_stream_uri: raw_stream_uri = source['uri']
                     if raw_stream_uri: break
        else: logging.warning(f"Could not find providers/sources for asset {asset_id} (Channel {channel_id})")
        if not raw_stream_uri: logging.warning(f"No stream URI found in sources for asset {asset_id} (Channel {channel_id})"); time.sleep(API_DELAY_SECONDS); continue
        processed_stream_url = process_stream_uri(raw_stream_uri)
        if processed_stream_url:
            channel_info['stream_url'] = processed_stream_url; logging.debug(f"Successfully processed stream URL for channel {channel_id} via asset lookup")
            channels_with_streams.append(channel_info); processed_count += 1
        else: logging.warning(f"Failed to process stream URI for asset {asset_id} (Channel {channel_id})")
        time.sleep(API_DELAY_SECONDS)
    logging.info(f"Asset lookup method obtained/verified stream URLs for {processed_count} channels.")
    return [ch for ch in channels_with_streams if ch.get('stream_url')]

def fetch_epg_data(channel_list):
    if not channel_list: return {}
    logging.info(f"Fetching EPG data for {len(channel_list)} channels (using Android TV EPG endpoint)...")
    consolidated_epg = {channel['id']: [] for channel in channel_list}; assets_cache = {}; channel_ids_in_list = {ch['id'] for ch in channel_list}
    today = datetime.now(timezone.utc); dates_to_fetch = [today + timedelta(days=d) for d in range(EPG_FETCH_DAYS)]
    total_requests = 0; total_programs_fetched = 0
    for date_obj in dates_to_fetch:
        date_str = date_obj.strftime('%Y%m%d'); offset = 0
        while offset <= MAX_EPG_OFFSET:
            total_requests += 1; logging.debug(f"Fetching EPG - Date: {date_str}, Offset: {offset}")
            fetch_url = EPG_FETCH_URL_TEMPLATE.format(date_str=date_str, offset=offset)
            page_data = fetch_data(fetch_url, is_json=True, headers=ANDROID_TV_HEADERS)
            if not page_data or 'channels' not in page_data or not isinstance(page_data['channels'], list):
                if page_data and 'channels' in page_data and not page_data['channels']: logging.debug(f"No more EPG channels found for date {date_str} at offset {offset}.")
                else: logging.warning(f"Invalid or missing EPG data structure for date {date_str}, offset {offset}. Response keys: {list(page_data.keys()) if isinstance(page_data, dict) else 'Non-dict response'}")
                break
            if len(page_data['channels']) == 0: logging.debug(f"Empty EPG channels list for date {date_str} at offset {offset}. Stopping for this date."); break
            if 'assets' in page_data and isinstance(page_data['assets'], dict): assets_cache.update(page_data['assets'])
            found_program_count_page = 0; processed_channel_count_this_page = 0
            for channel_schedule_data in page_data['channels']:
                channel_id = str(channel_schedule_data.get('channelId'))
                if channel_id in channel_ids_in_list:
                    processed_channel_count_this_page += 1
                    if channel_id not in consolidated_epg: consolidated_epg[channel_id] = []
                    for program_schedule in channel_schedule_data.get('schedule', []):
                        total_programs_fetched += 1; asset_id = program_schedule.get('assetId')
                        asset_details = assets_cache.get(asset_id)
                        if asset_details:
                            program_info = { 'start': program_schedule.get('start'), 'end': program_schedule.get('end'), 'assetId': asset_id, 'title': asset_details.get('title', 'Unknown Program'), 'descriptions': asset_details.get('descriptions',{}), 'episodeTitle': asset_details.get('episodeTitle'), }
                            if program_info['start'] and program_info['end']: consolidated_epg[channel_id].append(program_info); found_program_count_page +=1
                            else: logging.warning(f"EPG: Program for asset {asset_id} on channel {channel_id} missing start/end time in schedule.")
                        else: logging.warning(f"EPG: Asset details not found for assetId {asset_id} on channel {channel_id} (Date: {date_str}, Offset: {offset})")
            logging.debug(f"EPG - Date: {date_str}, Offset: {offset}: Processed {processed_channel_count_this_page} relevant channels, found {found_program_count_page} valid program entries on page.")
            offset += 50; time.sleep(API_DELAY_SECONDS)
    logging.info(f"Finished fetching EPG data after {total_requests} requests. Found {total_programs_fetched} raw program entries.")
    for ch_id, progs in consolidated_epg.items(): logging.debug(f"  Channel {ch_id}: Stored {len(progs)} program entries.")
    return consolidated_epg

# --- Generate M3U and EPG XML ---

def generate_epg_xml(channel_list_with_streams, consolidated_epg_data):
    logging.info("Generating EPG XML structure...")
    tv_element = ET.Element('tv', attrib={'generator-info-name': f'{GITHUB_USER}-{GITHUB_REPO}'})
    programme_count = 0; channel_ids_in_list = {c['id'] for c in channel_list_with_streams}
    logging.debug("Adding channel elements to EPG XML...")
    for channel in channel_list_with_streams:
        chan_el = ET.SubElement(tv_element, 'channel', attrib={'id': channel['id']})
        ET.SubElement(chan_el, 'display-name').text = channel['name'] # Only one display-name
        if channel['logo']: ET.SubElement(chan_el, 'icon', attrib={'src': channel['logo']})
        logging.debug(f"  Added channel: ID={channel['id']}, Name={channel['name']}")
    logging.debug("Adding programme elements to EPG XML...")
    logging.debug(f"Number of channels in consolidated_epg_data: {len(consolidated_epg_data)}")
    total_programs_in_data = sum(len(progs) for progs in consolidated_epg_data.values())
    logging.debug(f"Total program entries fetched before filtering/processing: {total_programs_in_data}")
    for channel_id, programs in consolidated_epg_data.items():
        if channel_id not in channel_ids_in_list: logging.debug(f"Skipping EPG programs for channel {channel_id} as it's not in the final M3U list."); continue
        program_processed_for_channel = 0
        logging.debug(f"Processing programs for channel {channel_id}...")
        for program in programs:
            program_asset_id = program.get('assetId', 'N/A'); program_title = program.get('title', 'N/A')
            logging.debug(f"  Processing program: AssetID={program_asset_id}, Title='{program_title}'")
            try:
                start_time_str = program.get('start'); end_time_str = program.get('end')
                logging.debug(f"    Raw Times: Start='{start_time_str}', End='{end_time_str}'")
                start_time = parse_iso_datetime(start_time_str); end_time = parse_iso_datetime(end_time_str) # Use updated parser
                if not start_time or not end_time: logging.warning(f"    Skipping program due to failed time parsing: AssetID={program_asset_id}, Title='{program_title}'"); continue
                logging.debug(f"    Parsed Times (UTC): Start={start_time}, End={end_time}")
                title = program.get('title', 'Unknown Program'); desc_obj = program.get('descriptions', {})
                desc = desc_obj.get('large') or desc_obj.get('medium') or desc_obj.get('small') or desc_obj.get('tiny')
                episode_title = program.get('episodeTitle'); asset_id = program.get('assetId')
                start_formatted = format_xmltv_time(start_time); stop_formatted = format_xmltv_time(end_time) # Use final updated formatter
                logging.debug(f"    Formatted Times: Start='{start_formatted}', Stop='{stop_formatted}'") # Check this output
                if start_formatted and stop_formatted:
                    prog_el = ET.SubElement(tv_element, 'programme', attrib={'start': start_formatted,'stop': stop_formatted,'channel': channel_id})
                    ET.SubElement(prog_el, 'title', attrib={'lang': 'en'}).text = title
                    if desc: ET.SubElement(prog_el, 'desc', attrib={'lang': 'en'}).text = desc
                    if episode_title and episode_title != title: ET.SubElement(prog_el, 'sub-title', attrib={'lang': 'en'}).text = episode_title
                    if asset_id:
                        system_type = "dd_progid" if asset_id.startswith("EP") else "dd_assetid"
                        ET.SubElement(prog_el, 'episode-num', attrib={'system': system_type}).text = asset_id
                    programme_count += 1; program_processed_for_channel += 1
                    logging.debug(f"    Successfully added <programme> element for '{title}'")
                else: logging.warning(f"    Skipping program due to invalid formatted time: AssetID={program_asset_id}, Title='{title}' (Channel: {channel_id})")
            except Exception as e: logging.exception(f"Error processing EPG program item {program_asset_id} for channel {channel_id}: {e}")
        logging.debug(f"  Finished processing channel {channel_id}, added {program_processed_for_channel} programme elements.")
    logging.info(f"Generated XML with {len(channel_list_with_streams)} channels and {programme_count} programmes.") # Check this count!
    if programme_count == 0 and total_programs_in_data > 0: logging.warning("EPG data was fetched, but no valid program entries could be added to the XML. Check time parsing/formatting issues in DEBUG logs.")
    return ET.ElementTree(tv_element)

def generate_m3u_playlist(channel_list_with_streams):
    logging.info("Generating M3U playlist...")
    playlist_parts = [f'#EXTM3U url-tvg="{EPG_RAW_URL}"\n']
    added_count = 0
    def sort_key(channel):
        try: num = int(channel.get('number', '999999')); return (num, channel['name'].lower())
        except (ValueError, TypeError): return (999999, channel['name'].lower())
    sorted_channels = sorted(channel_list_with_streams, key=sort_key)
    for channel in sorted_channels:
        stream_url = channel.get('stream_url'); channel_id = channel['id']; display_name = channel['name'].replace(',', ';')
        group_title = channel.get('group', 'General').replace(',', ';'); tvg_name = channel['name'].replace('"', "'"); logo = channel.get("logo", "")
        if stream_url:
            extinf_line = f'#EXTINF:-1 tvg-id="{channel_id}" tvg-name="{tvg_name}" tvg-logo="{logo}" group-title="{group_title}",{display_name}\n'
            playlist_parts.append(extinf_line); playlist_parts.append(f'{stream_url}\n'); added_count += 1
        else: logging.error(f"Channel {channel_id} ('{channel['name']}') reached M3U generation without a stream URL!")
    logging.info(f"Added {added_count} channels with stream URLs to M3U playlist.")
    return "".join(playlist_parts)

# --- Main Execution ---
if __name__ == "__main__":
    logging.info(f"--- Starting Xumo Scraper (Valencia List ID: {VALENCIA_LIST_ID}, Fallback/EPG List ID: {ANDROID_TV_LIST_ID}) ---")
    try: ensure_output_dir()
    except Exception as e: logging.error(f"Halting script because output directory could not be ensured: {e}"); sys.exit(1)

    channel_list_from_primary = None; final_channel_list_with_streams = None

    logging.info(f"Step 1: Attempting to get channel metadata via Valencia List endpoint ({VALENCIA_LIST_ID})...")
    channel_list_from_primary = get_channels_via_proxy_list()

    if channel_list_from_primary:
        logging.info(f"Found {len(channel_list_from_primary)} channels from Valencia endpoint.")
        streams_found_primary = sum(1 for ch in channel_list_from_primary if ch.get('stream_url'))
        logging.info(f"{streams_found_primary} channels from Valencia list include a direct stream URL.")
        if streams_found_primary < len(channel_list_from_primary):
            logging.warning(f"{len(channel_list_from_primary) - streams_found_primary} channels from Valencia list are missing stream URLs.")
            logging.info("Step 2: Attempting asset lookup for channels missing stream URLs...")
            final_channel_list_with_streams = fetch_stream_urls_via_asset_lookup(channel_list_from_primary)
        else:
            logging.info("All channels from Valencia list have stream URLs. Skipping asset lookup fetch.")
            final_channel_list_with_streams = [ch for ch in channel_list_from_primary if ch.get('stream_url')]
    else:
        logging.warning("Valencia List endpoint failed. Initiating Full Fallback: Android TV Method...")
        logging.info("Step 1 (Fallback): Getting channel list via Android TV endpoint...")
        fallback_channel_list = get_live_channels_list_android_tv()
        if not fallback_channel_list: logging.error("Fallback Failed: Could not get master channel list. Aborting."); sys.exit(1)
        logging.info("Step 2 (Fallback): Fetching stream URLs via Android TV asset lookup...")
        final_channel_list_with_streams = fetch_stream_urls_via_asset_lookup(fallback_channel_list)

    if not final_channel_list_with_streams:
         logging.warning("No channels with stream URLs found after trying all methods. Generating empty files.")
         save_m3u(f'#EXTM3U url-tvg="{EPG_RAW_URL}"\n', os.path.join(OUTPUT_DIR, PLAYLIST_FILENAME))
         empty_root = ET.Element('tv'); empty_tree = ET.ElementTree(empty_root)
         save_gzipped_xml(empty_tree, os.path.join(OUTPUT_DIR, EPG_FILENAME)) # Use corrected save function
         logging.info("Generated empty playlist and EPG files."); sys.exit(0)

    final_channel_list_with_streams = [ch for ch in final_channel_list_with_streams if ch.get('stream_url')]
    logging.info(f"Proceeding with {len(final_channel_list_with_streams)} channels confirmed to have stream URLs.")
    if not final_channel_list_with_streams:
        logging.warning("Filtering removed all channels (no streams found). Generating empty files.")
        save_m3u(f'#EXTM3U url-tvg="{EPG_RAW_URL}"\n', os.path.join(OUTPUT_DIR, PLAYLIST_FILENAME))
        empty_root = ET.Element('tv'); empty_tree = ET.ElementTree(empty_root)
        save_gzipped_xml(empty_tree, os.path.join(OUTPUT_DIR, EPG_FILENAME)) # Use corrected save function
        logging.info("Generated empty playlist and EPG files."); sys.exit(0)

    epg_data = fetch_epg_data(final_channel_list_with_streams)
    epg_tree = generate_epg_xml(final_channel_list_with_streams, epg_data)
    m3u_content = generate_m3u_playlist(final_channel_list_with_streams)
    save_m3u(m3u_content, os.path.join(OUTPUT_DIR, PLAYLIST_FILENAME))
    save_gzipped_xml(epg_tree, os.path.join(OUTPUT_DIR, EPG_FILENAME)) # Use corrected save function
    logging.info("--- Xumo Scraper Finished Successfully ---")
