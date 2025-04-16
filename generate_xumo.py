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
import math # For calculating total pages

# --- Configuration ---
ANDROID_TV_ENDPOINT = "https://android-tv-mds.xumo.com/v2" # Keep for fallback methods if needed
VALENCIA_API_ENDPOINT = "https://valencia-app-mds.xumo.com/v2" # Primary endpoint
GEO_ID = "us"

# --- List IDs ---
# Use the Valencia/Web List ID for BOTH primary channel list AND EPG now
PRIMARY_LIST_ID = "10006"
# Android TV List ID only needed if fallback is ever required for channels/streams
# ANDROID_TV_LIST_ID = "10032" # Less relevant now

# --- Endpoint URLs ---
PRIMARY_CHANNEL_LIST_URL = f"{VALENCIA_API_ENDPOINT}/proxy/channels/list/{PRIMARY_LIST_ID}.json?geoId={GEO_ID}"

# Fallback URLs (Less likely needed now, but keep for reference)
# ANDROID_TV_CHANNEL_LIST_URL = f"{ANDROID_TV_ENDPOINT}/channels/list/{ANDROID_TV_LIST_ID}.json?f=genreId&sort=hybrid&geoId={GEO_ID}"
# BROADCAST_NOW_URL_TEMPLATE = f"{ANDROID_TV_ENDPOINT}/channels/channel/{{channel_id}}/broadcast.json?hour={{hour_num}}"
# ASSET_DETAILS_URL_TEMPLATE = f"{ANDROID_TV_ENDPOINT}/assets/asset/{{asset_id}}.json?f=providers"

# <<< MODIFIED EPG URL: Uses PRIMARY_LIST_ID and includes {hour} placeholder >>>
EPG_FETCH_URL_TEMPLATE = f"{VALENCIA_API_ENDPOINT}/epg/{PRIMARY_LIST_ID}/{{date_str}}/{{hour}}.json?limit=50&offset={{offset}}&f=asset.title&f=asset.descriptions"

XUMO_LOGO_URL_TEMPLATE = "https://image.xumo.com/v1/channels/channel/{channel_id}/168x168.png?type=color_onBlack"

# --- Script Settings ---
EPG_FETCH_DAYS = 2 # How many days of EPG data (Today + Tomorrow)
MAX_EPG_OFFSET = 350 # Max offset to try (adjust based on approx channel count / 50)
HOURS_TO_FETCH = 24 # Fetch all 24 hours for each day/offset block
API_DELAY_SECONDS = 0.10 # Can potentially reduce delay slightly if needed
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
# Primarily use Web Headers now, as Valencia endpoint is the main target
WEB_HEADERS = { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36', 'Accept': 'application/json, text/plain, */*', 'Accept-Language': 'en-US,en;q=0.9', 'Origin': 'https://play.xumo.com', 'Referer': 'https://play.xumo.com/', }
# ANDROID_TV_HEADERS = { 'User-Agent': 'okhttp/4.9.3', } # Keep if fallback needed

# --- Logging Setup ---
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - [%(funcName)s] %(message)s', stream=sys.stdout)


# --- Helper Functions ---

def fetch_data(url, params=None, is_json=True, retries=2, delay=2, headers=WEB_HEADERS):
    """Fetches data from a URL, handles JSON parsing and errors, includes retries."""
    logging.debug(f"URL: {url}, Params: {params}")
    # logging.debug(f"Headers: {json.dumps(headers)}") # Reduce header logging verbosity
    for attempt in range(retries + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            logging.debug(f"Request URL: {response.url} -> Status: {response.status_code}")
            response.raise_for_status()
            if is_json:
                if not response.content: logging.warning(f"Empty response content received from {url}"); return None
                try: return response.json()
                except json.JSONDecodeError as e_final:
                    logging.error(f"Error decoding JSON. Content: {response.text[:500]}... - {e_final}")
                    if logging.getLogger().level == logging.DEBUG: logging.debug(f"Full Text:\n{response.text}")
                    return None # Don't retry JSON errors
            else:
                 try:
                     decoded_text = response.content.decode('utf-8', errors='ignore')
                     # Limit logging of non-json text unless debugging specific text issue
                     # if logging.getLogger().level == logging.DEBUG: logging.debug(f"Raw Text Response:\n{decoded_text[:1500]}...")
                     return decoded_text
                 except Exception as decode_ex: logging.error(f"Error decoding text response: {decode_ex}"); return None
        except requests.exceptions.HTTPError as e:
            logging.warning(f"Attempt {attempt+1}/{retries+1} HTTP Error: {e}")
            if response is not None: logging.warning(f"Error Response Content: {response.text[:500]}...")
            # Don't retry 404 (Not Found) as the hour/offset might just not exist
            if attempt < retries and response is not None and response.status_code not in [401, 403, 404, 429]:
                time.sleep(delay * (attempt + 1))
            elif attempt == retries: logging.error(f"Final attempt failed with HTTP Error: {e}"); return None
            else: break # Non-retriable HTTP error or final attempt failed (like 404)
        except requests.exceptions.RequestException as e:
            logging.warning(f"Attempt {attempt+1}/{retries+1} Network Error: {e}")
            if attempt < retries: time.sleep(delay * (attempt + 1))
            elif attempt == retries: logging.error(f"Final attempt failed with Network Error: {e}"); return None
    return None

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

def format_xmltv_time(dt_obj):
    """Formats datetime object into XMLTV time (YYYYMMDDHHMMSS +HHMM)."""
    if not isinstance(dt_obj, datetime): logging.warning(f"format_xmltv_time received non-datetime object: {type(dt_obj)}"); return ""
    if not dt_obj.tzinfo: dt_obj_utc = dt_obj.replace(tzinfo=timezone.utc)
    else: dt_obj_utc = dt_obj.astimezone(timezone.utc)
    main_part = dt_obj_utc.strftime('%Y%m%d%H%M%S'); offset_part = dt_obj_utc.strftime('%z')
    offset_part_clean = offset_part.replace(':', ''); full_time_str = f"{main_part} {offset_part_clean}"
    # Logging removed for brevity as this seemed stable
    # logging.debug(f"Formatted time: {full_time_str}");
    return full_time_str

def ensure_output_dir():
    if not os.path.exists(OUTPUT_DIR):
        logging.info(f"Creating output directory: {OUTPUT_DIR}")
        try: os.makedirs(OUTPUT_DIR)
        except OSError as e: logging.error(f"Failed to create directory {OUTPUT_DIR}: {e}"); raise

ADD_XMLTV_DOCTYPE = True # Keep DOCTYPE addition

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

def get_channels_via_primary_list():
    """Gets channel list from the primary Valencia endpoint."""
    logging.info(f"Attempting Primary Channel List: {PRIMARY_CHANNEL_LIST_URL}")
    data = fetch_data(PRIMARY_CHANNEL_LIST_URL, is_json=True, retries=1, headers=WEB_HEADERS) # Use WEB_HEADERS
    if not data or not isinstance(data, dict): logging.warning(f"Failed to fetch valid dictionary data from primary list endpoint."); return None
    processed_channels = []; channel_items = []
    # Assuming structure similar to Android TV / previous findings
    if 'channel' in data and isinstance(data['channel'], dict) and 'item' in data['channel'] and isinstance(data['channel']['item'], list):
        channel_items = data['channel']['item']; logging.debug("Found channel list under data['channel']['item']")
    elif 'items' in data and isinstance(data['items'], list): # Alternative structure check
        channel_items = data['items']; logging.debug("Found channel list under data['items']")
    else: logging.error(f"Could not find channel list in primary list response. Top-level keys: {list(data.keys())}"); return None

    logging.info(f"Found {len(channel_items)} potential channel items in primary list response.")
    if not channel_items: logging.warning("Primary list response contained an empty channel list."); return None

    for item in channel_items:
        # Basic Filtering (Live, Non-DRM) - adapt if needed based on Valencia flags
        callsign = item.get('callsign', '')
        properties = item.get('properties', {})
        is_live = properties.get('is_live') == "true"
        is_drm = callsign.endswith("-DRM") or callsign.endswith("DRM-CMS") # Use callsign for now

        if is_drm: logging.debug(f"Skipping potential DRM channel: {item.get('guid', {}).get('value')} ({item.get('title')})"); continue
        if not is_live: logging.debug(f"Skipping non-live channel: {item.get('guid', {}).get('value')} ({item.get('title')})"); continue

        # Extract needed info
        try:
            channel_id = item.get('guid', {}).get('value')
            title = item.get('title')
            number_str = item.get('number')
            logo_url = item.get('images', {}).get('logo') or item.get('logo')
            genre_list = item.get('genre'); genre = 'General'
            if isinstance(genre_list, list) and len(genre_list) > 0 and isinstance(genre_list[0], dict): genre = genre_list[0].get('value', 'General')
            elif isinstance(genre_list, str): genre = genre_list

            if not channel_id or not title: logging.warning(f"Skipping item due to missing ID or title: {item}"); continue

            channel_id_str = str(channel_id); final_logo_url = None
            if logo_url:
                 if logo_url.startswith('//'): final_logo_url = 'https:' + logo_url
                 elif logo_url.startswith('/'): final_logo_url = 'https://image.xumo.com' + logo_url
                 else: final_logo_url = logo_url
            else: final_logo_url = XUMO_LOGO_URL_TEMPLATE.replace("{channel_id}", channel_id_str)

            # Don't look for streams here, that's separate
            processed_channels.append({ 'id': channel_id_str, 'name': title, 'number': str(number_str) if number_str else None, 'callsign': callsign, 'logo': final_logo_url, 'group': genre, 'stream_url': None })
        except Exception as e: logging.warning(f"Error processing channel list item {item.get('id', 'N/A')}: {e}", exc_info=True)

    if not processed_channels: logging.warning("Primary channel list endpoint returned data, but no channels could be successfully processed."); return None
    logging.info(f"Successfully processed {len(processed_channels)} live, non-DRM channels from primary list endpoint.")
    return processed_channels

# <<< REWRITTEN EPG Fetching Function >>>
def fetch_epg_data(channel_list):
    """Fetches EPG data using the Valencia endpoint (10006), iterating through hours and offsets."""
    if not channel_list: return {}
    logging.info(f"Fetching EPG data for {len(channel_list)} channels (using Valencia EPG endpoint {PRIMARY_LIST_ID})...")

    # Prepare structure to hold consolidated data
    consolidated_epg = {channel['id']: [] for channel in channel_list}
    assets_cache = {} # Cache asset details across all calls
    channel_ids_in_final_list = {ch['id'] for ch in channel_list} # Set for quick lookup

    # Determine dates to fetch
    today = datetime.now(timezone.utc)
    dates_to_fetch = [today + timedelta(days=d) for d in range(EPG_FETCH_DAYS)]

    total_requests = 0
    total_programs_fetched = 0 # Count raw programs received from API
    total_programs_added = 0 # Count programs successfully added to our structure

    # --- Loop through Days ---
    for date_obj in dates_to_fetch:
        date_str = date_obj.strftime('%Y%m%d')
        logging.info(f"Fetching EPG for date: {date_str}")

        # --- Loop through Channel Offset Blocks ---
        offset = 0
        while offset <= MAX_EPG_OFFSET:
            logging.debug(f"Processing EPG Offset Block: {offset}")
            found_relevant_channel_in_offset_block = False

            # --- Loop through Hours (0-23) for this offset block ---
            for hour in range(HOURS_TO_FETCH):
                total_requests += 1
                fetch_url = EPG_FETCH_URL_TEMPLATE.format(date_str=date_str, hour=hour, offset=offset)
                logging.debug(f"  Fetching EPG - Date={date_str}, Hour={hour}, Offset={offset} -> {fetch_url}")

                # Use WEB_HEADERS for Valencia endpoint
                page_data = fetch_data(fetch_url, is_json=True, retries=1, delay=1, headers=WEB_HEADERS) # Fewer retries for hourly calls

                # Process data if fetch was successful
                if page_data and isinstance(page_data, dict):
                    # Cache assets from this page
                    if 'assets' in page_data and isinstance(page_data['assets'], dict):
                        assets_cache.update(page_data['assets'])

                    # Process channel schedules if present
                    if 'channels' in page_data and isinstance(page_data['channels'], list):
                        processed_channels_this_page = 0
                        programs_added_this_page = 0

                        for channel_schedule_data in page_data['channels']:
                            channel_id = str(channel_schedule_data.get('channelId'))

                            # Only process if this channel is in our final M3U list
                            if channel_id in channel_ids_in_final_list:
                                found_relevant_channel_in_offset_block = True # Mark that this offset block is useful
                                processed_channels_this_page += 1
                                if channel_id not in consolidated_epg: consolidated_epg[channel_id] = [] # Should exist, safety check

                                for program_schedule in channel_schedule_data.get('schedule', []):
                                    total_programs_fetched += 1
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
                                        # Add only if start/end times exist (parsing happens later)
                                        if program_info['start'] and program_info['end']:
                                            consolidated_epg[channel_id].append(program_info)
                                            programs_added_this_page += 1
                                            total_programs_added += 1
                                        else:
                                            logging.warning(f"EPG: Program for asset {asset_id} on channel {channel_id} missing start/end time in schedule.")
                                    else:
                                        logging.warning(f"EPG: Asset details not found for assetId {asset_id} on channel {channel_id} (Date={date_str}, Hour={hour}, Offset={offset})")

                        if programs_added_this_page > 0:
                             logging.debug(f"    Processed {processed_channels_this_page} relevant channels, added {programs_added_this_page} program entries from Hour {hour}.")

                    else:
                         # Log if 'channels' key is missing but response was otherwise okay
                         logging.debug(f"    No 'channels' key found in response for Hour {hour}, Offset {offset}.")
                else:
                    # Log if fetch failed (e.g., 404 for a specific hour/offset)
                    logging.debug(f"    Failed to fetch or invalid data for Hour {hour}, Offset {offset}. Skipping.")
                    # Optimization: If hour 0 fails for an offset, subsequent hours likely will too? Maybe break inner loop?
                    # if hour == 0:
                    #     logging.warning(f"    Hour 0 failed for Offset {offset}, skipping remaining hours for this offset.")
                    #     break # Break hour loop

                # Optional small delay between hourly fetches within the same offset block
                time.sleep(API_DELAY_SECONDS / 2) # Half the normal delay

            # --- End of Hour Loop ---

            # Check if we should continue to the next offset block
            # If the *entire block* (all hours) yielded no relevant channels, maybe stop early?
            # This requires checking if *any* hour for this offset had relevant channels.
            # If not found_relevant_channel_in_offset_block:
            #    logging.info(f"No relevant channels found in any hour for Offset {offset}. Stopping offset iteration for date {date_str}.")
            #    break # Break offset loop

            logging.debug(f"Finished processing all hours for Offset Block: {offset}")
            offset += 50 # Move to the next block of 50 channels
            time.sleep(API_DELAY_SECONDS) # Delay between offset block fetches

        # --- End of Offset Loop ---
    # --- End of Day Loop ---

    logging.info(f"Finished fetching EPG data after {total_requests} requests.")
    logging.info(f"Found {total_programs_fetched} raw program entries, successfully stored {total_programs_added} entries.")
    # Log final counts per channel
    for ch_id, progs in consolidated_epg.items():
        logging.debug(f"  Channel {ch_id}: Stored {len(progs)} program entries.")

    # Remove duplicate program entries for each channel (based on start time and asset ID)
    logging.info("Removing duplicate program entries...")
    final_epg = {}
    duplicates_removed = 0
    for channel_id, programs in consolidated_epg.items():
        seen_programs = set()
        unique_programs = []
        for prog in programs:
            # Create a unique key for each program instance
            prog_key = (prog.get('start'), prog.get('assetId'))
            if prog_key not in seen_programs:
                seen_programs.add(prog_key)
                unique_programs.append(prog)
            else:
                duplicates_removed += 1
        final_epg[channel_id] = unique_programs
        if duplicates_removed > 0:
            logging.debug(f"  Channel {channel_id}: Removed {duplicates_removed} duplicates. Final count: {len(unique_programs)}")
        duplicates_removed = 0 # Reset counter for next channel


    logging.info(f"Finished EPG processing. Total duplicates removed: {duplicates_removed}")
    return final_epg
# <<< END OF REWRITTEN EPG Fetching Function >>>

# --- Generate M3U and EPG XML ---

def generate_epg_xml(channel_list_with_streams, consolidated_epg_data):
    """Generates XMLTV file, ensuring only one display-name per channel."""
    logging.info("Generating EPG XML structure...")
    tv_element = ET.Element('tv', attrib={'generator-info-name': f'{GITHUB_USER}-{GITHUB_REPO}'})
    programme_count = 0; channel_ids_in_list = {c['id'] for c in channel_list_with_streams}
    logging.debug("Adding channel elements to EPG XML...")
    for channel in channel_list_with_streams:
        chan_el = ET.SubElement(tv_element, 'channel', attrib={'id': channel['id']})
        ET.SubElement(chan_el, 'display-name').text = channel['name'] # Only one display-name
        if channel['logo']: ET.SubElement(chan_el, 'icon', attrib={'src': channel['logo']})
        #logging.debug(f"  Added channel: ID={channel['id']}, Name={channel['name']}") # Reduce verbosity
    logging.debug("Adding programme elements to EPG XML...")
    total_programs_in_data = sum(len(progs) for progs in consolidated_epg_data.values())
    logging.debug(f"Total unique program entries before XML generation: {total_programs_in_data}")
    for channel_id, programs in consolidated_epg_data.items():
        if channel_id not in channel_ids_in_list: logging.debug(f"Skipping EPG programs for channel {channel_id} as it's not in the final M3U list."); continue
        program_processed_for_channel = 0
        #logging.debug(f"Processing programs for channel {channel_id}...") # Reduce verbosity
        for program in programs:
            program_asset_id = program.get('assetId', 'N/A'); program_title = program.get('title', 'N/A')
            #logging.debug(f"  Processing program: AssetID={program_asset_id}, Title='{program_title}'") # Reduce verbosity
            try:
                start_time_str = program.get('start'); end_time_str = program.get('end')
                #logging.debug(f"    Raw Times: Start='{start_time_str}', End='{end_time_str}'")
                start_time = parse_iso_datetime(start_time_str); end_time = parse_iso_datetime(end_time_str)
                if not start_time or not end_time: logging.warning(f"    Skipping program due to failed time parsing: AssetID={program_asset_id}, Title='{program_title}'"); continue
                #logging.debug(f"    Parsed Times (UTC): Start={start_time}, End={end_time}")
                title = program.get('title', 'Unknown Program'); desc_obj = program.get('descriptions', {})
                desc = desc_obj.get('large') or desc_obj.get('medium') or desc_obj.get('small') or desc_obj.get('tiny')
                episode_title = program.get('episodeTitle'); asset_id = program.get('assetId')
                start_formatted = format_xmltv_time(start_time); stop_formatted = format_xmltv_time(end_time)
                #logging.debug(f"    Formatted Times: Start='{start_formatted}', Stop='{stop_formatted}'") # Reduce verbosity
                if start_formatted and stop_formatted:
                    prog_el = ET.SubElement(tv_element, 'programme', attrib={'start': start_formatted,'stop': stop_formatted,'channel': channel_id})
                    ET.SubElement(prog_el, 'title', attrib={'lang': 'en'}).text = title
                    if desc: ET.SubElement(prog_el, 'desc', attrib={'lang': 'en'}).text = desc
                    if episode_title and episode_title != title: ET.SubElement(prog_el, 'sub-title', attrib={'lang': 'en'}).text = episode_title
                    if asset_id:
                        system_type = "dd_progid" if asset_id.startswith("EP") else "dd_assetid"
                        ET.SubElement(prog_el, 'episode-num', attrib={'system': system_type}).text = asset_id
                    programme_count += 1; program_processed_for_channel += 1
                    #logging.debug(f"    Successfully added <programme> element for '{title}'") # Reduce verbosity
                else: logging.warning(f"    Skipping program due to invalid formatted time: AssetID={program_asset_id}, Title='{title}' (Channel: {channel_id})")
            except Exception as e: logging.exception(f"Error processing EPG program item {program_asset_id} for channel {channel_id}: {e}")
        #logging.debug(f"  Finished processing channel {channel_id}, added {program_processed_for_channel} programme elements.") # Reduce verbosity
    logging.info(f"Generated XML with {len(channel_list_with_streams)} channels and {programme_count} programmes.") # Final count
    if programme_count == 0 and total_programs_in_data > 0: logging.warning("EPG data was fetched, but no valid program entries could be added to the XML. Check time parsing/formatting issues in DEBUG logs.")
    return ET.ElementTree(tv_element)

def generate_m3u_playlist(channel_list_with_streams):
    # Keep this function exactly the same
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
    logging.info(f"--- Starting Xumo Scraper (Primary List ID: {PRIMARY_LIST_ID}) ---")
    try: ensure_output_dir()
    except Exception as e: logging.error(f"Halting script because output directory could not be ensured: {e}"); sys.exit(1)

    primary_channel_list = None
    final_channel_list_with_streams = None # This will hold the final list for M3U/EPG generation

    # --- Step 1: Get Primary Channel List ---
    # We now rely solely on the Valencia list as it matches the EPG source
    logging.info(f"Step 1: Getting channel metadata via Primary List endpoint ({PRIMARY_LIST_ID})...")
    primary_channel_list = get_channels_via_primary_list()

    if not primary_channel_list:
        logging.error("Failed to get primary channel list. Aborting.")
        sys.exit(1)

    # --- Step 2: Fetch Stream URLs ---
    # We always need to fetch streams using the asset lookup method now,
    # as the primary channel list function doesn't include them reliably.
    logging.info(f"Step 2: Fetching stream URLs via asset lookup for {len(primary_channel_list)} channels...")
    final_channel_list_with_streams = fetch_stream_urls_via_asset_lookup(primary_channel_list)

    # --- Process Final List ---
    if not final_channel_list_with_streams:
         logging.warning("No channels with stream URLs found after processing. Generating empty files.")
         save_m3u(f'#EXTM3U url-tvg="{EPG_RAW_URL}"\n', os.path.join(OUTPUT_DIR, PLAYLIST_FILENAME))
         empty_root = ET.Element('tv'); empty_tree = ET.ElementTree(empty_root)
         save_gzipped_xml(empty_tree, os.path.join(OUTPUT_DIR, EPG_FILENAME))
         logging.info("Generated empty playlist and EPG files."); sys.exit(0)

    # Log the final count of channels that will be in the M3U/EPG
    logging.info(f"Proceeding with {len(final_channel_list_with_streams)} channels confirmed to have stream URLs.")

    # --- Step 3: Fetch EPG Data (Using new hourly method) ---
    # Pass the final list of channels that actually have streams
    epg_data = fetch_epg_data(final_channel_list_with_streams)

    # --- Step 4: Generate EPG XML ---
    epg_tree = generate_epg_xml(final_channel_list_with_streams, epg_data)

    # --- Step 5: Generate M3U Playlist ---
    m3u_content = generate_m3u_playlist(final_channel_list_with_streams)

    # --- Step 6: Save Files ---
    save_m3u(m3u_content, os.path.join(OUTPUT_DIR, PLAYLIST_FILENAME))
    save_gzipped_xml(epg_tree, os.path.join(OUTPUT_DIR, EPG_FILENAME))

    logging.info("--- Xumo Scraper Finished Successfully ---")
