import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import xml.etree.ElementTree as ET

# --- CONFIGURATION MAP ---
# Map your local XMLTV channel IDs to TV Passport Station IDs & Provider Lineup IDs
# Example: 'A&E' -> Station ID: 11047, Lineup ID: 9239
CHANNEL_MAP = {
    'A&E.ca': {'station_id': 11047, 'lineup_id': 9239},
    'ABC.east': {'station_id': 11048, 'lineup_id': 9239},
    # Add your missing channel IDs here following the exact same format
}

def scrape_tvpassport(station_id, lineup_id):
    """Fetches and parses a single channel's daily schedule from TV Passport."""
    session = requests.Session()
    session.cookies.set('st_va', str(lineup_id), domain='.tvpassport.com', path='/')
    
    today_str = datetime.now().strftime("%Y-%m-%d")
    url = f"https://tvpassport.com{station_id}/{today_str}"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    try:
        response = session.get(url, headers=headers, timeout=15)
        if response.status_code != 200:
            return []
        
        soup = BeautifulSoup(response.text, 'html.parser')
        listings = []
        rows = soup.find_all('div', class_='station-listings-row')
        
        for row in rows:
            time_el = row.find('div', class_='time')
            title_el = row.find('div', class_='item-title')
            desc_el = row.find('div', class_='item-description')
            
            if time_el and title_el:
                # Convert TV Passport text time (e.g. "11:00 AM") to XMLTV time format
                time_str = time_el.get_text(strip=True)
                title_str = title_el.get_text(strip=True)
                desc_str = desc_el.get_text(strip=True) if desc_el else ""
                
                # Baseline XMLTV string stamp converter logic
                try:
                    in_time = datetime.strptime(f"{today_str} {time_str}", "%Y-%m-%d %I:%M %p")
                    xml_start = in_time.strftime("%Y%m%d%H%M%S +0000")
                except ValueError:
                    continue # Skip malformed rows safely
                
                listings.append({
                    'start': xml_start,
                    'title': title_str,
                    'description': desc_str
                })
        return listings
    except Exception as e:
        print(f"Skipping TV Passport pull for station {station_id}: {e}")
        return []

def patch_epg_file(xml_path):
    """Reads the generated EPG file, finds mapped channels, and injects TV Passport data."""
    if not os.path.exists(xml_path):
        print(f"Target EPG file not found at {xml_path}")
        return
        
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
    except ET.ParseError:
        print("Invalid XML target file format. Aborting patch step.")
        return

    print("Beginning TV Passport auxiliary schedule injection...")
    
    # Process each configured channel map entry
    for channel_id, meta in CHANNEL_MAP.items():
        print(f"Scraping TV Passport data for target channel: {channel_id}")
        shows = scrape_tvpassport(meta['station_id'], meta['lineup_id'])
        
        if not shows:
            print(f"No active data returned for {channel_id}. Skipping injection.")
            continue
            
        # Clear out any existing placeholder programs for this specific channel
        for prog in root.findall(f"./programme[@channel='{channel_id}']"):
            root.remove(prog)
            
        # Inject the fresh programmatic blocks into the XML root tree structure
        for show in shows:
            prog_node = ET.SubElement(root, 'programme', {
                'start': show['start'],
                'channel': channel_id
            })
            title_node = ET.SubElement(prog_node, 'title', {'lang': 'en'})
            title_node.text = show['title']
            
            if show['description']:
                desc_node = ET.SubElement(prog_node, 'desc', {'lang': 'en'})
                desc_node.text = show['description']
                
    # Overwrite the file with the newly patched structural data layers
    tree.write(xml_path, encoding='UTF-8', xml_declaration=True)
    print("TV Passport schedule patch completed successfully!")

if __name__ == "__main__":
    # Point directly to your newly built EPG file path output location
    target_file = os.path.join(os.getcwd(), 'epg_new.xml')
    patch_epg_file(target_file)
