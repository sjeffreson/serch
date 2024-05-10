import requests
from bs4 import BeautifulSoup
import pandas as pd
import os, time, json

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
from typing import List, Dict, Tuple

with open('config.json') as f:
    config = json.load(f)
OUTPUT_DIR = config['paths']['output_dir']
DATAFRAME_MNTHLSTNRS = config['filenames']["artist_info_mnth_lstnrs"]

def scrape_monthly_listeners(artist_id, artist_name) -> int:
    url = f"https://open.spotify.com/artist/{artist_id}"
    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        monthly_listeners_element = soup.find("div", {"data-testid": "monthly-listeners-label"})
        if monthly_listeners_element:
            monthly_listeners = monthly_listeners_element.text.strip()
            monthly_listeners = int(monthly_listeners.split("monthly listener")[0].replace(",", "").strip())
            return monthly_listeners
        else:
            print(f"Failed to find monthly listeners for artist id: {artist_id} and artist name: {artist_name}. Monthly listeners must be 0.")
            return 0
    else:
        print(f"Failed to load page: {url} for artist name {artist_name}.")
        return None

def scrape_genres_from_bio(artist_id, artist_name, genres_to_find) -> int:
    url = f"https://open.spotify.com/artist/{artist_id}"
    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        bio_text = soup.find('span', class_='Type__TypeElement-sc-goli3j-0 kmjYak G_f5DJd2sgHWeto5cwbi', attrs={'data-encore-id': 'type'})
        if bio_text:
            bio_text = bio_text.get_text()
            bio_text = bio_text.lower()
            found_strings = [elem for elem in genres_to_find if elem in bio_text]
            return found_strings
        else:
            print(f"Failed to find bio for artist id: {artist_id} and artist name: {artist_name}.")
            return None
    else:
        print(f"Failed to load page: {url} for artist name {artist_name}.")
        return None

if __name__ == "__main__":
    pass