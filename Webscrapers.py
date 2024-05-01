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
    # find genres for artists with no genres in Spotify_artist_info-MonthlyListeners.csv
    start_idx = 0
    genres_to_search = [
        ' classical ', ' ambient ', ' blues ', ' christian ', ' dance ', ' hip hop ', ' indie ',
        ' reggae ', ' singer-songwriter ', ' country ', ' metal ', ' jazz ', ' soul ',
        ' goth ', ' electro ', ' house ', ' tech ', ' rock ', ' rap ', ' traditional ', ' americana ',
        ' funk ', ' drone ', ' edm ', ' folk ', ' lo fi ', ' blues ', ' punk ', ' pop ', ' new wave ',
        ' grindcore ', ' gospel ', ' latin ', ' dubstep ', ' choral ', ' orchestral ', ' opera ', ' choir ',
        ' baroque ', ' renaissance ', ' medieval ', ' romantic ', ' modern ', ' contemporary ',
    ]

    artist_info_df = pd.read_csv(OUTPUT_DIR + DATAFRAME_MNTHLSTNRS, usecols=["ids", "names", "genres"])
    artist_ids = artist_info_df[artist_info_df["genres"].isnull()]["ids"].tolist()
    logger.info(f"Number of artists with no genres: {len(artist_ids)}")

    artist_ids = artist_ids[start_idx:] # crude but fine, we did some before
    for i in range(300):
        artist_ids_genres, artist_genres, artist_names = [], [], []
        for artist_id in artist_ids[:100]:
            artist_name = artist_info_df[artist_info_df["ids"] == artist_id]["names"].values[0]
            genres = scrape_genres_from_bio(artist_id, artist_name, genres_to_search)
            if genres:
                genres = ','.join(genres)
                artist_ids_genres.append(artist_id)
                artist_genres.append(genres)
                artist_names.append(artist_name)
                logger.info(f"Genres {genres} found for: {artist_name}, {artist_id}.")
            else:
                logger.info(f"Genres not found for artist id: {artist_id} and artist name: {artist_name}, {artist_id}.")
    
        artist_info_genres_df = pd.DataFrame({"ids": artist_ids_genres, "names": artist_names, "genres": artist_genres})
        if os.path.exists(OUTPUT_DIR + "Spotify_bio_genres.csv"):
            artist_info_genres_df.to_csv(OUTPUT_DIR + "Spotify_bio_genres.csv", mode='a', header=False, index=False)
            logger.info(f'Appended {len(artist_info_genres_df)} artist genres to existing file: {OUTPUT_DIR + "Spotify_bio_genres.csv"}')
        else:
            artist_info_genres_df.to_csv(OUTPUT_DIR + "Spotify_bio_genres.csv", index=False)
            logger.info(f'Saved {len(artist_info_genres_df)} artist genres to new file: {OUTPUT_DIR + "Spotify_bio_genres.csv"}')
        artist_ids = artist_ids[100:]
        logger.info(f"Sleeping for 10 seconds.")
        time.sleep(10)