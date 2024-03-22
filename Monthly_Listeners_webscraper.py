import requests
from bs4 import BeautifulSoup
import pandas as pd
import os, time

# TO DO: put these in a config file
DEFAULT_OUTPUT_DIR = "/n/holystore01/LABS/itc_lab/Users/sjeffreson/serch/artist-database/"
DATAFRAME_ORIG = "Spotify_artist_info.csv"
DATAFRAME_MNTHLSTNRS = "Spotify_artist_info_Mnth-Lstnrs.csv"

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

if __name__ == "__main__":
    for chunk in pd.read_csv(DEFAULT_OUTPUT_DIR + DATAFRAME_ORIG, chunksize=100):
        artist_ids = chunk["ids"].tolist()
        artist_names = chunk["names"].tolist()

        monthly_listeners_batch = []
        for artist_id, artist_name in zip(artist_ids, artist_names):
            if artist_id == "missing":
                monthly_listeners_batch.append(-1)
                continue
            monthly_listeners = scrape_monthly_listeners(artist_id, artist_name)
            if monthly_listeners != None:
                monthly_listeners_batch.append(monthly_listeners)
            else:
                monthly_listeners_batch.append(-1)

        '''Add monthly listeneners as new column to dataframe chunk, and append to new CSV'''
        # TO DO: Check if the file exists, and if it does, append to it
        # Check if the artist already has a Monthly listener count, and if so, skip or update
        chunk["monthly_listeners"] = monthly_listeners_batch
        if not os.path.exists(DEFAULT_OUTPUT_DIR + DATAFRAME_MNTHLSTNRS):
            chunk.to_csv(DEFAULT_OUTPUT_DIR + DATAFRAME_MNTHLSTNRS, index=False)
        else:
            chunk.to_csv(DEFAULT_OUTPUT_DIR + DATAFRAME_MNTHLSTNRS, mode='a', header=False, index=False)

        print('Found and saved monthly listeners for one batch of 100 artists. Sleeping for 30s...')
        time.sleep(30)