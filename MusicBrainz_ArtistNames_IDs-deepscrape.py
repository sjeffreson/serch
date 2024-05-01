import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.exceptions import SpotifyException

import random
import numpy as np
import pandas as pd
import artist_info_helper as aih

from typing import List, Dict, Tuple
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
import sys, os, glob, re
regex = re.compile(r"\d+")

import pickle, json
import time
from datetime import datetime

import signal
def handler(signum, frame) -> None:
    raise TimeoutError("Operation timed out")

'''
Remember environment variables:
export SPOTIPY_CLIENT_ID='your-spotify-client-id'
export SPOTIPY_CLIENT_SECRET='your-spotify-client-secret'
export SPOTIPY_REDIRECT_URI='your-app-redirect-url'
'''

client_credentials_manager = SpotifyClientCredentials()
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

with open('config.json') as f:
    config = json.load(f)
OUTPUT_DIR = config['paths']['output_dir']
ARTIST_IDS_FILE = config['filenames']['artist_ids']
MISSING_NAMES_FILE = config['filenames']['missing_names']

def save_deepscraped_missing_ids(num_to_scrape=1000) -> None:
    '''Check through missing_names.csv and try again to get the IDs for these artists,
    using the maximum search limit of 1,000. If the ID still isn't found, shift to a new
    file deep-missing_names.csv. If found, add to the artist_ids.csv file.'''

    missing_names_df = pd.read_csv(OUTPUT_DIR + MISSING_NAMES_FILE)
    missing_names = missing_names_df['names'].tolist()
    if num_to_scrape is not None:
        missing_names = missing_names[:num_to_scrape]

    deep_found_ids, deep_found_names, deep_missing_names = [], [], []
    for missing_name in missing_names:
        spot_id = aih.get_artist_spotify_id_deepscrape(missing_name)
        if spot_id is not None:
            deep_found_ids.append(spot_id)
            deep_found_names.append(missing_name)
        else:
            deep_missing_names.append(missing_name)

    '''Append the deep-found IDs to the artist_ids.csv file'''
    if len(deep_found_ids) > 0:
        deep_found_ids_df = pd.DataFrame({'names': deep_found_names, 'ids': deep_found_ids})
        if not os.path.exists(OUTPUT_DIR + ARTIST_IDS_FILE):
            deep_found_ids_df.to_csv(OUTPUT_DIR + ARTIST_IDS_FILE, index=False)
            logger.info(f'Saved {len(deep_found_ids_df)} deep-scraped artist IDs to new file: {OUTPUT_DIR + ARTIST_IDS_FILE}')
        else:
            deep_found_ids_df.to_csv(OUTPUT_DIR + ARTIST_IDS_FILE, mode='a', header=False, index=False)
            logger.info(f'Appended {len(deep_found_ids_df)} deep-scraped artist IDs to existing file: {OUTPUT_DIR + ARTIST_IDS_FILE}')

    '''Save the deep-missing names to a new file'''
    deep_missing_names_df = pd.DataFrame({'names': deep_missing_names})
    if not os.path.exists(OUTPUT_DIR + 'deep_missing_names.csv'):
        deep_missing_names_df.to_csv(OUTPUT_DIR + 'deep_missing_names.csv', index=False)
        logger.info(f'Saved {len(deep_missing_names_df)} deep-missing artist names to new file: {OUTPUT_DIR + "deep_missing_names.csv"}')
    else:
        deep_missing_names_df.to_csv(OUTPUT_DIR + 'deep_missing_names.csv', mode='a', header=False, index=False)
        logger.info(f'Appended {len(deep_missing_names_df)} deep-missing artist names to existing file: {OUTPUT_DIR + "deep_missing_names.csv"}')
    
    '''Remove the scraped names from the missing names file'''
    missing_names_df = missing_names_df[~missing_names_df['names'].isin(deep_missing_names)]
    deep_found_names_df = pd.DataFrame({'names': deep_found_names})
    missing_names_df = missing_names_df[~missing_names_df['names'].isin(deep_found_names)]
    missing_names_df.to_csv(OUTPUT_DIR + MISSING_NAMES_FILE, index=False)
    logger.info(f'Removed {len(deep_missing_names) + len(deep_found_names)} deep-scraped artist names from missing names file: {OUTPUT_DIR + MISSING_NAMES_FILE}')

if __name__ == "__main__":
    for i in range(100):
        save_deepscraped_missing_ids(num_to_scrape=100)
        logger.info(f'Waiting 30 seconds before next batch.')
        time.sleep(30)