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

import json, pickle, time
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
ARTIST_NAMES_FILE = config['filenames']['artist_names']
ARTIST_IDS_FILE = config['filenames']['artist_ids']
MISSING_NAMES_FILE = config['filenames']['missing_names']
DEEP_MISSING_NAMES_FILE = config['filenames']['deep_missing_names']
CURRENT_YEAR = datetime.now().year
TIMEOUT = 10*60

def load_clean_artist_names() -> List[str]:
    '''Load data, clean bad and empty strings from the list of artist names, convert to lowercase'''

    artist_names = pd.read_csv(OUTPUT_DIR + ARTIST_NAMES_FILE)['artist_name'].tolist()

    seen = set()
    artist_names_unique = [name for name in artist_names if not (name in seen or seen.add(name))]
    artist_names_good = [name for name in artist_names_unique if name is not None and type(name) == str]
    artist_names_nonzero = [name for name in artist_names_good if len(name) > 0]
    artist_names_nobrack = [name for name in artist_names_nonzero if '[' not in name and ']' not in name]
    artist_names = [str(name.lower()) for name in artist_names_nobrack]

    return artist_names

def sample_artist_names(artist_names: List[str], random_seed: int, rand_num_artist_names: int) -> List[str]:
    '''Whittle down to a sample of artist names, from the total list retrieved from
    MusicBrainz.'''

    random.seed(random_seed)
    return random.sample(artist_names, rand_num_artist_names)

def get_names_already_in_file(artist_names: List[str], filename: str) -> List[str]:
    '''Retrieve artist names already stored in the file, to avoid duplicates'''

    saved_artists = []
    if os.path.exists(OUTPUT_DIR + filename):
        for chunk in pd.read_csv(OUTPUT_DIR + filename, usecols=['names'], chunksize=1000):
            chunk_artists = [name.lower() for name in chunk['names']]
            artist_names = [name.lower() for name in artist_names]
            saved_artists.extend(list(set(artist_names) & set(chunk_artists)))            
            if len(saved_artists) == len(artist_names):
                break
        return saved_artists
    else:
        return saved_artists

def only_new_names(artist_names: List[str]) -> List[str]:
    '''Check if artist names in list already have assigned info in the local datastore,
    or to the list of missing names, and return those that do not.'''

    if os.path.exists(OUTPUT_DIR + ARTIST_IDS_FILE):
        saved_artists = get_names_already_in_file(artist_names, ARTIST_IDS_FILE)
    if os.path.exists(OUTPUT_DIR + MISSING_NAMES_FILE):
        missing_artists = get_names_already_in_file(artist_names, MISSING_NAMES_FILE)
    if os.path.exists(OUTPUT_DIR + DEEP_MISSING_NAMES_FILE):
        deep_missing_artists = get_names_already_in_file(artist_names, DEEP_MISSING_NAMES_FILE)
    saved_artists += missing_artists + deep_missing_artists

    if len(saved_artists) > 0:
        logger.info(f'Already have {len(saved_artists)} of requested artists.')
        new_artist_names = [name for name in artist_names if name not in saved_artists]
        artist_names = new_artist_names
        
    logger.info(f'Found {len(artist_names)} new artists to fetch from Spotify')
    return artist_names

def retrieve_artist_ids(artist_names: List[str]) -> Tuple[List[str], List[str]]:
    '''Assign Spotify IDs to the artist names in the list. This includes a time-out
    condition in case we hit the daily rate limit.'''

    artist_ids, missing_names = [], []
    artist_names_batch = [artist_names[i:i + 100] for i in range(0, len(artist_names), 100)]
    for artist_names in artist_names_batch:
        try:
            signal.signal(signal.SIGALRM, handler)
            signal.alarm(TIMEOUT)
            for artist_name in artist_names:
                spot_id = aih.get_artist_spotify_id(artist_name)
                if spot_id is not None:
                    artist_ids.append(spot_id)
                else:
                    missing_names.append(artist_name)
            logger.info(f'Fetched {len(artist_ids)} artist ids from Spotify.')
            signal.alarm(0)
            if artist_names != artist_names_batch[-1]:
                logger.info(f'Waiting 30 seconds before next batch.')
                time.sleep(30)
        except TimeoutError as e:
            logger.critical(f"Operation timed out: {e}")
            sys.exit(1)

    return artist_ids, missing_names

def save_artist_ids(artist_names: List[str]) -> None:
    '''Save the artist names, IDs and missing names to a CSV file'''

    artist_ids, missing_names = retrieve_artist_ids(artist_names)
    artist_names = [name for name in artist_names if name not in missing_names]

    artist_ids_df = pd.DataFrame({'names': artist_names, 'ids': artist_ids})
    if not os.path.exists(OUTPUT_DIR + ARTIST_IDS_FILE):
        artist_ids_df.to_csv(OUTPUT_DIR + ARTIST_IDS_FILE, index=False)
        logger.info(f'Saved {len(artist_ids_df)} artist IDs to new file: {OUTPUT_DIR + ARTIST_IDS_FILE}')
    else:
        artist_ids_df.to_csv(OUTPUT_DIR + ARTIST_IDS_FILE, mode='a', header=False, index=False)
        logger.info(f'Appended {len(artist_ids_df)} artist IDs to existing file: {OUTPUT_DIR + ARTIST_IDS_FILE}')

    missing_names_df = pd.DataFrame({'missing_names': missing_names})
    if not os.path.exists(OUTPUT_DIR + MISSING_NAMES_FILE):
        missing_names_df.to_csv(OUTPUT_DIR + MISSING_NAMES_FILE, index=False)
        logger.info(f'Saved {len(missing_names_df)} missing artist names to new file: {OUTPUT_DIR + MISSING_NAMES_FILE}')
    else:
        missing_names_df.to_csv(OUTPUT_DIR + MISSING_NAMES_FILE, mode='a', header=False, index=False)
        logger.info(f'Appended {len(missing_names_df)} missing artist names to existing file: {OUTPUT_DIR + MISSING_NAMES_FILE}')
    
def extend_sample_MusicBrainz_artist_ids(num_artist_names: int=1000, random_seed: int=42) -> None:
    '''Check total number of rows in the existing IDs file and missing names file combined'''
    total_names_stored = 0
    if os.path.exists(OUTPUT_DIR + ARTIST_IDS_FILE):
        artist_ids_df = pd.read_csv(OUTPUT_DIR + ARTIST_IDS_FILE)
        total_names_stored = len(artist_ids_df)
    if os.path.exists(OUTPUT_DIR + MISSING_NAMES_FILE):
        missing_names_df = pd.read_csv(OUTPUT_DIR + MISSING_NAMES_FILE)
        total_names_stored += len(missing_names_df)
    if os.path.exists(OUTPUT_DIR + DEEP_MISSING_NAMES_FILE):
        deep_missing_names_df = pd.read_csv(OUTPUT_DIR + DEEP_MISSING_NAMES_FILE)
        total_names_stored += len(deep_missing_names_df)

    '''Run the ID search on N new artists in batches. Stored names will in excluded
    from the sample, so if we keep the seed fixed, we need to sample N more artists
    than are stored. This way we eventually build up the entire sample, but randomize
    the order, so that intermediate samples can be analyzed.'''
    start_time = time.time()

    artist_names = load_clean_artist_names()
    artist_names = sample_artist_names(
        artist_names=artist_names,
        random_seed=random_seed,
        rand_num_artist_names=total_names_stored + num_artist_names # remember: stored names will be excluded
    )
    artist_names = only_new_names(artist_names)
    save_artist_ids(artist_names)

    print(f"Total time to load and save data: {time.time() - start_time}.")

if __name__ == "__main__":
    '''Spotify seems to have an (undisclosed) daily limit on the number of requests that
    can be made. Keep this in mind.'''
    extend_sample_MusicBrainz_artist_ids(num_artist_names=10, random_seed=42)
