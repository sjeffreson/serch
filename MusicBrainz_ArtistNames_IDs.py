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

import pickle
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

DEFAULT_OUTPUT_DIR = "/n/holystore01/LABS/itc_lab/Users/sjeffreson/serch/artist-database/"
DEFAULT_ARTIST_NAMES_FILE = "all_artist_names.csv"
DEFAULT_ARTIST_IDS_FILE = "artist_ids.csv"
DEFAULT_MISSING_NAMES_FILE = "missing_names.csv"
DEFAULT_DEEP_MISSING_NAMES_FILE = "deep_missing_names.csv"
CURRENT_YEAR = datetime.now().year
TIMEOUT = 10*60

class ArtistNames:
    '''Analysis functions for the list of artist names from the MusicBrainz database.
    Retrieval of Spotify information for these artists, and storage of this information.'''
    def __init__(
        self,
        rand_num_artist_names: int = None,
        random_seed: int = 42,
        max_num_artist_names: int = 1e6,
    ):
        '''args:
            rand_num_artist_names: Number of artist names to retrieve from the MusicBrainz
            database via random selection. If None, no random selection occurs.
            random_seed: Seed for random selection of artist names.
            max_num_artist_names: Number of names to retrieve from the MusicBrainz database
            in order.
        '''

        self.rand_num_artist_names = rand_num_artist_names
        self.random_seed = random_seed
        self.max_num_artist_names = int(max_num_artist_names)

        '''Load the artist names from the MusicBrainz database, stored as csv file'''
        self.artist_names = pd.read_csv(DEFAULT_OUTPUT_DIR + DEFAULT_ARTIST_NAMES_FILE)['artist_name'].tolist()

        '''Initialize stores for artist_ids and missing names'''
        self.artist_ids, self.missing_names = [], []

    def clean_artist_names(self) -> None:
        '''Clean bad and empty strings from the list of artist names, convert to lowercase'''

        seen = set()
        artist_names_unique = [name for name in self.artist_names if not (name in seen or seen.add(name))]
        artist_names_good = [name for name in artist_names_unique if name is not None and type(name) == str]
        artist_names_nonzero = [name for name in artist_names_good if len(name) > 0]
        artist_names_nobrack = [name for name in artist_names_nonzero if '[' not in name and ']' not in name]

        self.artist_names = [str(name.lower()) for name in artist_names_nobrack]

    def sample_artist_names(self) -> None:
        '''Whittle down to a sample of artist names, from the total list retrieved from
        MusicBrainz.'''

        if self.rand_num_artist_names is not None:
            random.seed(self.random_seed)
            self.artist_names = random.sample(self.artist_names, self.rand_num_artist_names)
        else:
            self.artist_names = self.artist_names[:self.max_num_artist_names]

    def get_names_already_in_file(self, filename: str) -> List[str]:
        '''Retrieve artist names already stored in the file, to avoid duplicates'''

        saved_artists = []
        if os.path.exists(DEFAULT_OUTPUT_DIR + filename):
            for chunk in pd.read_csv(DEFAULT_OUTPUT_DIR + filename, usecols=['names'], chunksize=1000):
                chunk_artists = [name.lower() for name in chunk['names']]
                artist_names = [name.lower() for name in self.artist_names]
                saved_artists.extend(list(set(artist_names) & set(chunk_artists)))            
                if len(saved_artists) == len(artist_names):
                    break
            return saved_artists
        else:
            return saved_artists

    def only_new_names(self) -> None:
        '''Check if artist names in list already have assigned info in the local datastore,
        or to the list of missing names, and return those that do not.'''

        if os.path.exists(DEFAULT_OUTPUT_DIR + DEFAULT_ARTIST_IDS_FILE):
            saved_artists = self.get_names_already_in_file(DEFAULT_ARTIST_IDS_FILE)
        if os.path.exists(DEFAULT_OUTPUT_DIR + DEFAULT_MISSING_NAMES_FILE):
            missing_artists = self.get_names_already_in_file(DEFAULT_MISSING_NAMES_FILE)
        if os.path.exists(DEFAULT_OUTPUT_DIR + DEFAULT_DEEP_MISSING_NAMES_FILE):
            deep_missing_artists = self.get_names_already_in_file(DEFAULT_DEEP_MISSING_NAMES_FILE)
        saved_artists += missing_artists + deep_missing_artists

        if len(saved_artists) > 0:
            logger.info(f'Already have {len(saved_artists)} of requested artists.')
            new_artist_names = [name for name in self.artist_names if name not in saved_artists]
            self.artist_names = new_artist_names
            
        logger.info(f'Found {len(self.artist_names)} new artists to fetch from Spotify')

    def retrieve_artist_ids(self) -> None:
        '''Assign Spotify IDs to the artist names in the list. This includes a time-out
        condition in case we hit the daily rate limit.'''

        artist_names_batch = [self.artist_names[i:i + 100] for i in range(0, len(self.artist_names), 100)]
        for artist_names in artist_names_batch:
            try:
                signal.signal(signal.SIGALRM, handler)
                signal.alarm(TIMEOUT)
                for artist_name in artist_names:
                    spot_id = aih.get_artist_spotify_id(artist_name)
                    if spot_id is not None:
                        self.artist_ids.append(spot_id)
                    else:
                        self.missing_names.append(artist_name)
                logger.info(f'Fetched {len(self.artist_ids)} artist ids from Spotify.')
                signal.alarm(0)
                if artist_names != artist_names_batch[-1]:
                    logger.info(f'Waiting 30 seconds before next batch.')
                    time.sleep(30)
            except TimeoutError as e:
                logger.critical(f"Operation timed out: {e}")
                sys.exit(1)

    def save_artist_ids(self) -> None:
        if len(self.artist_names)>0 and len(self.artist_ids)==0:
            logger.warning(f"No artist IDs fetched yet, fetching now...")
            self.retrieve_artist_ids()

        '''Remove missing names from names'''
        self.artist_names = [name for name in self.artist_names if name not in self.missing_names]

        '''Save artist names, ids and missing ids to a CSV file'''
        artist_ids_df = pd.DataFrame({'names': self.artist_names, 'ids': self.artist_ids})
        if not os.path.exists(DEFAULT_OUTPUT_DIR + DEFAULT_ARTIST_IDS_FILE):
            artist_ids_df.to_csv(DEFAULT_OUTPUT_DIR + DEFAULT_ARTIST_IDS_FILE, index=False)
            logger.info(f'Saved {len(artist_ids_df)} artist IDs to new file: {DEFAULT_OUTPUT_DIR + DEFAULT_ARTIST_IDS_FILE}')
        else:
            artist_ids_df.to_csv(DEFAULT_OUTPUT_DIR + DEFAULT_ARTIST_IDS_FILE, mode='a', header=False, index=False)
            logger.info(f'Appended {len(artist_ids_df)} artist IDs to existing file: {DEFAULT_OUTPUT_DIR + DEFAULT_ARTIST_IDS_FILE}')

        missing_names_df = pd.DataFrame({'missing_names': self.missing_names})
        if not os.path.exists(DEFAULT_OUTPUT_DIR + DEFAULT_MISSING_NAMES_FILE):
            missing_names_df.to_csv(DEFAULT_OUTPUT_DIR + DEFAULT_MISSING_NAMES_FILE, index=False)
            logger.info(f'Saved {len(missing_names_df)} missing artist names to new file: {DEFAULT_OUTPUT_DIR + DEFAULT_MISSING_NAMES_FILE}')
        else:
            missing_names_df.to_csv(DEFAULT_OUTPUT_DIR + DEFAULT_MISSING_NAMES_FILE, mode='a', header=False, index=False)
            logger.info(f'Appended {len(missing_names_df)} missing artist names to existing file: {DEFAULT_OUTPUT_DIR + DEFAULT_MISSING_NAMES_FILE}')
    
if __name__ == "__main__":
    '''Spotify seems to have an (undisclosed) daily limit on the number of requests that
    can be made. Keep this in mind.'''

    '''Check total number of rows in the existing IDs file and missing names file combined'''
    total_names_stored = 0
    if os.path.exists(DEFAULT_OUTPUT_DIR + DEFAULT_ARTIST_IDS_FILE):
        artist_ids_df = pd.read_csv(DEFAULT_OUTPUT_DIR + DEFAULT_ARTIST_IDS_FILE)
        total_names_stored = len(artist_ids_df)
    if os.path.exists(DEFAULT_OUTPUT_DIR + DEFAULT_MISSING_NAMES_FILE):
        missing_names_df = pd.read_csv(DEFAULT_OUTPUT_DIR + DEFAULT_MISSING_NAMES_FILE)
        total_names_stored += len(missing_names_df)
    if os.path.exists(DEFAULT_OUTPUT_DIR + DEFAULT_DEEP_MISSING_NAMES_FILE):
        deep_missing_names_df = pd.read_csv(DEFAULT_OUTPUT_DIR + DEFAULT_DEEP_MISSING_NAMES_FILE)
        total_names_stored += len(deep_missing_names_df)

    '''Run the ID search on 2000 new artists in a batch. Stored names will in excluded
    from the sample, so if we keep the seed fixed, we need to sample 1000 more artists
    than are stored. This way we eventually build up the entire sample, but randomize
    the order, so that intermediate samples can be analyzed.'''

    num_artist_names = 2000 # to search in one batch
    random_seed = 42
    start_time = time.time()

    data = ArtistNames(
        rand_num_artist_names=total_names_stored + num_artist_names, # remember: stored names will be excluded
        random_seed=random_seed
    )

    '''Clean bad and empty strings from the list of artist names, convert to lowercase
    for later comparison.'''
    data.clean_artist_names()

    '''Get sample of names to work with'''
    data.sample_artist_names()

    '''Get the artist names that are not already stored'''
    data.only_new_names()

    '''Search and set the artist IDs for these names, save to file'''
    data.retrieve_artist_ids()
    data.save_artist_ids()

    print(f"Total time to load and save data: {time.time() - start_time}.")