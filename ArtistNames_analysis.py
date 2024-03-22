import spotipy
from spotipy.oauth2 import SpotifyOAuth
from spotipy.exceptions import SpotifyException

import pickle
import sys, os, glob, re
regex = re.compile(r"\d+")
import random
import numpy as np
from typing import List, Dict, Tuple
import argparse, logging
import time
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import pandas as pd
import artist_info_helper as aih

import signal
def handler(signum, frame):
    raise TimeoutError("Operation timed out")

'''
Remember environment variables:
export SPOTIPY_CLIENT_ID='your-spotify-client-id'
export SPOTIPY_CLIENT_SECRET='your-spotify-client-secret'
export SPOTIPY_REDIRECT_URI='your-app-redirect-url'
'''

scope = "playlist-read-private playlist-read-collaborative"
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))

# TO DO: put these in a config file
DEFAULT_OUTPUT_DIR = "/n/holystore01/LABS/itc_lab/Users/sjeffreson/serch/artist-database/"
DEFAULT_ARTIST_NAMES_FILE = "all_artist_names.csv"
DEFAULT_DATAFRAME_FILE = "Spotify_artist_info.csv"
TIMEOUT = 10*60

class ArtistNames:
    '''Analysis functions for the list of artist names from the MusicBrainz database.
    Retrieval of Spotify information for these artists, cleaning of artists not on
    Spotify, cleaning of inactive artists, etc.'''
    def __init__(
        self,
        max_num_artist_names: int = 1e6,
        rand_num_artist_names: int = None,
        random_seed: int = 42,
        current_year: int = 2024
    ):

        self.max_num_artist_names = int(max_num_artist_names)
        self.rand_num_artist_names = rand_num_artist_names
        self.random_seed = random_seed
        self.current_year = current_year

        '''Load the artist names from the MusicBrainz database, stored as csv file'''
        artist_names = pd.read_csv(DEFAULT_OUTPUT_DIR + DEFAULT_ARTIST_NAMES_FILE)['artist_name'].tolist()

        '''Cleaning bad and empty strings from the list of artist names'''
        artist_names = self.remove_duplicates_preserve_order(artist_names)
        artist_names = [name for name in artist_names if name is not None and type(name) == str]
        artist_names = [name for name in artist_names if len(name) > 0]
        '''remove names with brackets, these are indicated as not artists by MusicBrainz'''
        artist_names = [name for name in artist_names if '[' not in name and ']' not in name]

        if self.rand_num_artist_names is not None:
            random.seed(self.random_seed)
            self.artist_names = random.sample(artist_names, self.rand_num_artist_names)
        else:
            self.artist_names = artist_names[:self.max_num_artist_names]
        del artist_names
        self.artist_names = [str(name.lower()) for name in self.artist_names] # for comparison
        self.artist_info = None

    def remove_duplicates_preserve_order(self, seq) -> List[str]:
        seen = set()
        return [x for x in seq if not (x in seen or seen.add(x))]

    def get_artist_info(self):
        '''Get the relevant information for the set of artist names saved from the
        MusicBrainz database. Use batches of 100 artists with timeout in between to
        avoid rate limiting.'''
        ## TO DO: separate ID collection and info collection into two functions, this
        ## is not readable.

        '''Check if artists already stored in local datastore, if so load from there'''
        saved_artists = []
        if not os.path.exists(DEFAULT_OUTPUT_DIR + DEFAULT_DATAFRAME_FILE):
            new_artists = self.artist_names
        else:
            for chunk in pd.read_csv(DEFAULT_OUTPUT_DIR + DEFAULT_DATAFRAME_FILE, usecols=['names'], chunksize=1000):
                chunk_artists = [name.lower() for name in chunk['names']]
                artist_names = [name.lower() for name in self.artist_names]
                saved_artists.extend(list(set(artist_names) & set(chunk_artists)))            
                if len(saved_artists) == len(artist_names):
                    break
            new_artists = [name for name in self.artist_names if name not in saved_artists]
            logger.info(f'Already have {len(saved_artists)} of requested artists stored at: {DEFAULT_OUTPUT_DIR + DEFAULT_DATAFRAME_FILE}')
            logger.info(f'Found {len(new_artists)} new artists to fetch from Spotify')

        '''Retrieve IDs from Spotify'''
        artist_ids, missing_names = [], []
        artist_names_batch = [new_artists[i:i + 100] for i in range(0, len(new_artists), 100)]
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

        '''Maximum 50 artists per request to sp.artists'''
        try:
            signal.signal(signal.SIGALRM, handler)
            signal.alarm(TIMEOUT)
            artists_info = []
            artist_ids_batches = [artist_ids[i:i + 50] for i in range(0, len(artist_ids), 50)]
            for artist_ids_batch in artist_ids_batches:
                artists_info_batch = sp.artists(artist_ids_batch)['artists']
                artists_info.extend(artists_info_batch)
            signal.alarm(0)
        except TimeoutError as e:
            logger.critical(f"Operation timed out: {e}")
            sys.exit(1)

        logger.info(f"Fetched {len(artists_info)} artists' info from Spotify.")
        logger.info(f'Waiting 1 minute before querying albums.')
        if(len(artists_info) > 0):
            time.sleep(60)

        '''Extract all info into a dictionary, note that this involves a call to
        artist_albums for each artist, hence the batching and timeout.'''
        all_artist_info = aih.ArtistInfoDict()
        try:
            signal.signal(signal.SIGALRM, handler)
            signal.alarm(TIMEOUT)
            logger.info(f'Fetching release dates and number of tracks for {len(artists_info)} artists, will timeout.')
            artist_info_batches = [artists_info[i:i + 50] for i in range(0, len(artists_info), 50)]
            for artist_info_batch in artist_info_batches:
                for artist_info in artist_info_batch:
                    all_artist_info.append_artist_info(artist_info)
                logger.info(f'Fetched release dates and number of tracks for {len(artist_info_batch)} artists. Pausing for 30s...')
                time.sleep(30)
            signal.alarm(0)
        except TimeoutError as e:
            logger.critical(f"Operation timed out: {e}")
            sys.exit(1)
        all_artist_info.check_equal_length()

        '''Store missing IDs too'''
        for missing_name in missing_names:
            all_artist_info.append_missing_info(missing_name)

        '''Save newly fetched artist info to local datastore'''
        df_new = pd.DataFrame(all_artist_info.data)
        if not os.path.exists(DEFAULT_OUTPUT_DIR + DEFAULT_DATAFRAME_FILE):
            df_new.to_csv(DEFAULT_OUTPUT_DIR + DEFAULT_DATAFRAME_FILE, index=False)
            logger.info(f'Saved {len(df_new)} artists to new file: {DEFAULT_OUTPUT_DIR + DEFAULT_DATAFRAME_FILE}')
        else:
            df_new.to_csv(DEFAULT_OUTPUT_DIR + DEFAULT_DATAFRAME_FILE, mode='a', header=False, index=False)
            logger.info(f'Appended {len(df_new)} artists to existing file: {DEFAULT_OUTPUT_DIR + DEFAULT_DATAFRAME_FILE}')

        '''load all rows from dataframe corresponding to self.artist_names.'''
        df = pd.read_csv(DEFAULT_OUTPUT_DIR + DEFAULT_DATAFRAME_FILE)
        df = df[df['names'].isin(self.artist_names)]

        self.artist_info = df[df['ids'] != 'missing']

    def load_artist_info_from_file(self, file_name: str = None):
        '''Load the artist information from a previously-created CSV file, and assign
        it to the artist_info attribute.'''
        if self.artist_info is None:
            self.artist_info = pd.read_csv(DEFAULT_OUTPUT_DIR + file_name)

    def get_active_artists(self, strict=False) -> Dict[str, np.array]:
        '''Get artists that fulfil both of the following:
        1. Have produced new music in the past five (two) years
        2. If they're more than two (zero) years old, have an average of
        two (four) tracks per year since their first release.
        This does not include legacy artists who were famous but have retired.
        If strict is True, the stricter versions of both conditions are used.
        '''

        if strict:
            num_years = 2
            num_tracks_per_year = 4
            first_release_cnd = 0 # start counting tracks from this many years before current
        else:
            num_years = 5
            num_tracks_per_year = 2
            first_release_cnd = 2

        if self.artist_info is None:
            logger.info('Fetching artist info from Spotify...')
            self.get_artist_info()

        cnd0 = np.array([num_releases > 0 for num_releases in self.artist_info['num_releases']])
        active_artist_info = {key: np.array(self.artist_info[key])[cnd0] for key in self.artist_info.keys()}

        cnd1 = np.array([last_release > self.current_year - num_years for last_release in active_artist_info['last_release']])
        active_artist_info = {key: np.array(active_artist_info[key])[cnd1] for key in active_artist_info.keys()}

        cnd2 = np.array([
            (self.current_year - first_release < first_release_cnd) or
            (num_tracks > (self.current_year - first_release)*num_tracks_per_year)
            for first_release, num_tracks
            in zip(active_artist_info['first_release'], active_artist_info['num_tracks'])
        ])
        active_artist_info = {key: np.array(active_artist_info[key])[cnd2] for key in active_artist_info.keys()}

        return active_artist_info

    def get_new_active_artists(self) -> Dict[str, np.array]:
        '''Get artists that fulfil both of the following:
        1. Have a first release date in the past five years
        2. Have an average of two tracks per year since their first release
        '''

        active_artists = self.get_active_artists()
        cnd_new = np.array(
            [first_release > self.current_year - 5 for first_release in active_artists['first_release']]
        )
        new_active_artists = {key: np.array(active_artists[key])[cnd_new] for key in active_artists.keys()}

        return new_active_artists

    def get_legacy_artists(self) -> Dict[str, np.array]:
        '''Get the artists that fulfil all of the following:
        1. Have not produced new music in the past five years
        2. Have had more than 2 lifetime releases
        3. Have more tracks than years between their first and last releases
        This removes inactive artists that never found success due to lack of trying.'''

        if self.artist_info is None:
            logger.info('Fetching artist info from Spotify...')
            self.get_artist_info()

        cnd0 = np.array([num_releases > 0 for num_releases in self.artist_info['num_releases']])
        legacy_artist_info = {key: np.array(self.artist_info[key])[cnd0] for key in self.artist_info.keys()}

        cnd1 = np.array([
            num_releases > 2 and last_release < self.current_year - 5 for num_releases, last_release
            in zip(legacy_artist_info['num_releases'], legacy_artist_info['last_release'])
        ])
        legacy_artist_info = {key: np.array(legacy_artist_info[key])[cnd1] for key in legacy_artist_info.keys()}

        cnd2 = np.array([
            num_tracks > last_release - first_release for first_release, last_release, num_tracks
            in zip(
                legacy_artist_info['first_release'],
                legacy_artist_info['last_release'],
                legacy_artist_info['num_tracks']
            )
        ])
        legacy_artist_info = {key: np.array(legacy_artist_info[key])[cnd2] for key in legacy_artist_info.keys()}

        return legacy_artist_info

    def get_famous_legacy_artists(self) -> Dict[str, np.array]:
        '''Get the artists that have had more than 2 lifetime releases, have not
        produced new music in the past five years, and have a Spotify popularity
        score of at least 50.'''

        if self.artist_info is None:
            logger.info('Fetching artist info from Spotify...')
            self.get_artist_info()

        cnd0 = np.array([num_releases > 0 for num_releases in self.artist_info['num_releases']])
        famous_legacy_artist_info = {key: np.array(self.artist_info[key])[cnd0] for key in self.artist_info.keys()}

        cnd1 = np.array([
            num_releases > 2 and last_release < self.current_year - 5 and popularity > 50
            for num_releases, last_release, popularity
            in zip(
                famous_legacy_artist_info['num_releases'],
                famous_legacy_artist_info['last_release'],
                famous_legacy_artist_info['popularity']
            )
        ])
        famous_legacy_artist_info = {key: np.array(famous_legacy_artist_info[key])[cnd1] for key in famous_legacy_artist_info.keys()}

        return famous_legacy_artist_info