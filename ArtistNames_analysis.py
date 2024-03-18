import spotipy
from spotipy.oauth2 import SpotifyOAuth
from spotipy.exceptions import SpotifyException

import pandas as pd

import argparse, logging
import time
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import pickle
import sys, os, glob, re
regex = re.compile(r"\d+")
import random
import numpy as np
from typing import List, Dict, Tuple

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

DEFAULT_OUTPUT_DIR = "/n/holystore01/LABS/itc_lab/Users/sjeffreson/serch/artist-database/"
DEFAULT_ARTIST_NAMES_FILE = "all_artist_names.csv"
DEFAULT_DATAFRAME_FILE = "Spotify_artist_info.csv"

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

        self.max_num_artist_names = max_num_artist_names
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

        if rand_num_artist_names is not None:
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

    def get_artist_spotify_id(self, artist_name, limit=10) -> Dict[str, str]:
        '''Fetch artist ids from Spotify. Note the max search query string length is 100
        characters. Because the search returns the closest most popular match, it
        sometimes misses the artist we're looking for. To respect rate limits, set to 10
        by default, but we can comb through more forcefully later to double-check whether
        IDs are really missing.'''

        results = sp.search(q=f'artist:"{artist_name}"', type='artist', limit=limit)
        artists = results['artists']['items']

        if len(artists) == 0:
            return None
        else:
            for artist in artists:
                if artist['name'].lower() == artist_name.lower(): # make sure exact match, search returns closest
                    return artist['id']
            return None

    def get_artist_release_dates(self, artist_id) -> List[str]:
        '''Get the release years of all albums and singles by a given artist.'''

        artist_albums = sp.artist_albums(artist_id, album_type='album,single')

        release_dates, album_ids = [], []
        for album in artist_albums['items']:
            release_dates.append(album['release_date'])
            album_ids.append(album['id'])
        release_dates = [int(date.split('-')[0]) for date in release_dates]
        return album_ids, release_dates

    def get_artist_num_tracks(self, album_ids) -> int:
        '''Get the number of tracks in each album by a given artist. Maximum 20 albums per
        request, so split into batches of 20.'''

        album_ids_batches = [album_ids[i:i + 20] for i in range(0, len(album_ids), 20)]
        num_tracks = 0
        for album_ids_batch in album_ids_batches:
            albums = sp.albums(album_ids_batch)['albums']
            num_tracks += np.sum([album['total_tracks'] for album in albums])

        return num_tracks

    def get_artist_info(self):
        '''Get the relevant information for the set of artist names saved from the
        MusicBrainz database. Use batches of 100 artists with timeout in between to
        avoid rate limiting.'''

        all_artist_info = {
            'ids': [],
            'names': [],
            'popularity': [],
            'followers': [],
            'genres': [],
            'first_release': [],
            'last_release': [],
            'num_releases': [],
            'num_tracks': []
        }

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
            logger.info(f'Already have {len(saved_artists)} artists stored at: {DEFAULT_OUTPUT_DIR + DEFAULT_DATAFRAME_FILE}')
            logger.info(f'Found {len(new_artists)} new artists to fetch from Spotify')

        '''Retrieve IDs from Spotify'''
        timeout = 600
        artist_ids, missing_names = [], []
        artist_names_batch = [new_artists[i:i + 100] for i in range(0, len(new_artists), 100)]
        for artist_names in artist_names_batch:
            try:
                signal.signal(signal.SIGALRM, handler)
                signal.alarm(timeout)
                for artist_name in artist_names:
                    spot_id = self.get_artist_spotify_id(artist_name)
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
            signal.alarm(timeout)
            artists_info = []
            artist_ids_batches = [artist_ids[i:i + 50] for i in range(0, len(artist_ids), 50)]
            for artist_ids_batch in artist_ids_batches:
                artists_info_batch = sp.artists(artist_ids_batch)['artists']
                artists_info.extend(artists_info_batch)
            signal.alarm(0)
        except TimeoutError as e:
            logger.critical(f"Operation timed out: {e}")
            sys.exit(1)

        logger.info(f'Fetched {len(artists_info)} artists info from Spotify.')
        logger.info(f'Waiting 1 minute before querying albums.')
        if(len(artists_info) > 0):
            time.sleep(60)

        '''Store all information in a pandas DataFrame'''
        for artist_info in artists_info:
            all_artist_info['ids'].append(str(artist_info['id']))
            all_artist_info['names'].append(str(artist_info['name']).lower())
            all_artist_info['popularity'].append(int(artist_info['popularity']))
            all_artist_info['followers'].append(int(artist_info['followers']['total']))
            genres = ', '.join(artist_info['genres'])
            all_artist_info['genres'].append(str(genres))
            '''Get release dates and number of tracks for each artist'''

        '''Divide artists into batches of 30 to avoid rate limiting'''
        try:
            signal.signal(signal.SIGALRM, handler)
            signal.alarm(timeout)
            artist_info_batches = [artists_info[i:i + 30] for i in range(0, len(artists_info), 30)]
            for artist_info_batch in artist_info_batches:
                for artist_info in artist_info_batch:
                    album_ids, artist_release_dates = self.get_artist_release_dates(str(artist_info['id']))
                    if(len(artist_release_dates) == 0):
                        all_artist_info['first_release'].append(int(-1))
                        all_artist_info['last_release'].append(int(-1))
                        all_artist_info['num_releases'].append(0)
                        all_artist_info['num_tracks'].append(0)
                    else:
                        all_artist_info['first_release'].append(int(min(artist_release_dates)))
                        all_artist_info['last_release'].append(int(max(artist_release_dates)))
                        all_artist_info['num_releases'].append(int(len(artist_release_dates)))
                        all_artist_info['num_tracks'].append(int(self.get_artist_num_tracks(album_ids)))
                logger.info(f'Fetched release dates and number of tracks for {len(artist_info_batch)} artists. Pausing for 30s...')
                time.sleep(30)
            signal.alarm(0)
        except TimeoutError as e:
            logger.critical(f"Operation timed out: {e}")
            sys.exit(1)

        '''Store missing IDs too'''
        for missing_name in missing_names:
            all_artist_info['ids'].append('missing')
            all_artist_info['names'].append(str(missing_name).lower())
            all_artist_info['popularity'].append(-1)
            all_artist_info['followers'].append(-1)
            all_artist_info['genres'].append('missing')
            all_artist_info['first_release'].append(-1)
            all_artist_info['last_release'].append(-1)
            all_artist_info['num_releases'].append(-1)
            all_artist_info['num_tracks'].append(-1)

        df_new = pd.DataFrame(all_artist_info)

        '''Save newly fetched artist info to local datastore'''
        if not os.path.exists(DEFAULT_OUTPUT_DIR + DEFAULT_DATAFRAME_FILE):
            df_new.to_csv(DEFAULT_OUTPUT_DIR + DEFAULT_DATAFRAME_FILE, index=False)
            logger.info(f'Saved {len(df_new)} artists to new file: {DEFAULT_OUTPUT_DIR + DEFAULT_DATAFRAME_FILE}')
        else:
            df_new.to_csv(DEFAULT_OUTPUT_DIR + DEFAULT_DATAFRAME_FILE, mode='a', header=False, index=False)
            logger.info(f'Appended {len(df_new)} artists to existing file: {DEFAULT_OUTPUT_DIR + DEFAULT_DATAFRAME_FILE}')

        '''load all rows from dataframe corresponding to self.artist_names
        TO DO: Deal with possible case that CSV is very big, not an issue for now.'''
        df = pd.read_csv(DEFAULT_OUTPUT_DIR + DEFAULT_DATAFRAME_FILE)
        df = df[df['names'].isin(self.artist_names)]

        self.artist_info = df

    def get_active_artists(self, strict=False) -> Dict[str, np.array]:
        '''Get artists that fulfil both of the following:
        1. Have produced new music in the past five (two) years
        2. If they're more than 2 and less than 10 years old, have an average of
        one (four) track per year since their first release.
        This does not include legacy artists who were famous but have retired.
        If strict is True, the stricter versions of both conditions are used.
        '''

        if strict:
            num_years = 2
            num_tracks_per_year = 4
            first_release_cnd = 0 # start counting tracks from this many years before current
        else:
            num_years = 5
            num_tracks_per_year = 1
            first_release_cnd = 2

        if self.artist_info is None:
            logger.info('Fetching artist info from Spotify...')
            self.get_artist_info()

        cnd0 = np.array([num_releases > 0 for num_releases in self.artist_info['num_releases']])
        active_artist_info = {key: np.array(self.artist_info[key])[cnd0] for key in self.artist_info.keys()}

        cnd1 = np.array([last_release > self.current_year - num_years for last_release in active_artist_info['last_release']])
        active_artist_info = {key: np.array(active_artist_info[key])[cnd1] for key in active_artist_info.keys()}

        cnd2 = np.array([
            #(self.current_year - first_release > 10) or
            (self.current_year - first_release < first_release_cnd) or
            (num_tracks > (self.current_year - first_release)*num_tracks_per_year)
            for first_release, num_tracks
            in zip(active_artist_info['first_release'], active_artist_info['num_tracks'])
        ])
        active_artist_info = {key: np.array(active_artist_info[key])[cnd2] for key in active_artist_info.keys()}

        return active_artist_info


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

    # def get_specific_genre(self) -> Dict[str, np.array]:
    #     '''Get artists that have a specific genre.'''
    #     cnd = np.array(['pop' in genre for genre in self.artist_info['genres']])
    #     specific_genre_artist_info = {key: np.array(self.artist_info[key])[cnd] for key in self.artist_info.keys()}

    #     return specific_genre_artist_info

        # def comb_apparently_missing_artists(self):
    #     '''Go through the stored data and double-check whether artists are actually missing
    #     by working through 1000 search results. Dataset is fine for some crude checks for
    #     now, but we should really do this before publishing, as the search still orders by
    #     popularity.'''