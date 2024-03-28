import spotipy
from spotipy.oauth2 import SpotifyOAuth
from spotipy.exceptions import SpotifyException

import numpy as np
from datetime import datetime

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
from typing import List, Dict, Tuple

import time
import signal
def handler(signum, frame) -> None:
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
CURRENT_YEAR = datetime.now().year
TIMEOUT = 10*60

def get_artist_spotify_id(artist_name, limit=10) -> Dict[str, str]:
    '''Fetch artist id from Spotify by name. Note the max search query string length is 100
    characters. Because the search returns the closest *most popular* match, it
    sometimes misses the artist we're looking for. To respect rate limits, set to 10
    by default, but we can comb through more forcefully in another function to double-
    check whether an ID is really missing.'''

    results = sp.search(q=f'artist:"{artist_name}"', type='artist', limit=limit)
    artists = results['artists']['items']

    if len(artists) == 0:
        return None
    else:
        for artist in artists:
            if artist['name'].lower() == artist_name.lower(): # make sure exact match, search returns closest
                return artist['id']
        return None

def get_artist_release_dates(artist_id) -> List[str]:
    '''Get the release years of all albums and singles by a given artist.'''

    artist_albums = sp.artist_albums(artist_id, album_type='album,single')

    release_dates, album_ids, album_types, total_tracks = [], [], [], []
    for album in artist_albums['items']:
        album_type = album['album_type']
        if album_type == 'album' or album_type == 'single':
            release_dates.append(album['release_date'])
            album_types.append(album['album_type'])
            total_tracks = album['total_tracks']
            album_ids.append(album['id'])
    release_dates = [int(date.split('-')[0]) for date in release_dates]
    return album_ids, release_dates, album_types, total_tracks

class ArtistInfoDict:
    '''Data structure to store artist information. Default keys are listed.
    The user can exclude keys by passing a list of keys to exclude_keys.'''
    def __init__(self, artist_info=None, exclude_keys=None):
        self.keys = [
            'ids',
            'names',
            'popularity',
            'followers',
            'genres',
            'first_release',
            'last_release',
            'num_releases',
            'num_tracks'
        ]
        if exclude_keys:
            if 'ids' in exclude_keys:
                logger.critical("Cannot exclude 'ids' key.")
                sys.exit(1)
            self.keys = [key for key in self.keys if key not in exclude_keys]

        self.data = {key: [] for key in self.keys}

        if artist_info: # initial store
            self.append_artist_info(artist_info)

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = value

    def check_equal_length(self):
        '''Check that all lists in the dictionary have the same length.'''
        for key in self.keys:
            if len(self.data[key]) != len(self.data['ids']):
                logger.critical(f"Length of {key} does not match length of ids.")
                sys.exit(1)

    def append_artist_info(self, artist_info):
        if 'ids' in self.keys:
            self.data['ids'].append(str(artist_info['id']))
        if 'names' in self.keys:
            self.data['names'].append(str(artist_info['name']).lower())
        if 'popularity' in self.keys:
            self.data['popularity'].append(int(artist_info['popularity']))
        if 'followers' in self.keys:
            self.data['followers'].append(int(artist_info['followers']['total']))
        if 'genres' in self.keys:
            genres = ', '.join(artist_info['genres'])
            self.data['genres'].append(str(genres))
        if 'first_release' in self.keys or 'last_release' in self.keys or 'num_releases' in self.keys or 'num_tracks' in self.keys:
            album_ids, release_dates, album_types, total_tracks = get_artist_release_dates(artist_info['id'])
            if(len(release_dates) > 0):
                if 'first_release' in self.keys:
                    self.data['first_release'].append(int(min(release_dates)))
                if 'last_release' in self.keys:
                    self.data['last_release'].append(int(max(release_dates)))
                if 'num_releases' in self.keys:
                    self.data['num_releases'].append(int(len(release_dates)))
                if 'num_tracks' in self.keys:
                    self.data['num_tracks'].append(int(np.sum(total_tracks)))
            else:
                if 'first_release' in self.keys:
                    self.data['first_release'].append(int(-1))
                if 'last_release' in self.keys:
                    self.data['last_release'].append(int(-1))
                if 'num_releases' in self.keys:
                    self.data['num_releases'].append(int(0))
                if 'num_tracks' in self.keys:
                    self.data['num_tracks'].append(int(0))

def get_spotify_artists_info(artist_ids):
    '''Get all artist information for a set of artist ids. This returns the whole
    Spotify info structure for that list of artist IDs, and includes a time-out
    condition in case we hit the daily rate limit.'''

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
    logger.info(f'Fetched artist information for {len(artists_info)} artists.')

    return artists_info

def generate_artist_info_dict(artists_info) -> ArtistInfoDict:
    '''Fill out all the artist information from the Spotify artist info structure,
    including information about artist releases, which must be calculated from
    the artist_albums endpoint, hence the batching and timeout.'''

    all_artist_info = ArtistInfoDict()
    try:
        logger.info(f'Fetching release dates and number of tracks for {len(artists_info)} artists, will timeout.')
        artists_info_batches = [artists_info[i:i + 50] for i in range(0, len(artists_info), 50)]
        signal.signal(signal.SIGALRM, handler)
        signal.alarm(len(artists_info)*(60 + 10)) # 1 min per batch + downtime
        i = 0
        for artist_info_batch in artists_info_batches:
            for artist_info in artist_info_batch:
                all_artist_info.append_artist_info(artist_info)
            logger.info(f'{i}: Fetched release dates and number of tracks for batch {len(artist_info_batch)} artists. Pausing for 10s...')
            i += 1
            time.sleep(10)
        signal.alarm(0)
    except TimeoutError as e:
        logger.critical(f"Operation timed out: {e}")
        sys.exit(1)
    all_artist_info.check_equal_length()
    logger.info(f'Finished fetching release dates and number of tracks for {len(all_artist_info["ids"])} artists.')

    return all_artist_info

def get_active_artists(artist_info_dict, strict=False) -> Dict[str, np.array]:
    '''Get artists from artist_info that fulfil both of the following:
    1. Have produced new music in the past five (two) years
    2. If they're more than two (zero) years old, have an average of
    two (four) tracks per year since their first release.

    args:
        artist_info_dict: dict or Pandas dataframe of artist information
        strict: bool, whether to use the stricter conditions
    returns:
        dict of active artist information
    '''

    required_keys = ['first_release', 'last_release', 'num_tracks', 'num_releases']
    if not all(key in artist_info_dict.keys() for key in required_keys):
        logger.critical(f"Missing required keys in artist_info: {required_keys}")
        sys.exit(1)

    if strict:
        num_years = 2
        num_tracks_per_year = 4
        first_release_cnd = 0 # start counting tracks from this many years before current
    else:
        num_years = 5
        num_tracks_per_year = 2
        first_release_cnd = 2

    cnd0 = np.array([num_releases > 0 for num_releases in artist_info_dict['num_releases']])
    active_artist_info = {key: np.array(artist_info_dict[key])[cnd0] for key in artist_info_dict.keys()}

    cnd1 = np.array([last_release > CURRENT_YEAR - num_years for last_release in active_artist_info['last_release']])
    active_artist_info = {key: np.array(active_artist_info[key])[cnd1] for key in active_artist_info.keys()}

    cnd2 = np.array([
        (CURRENT_YEAR - first_release < first_release_cnd) or
        (num_tracks > (CURRENT_YEAR - first_release)*num_tracks_per_year)
        for first_release, num_tracks
        in zip(active_artist_info['first_release'], active_artist_info['num_tracks'])
    ])
    active_artist_info = {key: np.array(active_artist_info[key])[cnd2] for key in active_artist_info.keys()}

    return active_artist_info

def get_new_active_artists(artist_info_dict) -> Dict[str, np.array]:
    '''Get artists from artist_info that fulfil both of the following:
    1. Have a first release date in the past five years
    2. Have an average of two tracks per year since their first release
    args:
        artist_info_dict: dict or Pandas dataframe of artist information
    returns:
        dict of new active artist information
    '''

    active_artists = get_active_artists(artist_info_dict)
    cnd_new = np.array(
        [first_release > CURRENT_YEAR - 5 for first_release in active_artists['first_release']]
    )
    new_active_artists = {key: np.array(active_artists[key])[cnd_new] for key in active_artists.keys()}

    return new_active_artists

def get_legacy_artists(artist_info_dict) -> Dict[str, np.array]:
    '''Get artists from artist_info that fulfil all of the following:
    1. Have not produced new music in the past five years
    2. Have had more than 2 lifetime releases
    3. Have two or more tracks per years between their first and last releases
    This returns artists that are no longer producing, but found success when they were.
    args:
        artist_info_dict: dict or Pandas dataframe of artist information
    returns:
        dict of legacy artist information
    '''

    required_keys = ['first_release', 'last_release', 'num_tracks', 'num_releases']
    if not all(key in artist_info_dict.keys() for key in required_keys):
        logger.critical(f"Missing required keys in artist_info: {required_keys}")
        sys.exit(1)

    cnd0 = np.array([num_releases > 0 for num_releases in artist_info_dict['num_releases']])
    legacy_artist_info = {key: np.array(artist_info_dict[key])[cnd0] for key in artist_info_dict.keys()}

    cnd1 = np.array([
        num_releases > 2 and last_release < CURRENT_YEAR - 5 for num_releases, last_release
        in zip(legacy_artist_info['num_releases'], legacy_artist_info['last_release'])
    ])
    legacy_artist_info = {key: np.array(legacy_artist_info[key])[cnd1] for key in legacy_artist_info.keys()}

    cnd2 = np.array([
        num_tracks >= 2.*(last_release - first_release) for first_release, last_release, num_tracks
        in zip(
            legacy_artist_info['first_release'],
            legacy_artist_info['last_release'],
            legacy_artist_info['num_tracks']
        )
    ])
    legacy_artist_info = {key: np.array(legacy_artist_info[key])[cnd2] for key in legacy_artist_info.keys()}

    return legacy_artist_info