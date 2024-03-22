import spotipy
from spotipy.oauth2 import SpotifyOAuth
from spotipy.exceptions import SpotifyException

import numpy as np

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
from typing import List, Dict, Tuple

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

    def append_missing_info(self, missing_name):
        if 'ids' in self.keys:
            self.data['ids'].append('missing')
        if 'names' in self.keys:
            self.data['names'].append(str(missing_name).lower())
        if 'popularity' in self.keys:
            self.data['popularity'].append(-1)
        if 'followers' in self.keys:
            self.data['followers'].append(-1)
        if 'genres' in self.keys:
            self.data['genres'].append('missing')
        if 'first_release' in self.keys:
            self.data['first_release'].append(-1)
        if 'last_release' in self.keys:
            self.data['last_release'].append(-1)
        if 'num_releases' in self.keys:
            self.data['num_releases'].append(-1)
        if 'num_tracks' in self.keys:
            self.data['num_tracks'].append(-1)