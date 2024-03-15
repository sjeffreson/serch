import spotipy
from spotipy.oauth2 import SpotifyOAuth
from spotipy.exceptions import SpotifyException

import argparse, logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import pickle
import sys, os, glob, re
regex = re.compile(r"\d+")
import random
import numpy as np
from typing import List, Dict, Tuple

'''
Remember environment variables:
export SPOTIPY_CLIENT_ID='your-spotify-client-id'
export SPOTIPY_CLIENT_SECRET='your-spotify-client-secret'
export SPOTIPY_REDIRECT_URI='your-app-redirect-url'
'''

scope = "playlist-read-private playlist-read-collaborative"
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))

DEFAULT_OUTPUT_DIR = "/n/holystore01/LABS/itc_lab/Users/sjeffreson/serch/artist-database/"

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

        '''Load the artist names from the MusicBrainz database, up to the maximum number'''
        artist_names = []
        filenames = glob.glob(DEFAULT_OUTPUT_DIR + "/artist_names_*.pkl")
        filenos = [int(filename.split('_')[-1].split('.')[0]) for filename in filenames]
        filenames = [elem for _, elem in sorted(zip(filenos, filenames))]

        for filename in filenames:
            with open(filename, "rb") as f:
                artist_names.extend(pickle.load(f))

        '''Cleaning duplicates and empty strings'''
        artist_names = self.remove_duplicates_preserve_order(artist_names)
        artist_names = [name for name in artist_names if name is not None]
        artist_names = [name for name in artist_names if len(name) > 0]
        '''remove names with brackets, these are indicated as not artists by MusicBrainz'''
        artist_names = [name for name in artist_names if '[' not in name and ']' not in name]

        self.artist_names = artist_names

        if rand_num_artist_names is not None:
            random.seed(self.random_seed)
            self.artist_names = random.sample(self.artist_names, self.rand_num_artist_names)

        self.artist_info = self.get_artist_info()

    def remove_duplicates_preserve_order(self, seq):
        seen = set()
        return [x for x in seq if not (x in seen or seen.add(x))]

    def get_artist_spotify_ids(self, query) -> Dict[str, str]:
        '''Fetch artist ids from Spotify. Max search query string length is 100 characters.'''

        if len(query.split(' OR ')) > 50:
            raise ValueError('Query length is too long. Max length is 50 artists.')

        results = sp.search(q=query, type='artist', limit=50)
        artists = results['artists']['items']

        # print total number of results
        print(f"Total number of results: {results['artists']['total']}")

        '''check for exact matches'''
        exact_matches = []
        for artist in artists:
            if artist['name'].lower() in query.lower():
                exact_matches.append(artist)

        if len(exact_matches) == 0:
            return None
        else:
            artist_ids = [artist['id'] for artist in exact_matches]
            return artist_ids

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

        '''Break artist names into queries of max length 200 characters'''
        queries = []
        while len(self.artist_names) > 0:
            query = 'artists:'
            last_name = self.artist_names[0]
            while len(query) <= 200 and len(self.artist_names) > 0:
                last_name = self.artist_names[0]
                query += f'"{self.artist_names.pop(0)}"' + ' OR '
            query = query[:-4] # remove last ' OR '
            if len(query) > 200:
                self.artist_names.insert(0, last_name)
                query = query[:-len(last_name)-4]
            print(type(query), query, len(query))
            queries.append(query)

        artist_ids = []
        for query in queries:
            spot_ids = self.get_artist_spotify_ids(query)
            if spot_ids is not None:
                artist_ids.extend(spot_ids)
        print(artist_ids, len(artist_ids))

        # '''Maximum 50 artists per request to sp.artists'''
        # artists_info = []
        # artist_ids_batches = [artist_ids[i:i + 50] for i in range(0, len(artist_ids), 50)]
        # for artist_ids_batch in artist_ids_batches:
        #     artists_info_batch = sp.artists(artist_ids_batch)['artists']
        #     artists_info.extend(artists_info_batch)

        # '''Store all information in the dictionary'''
        # for artist_info in artists_info:
        #     all_artist_info['ids'].append(artist_info['id'])
        #     all_artist_info['names'].append(artist_info['name'])
        #     all_artist_info['popularity'].append(artist_info['popularity'])
        #     all_artist_info['followers'].append(artist_info['followers']['total'])
        #     genres = ', '.join(artist_info['genres'])
        #     all_artist_info['genres'].append(genres)
            # album_ids, artist_release_dates = self.get_artist_release_dates(artist_info['id'])
            # # if(len(artist_release_dates) == 0):
            # #     all_artist_info['first_release'].append(None)
            # #     all_artist_info['last_release'].append(None)
            # #     all_artist_info['num_releases'].append(0)
            # #     all_artist_info['num_tracks'].append(0)
            # # else:
            # #     all_artist_info['first_release'].append(min(artist_release_dates))
            # #     all_artist_info['last_release'].append(max(artist_release_dates))
            # #     all_artist_info['num_releases'].append(len(artist_release_dates))
            # #     if min(artist_release_dates) > self.current_year - 10:
            # #         all_artist_info['num_tracks'].append(self.get_artist_num_tracks(album_ids))
            # #     else:
            # #         all_artist_info['num_tracks'].append(None)

        return all_artist_info

    def save_artist_info(self, filename: str):
        '''Save the artist information to disk.'''
        with open(filename, 'wb') as f:
            pickle.dump(self.artist_info, DEFAULT_INPUT_DIR + f)

    def clean_unproductive_artists(self) -> Dict[str, np.array]:
        '''Remove artists that fulfil either of the following:
        1. Have two or fewer albums/EPs in total AND have not produced new music in
           the past five years (never found success, broke up).
        2. If they're more than 2 and less than 10 years old, have a total number of
           tracks that's smaller than the number of years since their first release (hobbyists).
        Note that this leaves legacy artists who were famous but have retired.'''

        cnd0 = np.array([num_releases > 0 for num_releases in self.artist_info['num_releases']])
        artist_info_cut = {key: np.array(self.artist_info[key])[cnd0] for key in self.artist_info.keys()}

        cnd1 = np.array([
            num_releases > 2 or last_release > self.current_year - 5
            for num_releases, last_release
            in zip(artist_info_cut['num_releases'], artist_info_cut['last_release'])
        ])
        artist_info_cut = {key: np.array(artist_info_cut[key])[cnd1] for key in artist_info_cut.keys()}

        cnd2 = np.array([
            self.current_year - first_release > 10 or self.current_year - first_release < 2 or num_tracks > self.current_year - first_release
            for first_release, num_tracks
            in zip(artist_info_cut['first_release'], artist_info_cut['num_tracks'])
        ])
        artist_info_cut = {key: np.array(artist_info_cut[key])[cnd2] for key in artist_info_cut.keys()}

        self.artist_info = artist_info_cut

    def get_active_artists(self) -> Dict[str, np.array]:
        '''Get artists that fulfil both of the following:
        1. Have produced new music in the past five years
        2. If they're more than 2 and less than 10 years old, have a total number of
           tracks that's larger than the number of years since their first release.
        This does not include legacy artists who were famous but have retired.
        '''

        cnd0 = np.array([num_releases > 0 for num_releases in self.artist_info['num_releases']])
        active_artist_info = {key: np.array(self.artist_info[key])[cnd0] for key in self.artist_info.keys()}

        cnd1 = np.array([last_release > self.current_year - 5 for last_release in active_artist_info['last_release']])
        active_artist_info = {key: np.array(active_artist_info[key])[cnd1] for key in active_artist_info.keys()}

        cnd2 = np.array([
            (self.current_year - first_release > 10) or
            (self.current_year - first_release < 2) or
            (num_tracks > self.current_year - first_release)
            for first_release, num_tracks
            in zip(active_artist_info['first_release'], active_artist_info['num_tracks'])
        ])
        active_artist_info = {key: np.array(active_artist_info[key])[cnd2] for key in active_artist_info.keys()}

        return active_artist_info

    def get_legacy_artists(self) -> Dict[str, np.array]:
        '''Get the artists that have had more than 2 lifetime releases, but have not
        produced new music in the past five years.'''

        cnd0 = np.array([num_releases > 0 for num_releases in self.artist_info['num_releases']])
        legacy_artist_info = {key: np.array(self.artist_info[key])[cnd0] for key in self.artist_info.keys()}

        cnd1 = np.array([
            num_releases > 2 and last_release < self.current_year - 5 for num_releases, last_release
            in zip(legacy_artist_info['num_releases'], legacy_artist_info['last_release'])
        ])
        legacy_artist_info = {key: np.array(legacy_artist_info[key])[cnd1] for key in legacy_artist_info.keys()}

        return legacy_artist_info

    def get_famous_legacy_artists(self) -> Dict[str, np.array]:
        '''Get the artists that have had more than 2 lifetime releases, have not
        produced new music in the past five years, and have a Spotify popularity
        score of at least 50.'''

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