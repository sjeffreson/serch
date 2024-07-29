import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.exceptions import SpotifyException

import numpy as np
from datetime import datetime

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
from typing import List, Dict, Tuple

import sys
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

client_credentials_manager = SpotifyClientCredentials()
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

def get_tracks_info(track_ids):
    '''Get all the track information for a set of track IDs. This returns the
    whole Spotify info structure for that list of track IDs, and includes a time-out
    condition in case we hit the daily rate limit.'''

    '''Maximum 50 tracks per request to sp.tracks (web docs say 100 but it's 50)'''
    try:
        tracks_info = []
        track_ids_batches = [track_ids[i:i + 50] for i in range(0, len(track_ids), 50)]
        signal.signal(signal.SIGALRM, handler)
        signal.alarm(len(track_ids_batches)*20) # 20s per batch
        for track_ids_batch in track_ids_batches:
            tracks_info_batch = sp.tracks(track_ids_batch)['tracks']
            tracks_info.extend(tracks_info_batch)
            if(len(track_ids_batches)>40): # more than 2000 tracks
                logger.info("Done with {:d} tracks out of {:d}, sleeping for 2 seconds".format(len(tracks_info), len(track_ids)))
                time.sleep(2)
        signal.alarm(0)
    except TimeoutError as e:
        logger.critical(f"Operation timed out: {e}")
        sys.exit(1)
    logger.info(f'Fetched track information for {len(tracks_info)} tracks.')

    return tracks_info

def get_tracks_audio_info(track_ids):
    '''Get all the track audio information for a set of track IDs. This returns the
    whole Spotify info structure for that list of track IDs, and includes a time-out
    condition in case we hit the daily rate limit.'''

    '''Maximum 50 tracks per request to sp.audio_features (web docs say 100 but it's 50)'''
    try:
        signal.signal(signal.SIGALRM, handler)
        tracks_audio_info = []
        track_ids_batches = [track_ids[i:i + 50] for i in range(0, len(track_ids), 50)]
        signal.alarm(len(track_ids_batches)*20) # 20s per batch
        for track_ids_batch in track_ids_batches:
            tracks_audio_info_batch = sp.audio_features(track_ids_batch)
            tracks_audio_info.extend(tracks_audio_info_batch)
            if(len(track_ids_batches)>40): # more than 2000 tracks
                logger.info("Done with {:d} tracks out of {:d}, sleeping for 2 seconds".format(len(tracks_audio_info), len(track_ids)))
                time.sleep(2)
        signal.alarm(0)
    except TimeoutError as e:
        logger.critical(f"Operation timed out: {e}")
        sys.exit(1)
    logger.info(f'Fetched track audio information for {len(tracks_audio_info)} tracks.')

    return tracks_audio_info

class TrackInfoDict:
    '''Data structure to store track information. Default keys are listed.
    The user can exclude keys by passing a list of keys to exclude_keys.'''
    def __init__(self, tracks_info=None, tracks_audio_info=None, exclude_keys=None):
        self.keys = [
            'ids',
            'names',
            'popularity',
            'markets',
            'artists',
            'release_date',
            'duration_ms',
            'acousticness',
            'danceability',
            'energy',
            'instrumentalness',
            'liveness',
            'loudness',
            'speechiness',
            'tempo',
            'valence',
            'musicalkey',
            'musicalmode',
            'time_signature',
        ]
        if exclude_keys:
            if 'ids' in exclude_keys:
                logger.critical("Cannot exclude 'ids' key.")
                sys.exit(1)
            self.keys = [key for key in self.keys if key not in exclude_keys]

        self.data = {key: [] for key in self.keys}

        '''Initial store'''
        if tracks_info and tracks_audio_info:
            self.append_track_info(tracks_info)
            self.append_track_audio_info(tracks_audio_info)
        else:
            logger.critical("Need to provide both tracks_info and tracks_audio_info to initialize TrackInfoDict.")

        '''Store the vmins and vmaxs for each quantity from the web API docs'''
        self.vmins = {
            'acousticness': 0.0,
            'danceability': 0.0,
            'energy': 0.0,
            'instrumentalness': 0.0,
            'liveness': 0.0,
            'loudness': -60.0,
            'speechiness': 0.0,
            'tempo': 0.0,
            'valence': 0.0,
            'musicalkey': 0,
            'musicalmode': 0,
            'time_signature': 0,
            'popularity': 0,
            'markets': 0,
            'days_since_release': 0,
            'monthly_listeners': 0,
            'duration_ms': 0,
        }
        self.vmaxs = {
            'acousticness': 1.0,
            'danceability': 1.0,
            'energy': 1.0,
            'instrumentalness': 1.0,
            'liveness': 1.0,
            'loudness': 0.0,
            'speechiness': 1.0,
            'tempo': 200.0,
            'valence': 1.0,
            'musicalkey': 11,
            'musicalmode': 1,
            'time_signature': 5,
            'popularity': 100,
            'markets': 180,
            'days_since_release': 60,
            'monthly_listeners': 1e8,
            'duration_ms': 50,
        }

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

    def append_track_info(self, tracks_info):
        if 'ids' in self.keys:
            self.data['ids'].extend([track['id'] for track in tracks_info])
        if 'names' in self.keys:
            self.data['names'].extend([track['name'] for track in tracks_info])
        if 'popularity' in self.keys:
            self.data['popularity'].extend([track['popularity'] for track in tracks_info])
        if 'markets' in self.keys:
            self.data['markets'].extend([len(track['available_markets']) for track in tracks_info])
        if 'artists' in self.keys:
            artists = [', '.join([artist['id'] for artist in track['artists']]) for track in tracks_info]
            self.data['artists'].extend(artists)
        if 'release_date' in self.keys:
            self.data['release_date'].extend([track['album']['release_date'] for track in tracks_info])
        if 'duration_ms' in self.keys:
            self.data['duration_ms'].extend([track['duration_ms'] for track in tracks_info])

    def append_track_audio_info(self, tracks_audio_info):
        if 'acousticness' in self.keys:
            self.data['acousticness'].extend([track['acousticness'] if track else None for track in tracks_audio_info])
        if 'danceability' in self.keys:
            self.data['danceability'].extend([track['danceability'] if track else None for track in tracks_audio_info])
        if 'energy' in self.keys:
            self.data['energy'].extend([track['energy'] if track else None for track in tracks_audio_info])
        if 'instrumentalness' in self.keys:
            self.data['instrumentalness'].extend([track['instrumentalness'] if track else None for track in tracks_audio_info])
        if 'liveness' in self.keys:
            self.data['liveness'].extend([track['liveness'] if track else None for track in tracks_audio_info])
        if 'loudness' in self.keys:
            self.data['loudness'].extend([track['loudness'] if track else None for track in tracks_audio_info])
        if 'speechiness' in self.keys:
            self.data['speechiness'].extend([track['speechiness'] if track else None for track in tracks_audio_info])
        if 'tempo' in self.keys:
            self.data['tempo'].extend([track['tempo'] if track else None for track in tracks_audio_info])
        if 'valence' in self.keys:
            self.data['valence'].extend([track['valence'] if track else None for track in tracks_audio_info])
        if 'musicalkey' in self.keys:
            self.data['musicalkey'].extend([track['key'] if track else None for track in tracks_audio_info])
        if 'musicalmode' in self.keys:
            self.data['musicalmode'].extend([track['mode'] if track else None for track in tracks_audio_info])
        if 'time_signature' in self.keys:
            self.data['time_signature'].extend([track['time_signature'] if track else None for track in tracks_audio_info])

if __name__ == "__main__":
    #track_id = '1vSxNj3PmaDryvAoxDCEHB'
    track_id = '4wjW3eJAl0fuMbm5gujz1B'
    #track_id = '718purgMpFb2Axhuz0Hbq1'
    track_info = sp.track(track_id)
    track_audio_info = sp.audio_features(track_id)[0]
    track_audio_analysis = sp.audio_analysis(track_id)['bars']
    # print all the properties
    #print(track_info)
    print(track_audio_analysis)
    print(track_audio_info)

