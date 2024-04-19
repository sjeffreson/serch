import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

import pandas as pd
import numpy as np

from typing import List, Dict, Tuple
import time, os, sys
from datetime import datetime
import pickle

import track_info_helper as tih

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

'''
Just some basic functions to quickly retrieve the popularity of artists in various
Spotify and custom playlists.

Remember environment variables:
export SPOTIPY_CLIENT_ID='your-spotify-client-id'
export SPOTIPY_CLIENT_SECRET='your-spotify-client-secret'
export SPOTIPY_REDIRECT_URI='your-app-redirect-url'
'''

# TO DO: put these in a config file
DEFAULT_OUTPUT_DIR = "/n/holystore01/LABS/itc_lab/Users/sjeffreson/serch/artist-database/Editorial-playlists/"

client_credentials_manager = SpotifyClientCredentials()
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

def generate_track_info_for_date(date: str) -> None:
    '''Generate track info for a given date, by reading in the track IDs for that date
    and then retrieving the full track info structure for each track ID from Spotify.'''

    tracks_info_file = "track_info_last_24hrs_{:s}.csv".format(date)
    tracks_id_file = "track_ids_last_24hrs_{:s}.csv".format(date)

    '''First check if track info already processed and in a csv file, if so, return.'''
    if os.path.exists(DEFAULT_OUTPUT_DIR + tracks_info_file):
        logger.info("Track info already processed for date {:s}.".format(date))
        return

    '''Generate track info for a given date, by reading in the track IDs for that date
    and then retrieving the full track info structure for each track ID.'''
    track_ids_df = pd.read_csv(DEFAULT_OUTPUT_DIR + tracks_id_file, usecols=["track_ids"])
    track_ids = track_ids_df["track_ids"].tolist()
    track_ids = list(set(track_ids)) # remove duplicates
    
    '''Retrieve full info structure for each track, from Spotify'''
    tracks_info = tih.get_tracks_info(track_ids)
    tracks_audio_info = tih.get_tracks_audio_info(track_ids)
    track_info_dict = tih.TrackInfoDict(tracks_info, tracks_audio_info)

    '''Append to the Spotify_track_info.csv file as a new row.'''
    track_info_df = pd.DataFrame({key: track_info_dict[key] for key in track_info_dict.keys})
    track_info_df.to_csv(DEFAULT_OUTPUT_DIR + tracks_info_file, mode='a', index=False)

def generate_track_info_all_dates() -> None:
    '''Strip all dates present in folder from their names. Then process each date in turn.'''

    dates = [f.split("track_ids_last_24hrs_")[-1].split(".csv")[0] for f in os.listdir(DEFAULT_OUTPUT_DIR) if "track_ids_last_24hrs_" in f]
    
    for date in dates:
        logger.info("Processing date {:s}.".format(date))
        generate_track_info_for_date(date)

if __name__ == "__main__":
    generate_track_info_all_dates()