import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import numpy as np

from typing import List, Dict, Tuple
import time, os
from datetime import datetime
import pickle

from Monthly_Listeners_webscraper import scrape_monthly_listeners
import artist_info_helper as aih
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

client_credentials_manager = SpotifyClientCredentials()
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# TO DO: put these in a config file
DEFAULT_OUTPUT_DIR = "/n/holystore01/LABS/itc_lab/Users/sjeffreson/serch/artist-database/"
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
DEFAULT_DATAFRAME = "Playlist_names-IDs_{:s}.csv".format(CURRENT_DATE)
USER_ID = 'spotify'
TIMEOUT = 10*60

def get_artist_info(num_to_scrape: int=None) -> None:
    '''Get artist information for all artist IDs that are in artist_ids.csv but not
    in Spotify_artist_info.csv.'''

    artist_ids_df = pd.read_csv(DEFAULT_OUTPUT_DIR + "artist_ids.csv", usecols=["ids"])
    artist_ids_total = artist_ids_df["ids"].tolist()

    artist_ids_info_df = pd.read_csv(DEFAULT_OUTPUT_DIR + "Spotify_artist_info.csv", usecols=["ids"])
    artist_ids_info = artist_ids_info_df["ids"].tolist()

    artist_ids = [x for x in artist_ids_total if x not in artist_ids_info]
    if num_to_scrape is not None:
        artist_ids = artist_ids[:num_to_scrape]
    logger.info("Total number of artists to scrape: {:d}".format(len(artist_ids)))

    '''Retrieve full info structure for each artist, from Spotify'''
    artists_info = aih.get_spotify_artists_info(artist_ids)

    '''Generate all info in the ArtistInfoDict structure, which involves
    retrieving artist album information via the artist_albums endpoint.'''
    artist_info_dict = aih.generate_artist_info_dict(artists_info)

    '''Append to the Spotify_artist_info.csv file as a new row.'''
    artist_info_df = pd.DataFrame({key: artist_info_dict[key] for key in artist_info_dict.keys})
    artist_info_df.to_csv(DEFAULT_OUTPUT_DIR + "Spotify_artist_info.csv", mode='a', header=False, index=False)

def get_artist_monthly_listeners() -> None:
    '''Scrape monthly listeners for artist IDs that appear in Spotify_artist_info.csv
    but not in Spotify_artist_info_Mnth-Lstnrs.csv.'''

    artist_info_df = pd.read_csv(DEFAULT_OUTPUT_DIR + "Spotify_artist_info.csv")
    artist_ids = artist_info_df["ids"].tolist()

    artist_info_mnth_lstnrs_df = pd.read_csv(DEFAULT_OUTPUT_DIR + "Spotify_artist_info_Mnth-Lstnrs.csv", usecols=["ids"])
    artist_ids_mnth_lstnrs = artist_info_mnth_lstnrs_df["ids"].tolist()

    artist_ids = [x for x in artist_ids if x not in artist_ids_mnth_lstnrs]
    logger.info("Total number of artists to scrape: {:d}".format(len(artist_ids)))

    artist_monthly_listeners = []
    i = 0
    for artist_id in artist_ids:
        artist_monthly_listeners.append(scrape_monthly_listeners(artist_id, "temp"))
        i += 1
        if i % 100 == 0:
            artist_monthly_listeners_print = [x for x in artist_monthly_listeners if x is not None]
            logger.info("Scraped monthly listeners for {:d} artists. Current average is {:f}.".format(i, sum(artist_monthly_listeners_print)/i))

    '''Append to the Spotify_artist_info_Mnth-Lstnrs.csv file, along with all the info
    from Spotify_artist_info.csv for those IDs.'''
    artist_info_df = pd.read_csv(DEFAULT_OUTPUT_DIR + "Spotify_artist_info.csv")
    artist_info_df = artist_info_df[artist_info_df["ids"].isin(artist_ids)]
    artist_info_df["monthly_listeners"] = artist_monthly_listeners
    artist_info_df.to_csv(DEFAULT_OUTPUT_DIR + "Spotify_artist_info_Mnth-Lstnrs.csv", mode='a', header=False, index=False)

def get_artist_random_track_ids(num_to_scrape: int=None) -> None:
    '''Scrape monthly listeners for artist IDs that appear in Spotify_artist_info_Mnth-Lstnrs.csv
    but not in Spotify_artist_info_Random-Track-IDs.csv. If num_to_scrape is None, scrape all.'''

    artist_info_df = pd.read_csv(DEFAULT_OUTPUT_DIR + "Spotify_artist_info_Mnth-Lstnrs.csv")
    artist_ids = artist_info_df["ids"].tolist()

    try:
        artist_info_rand_tracks_df = pd.read_csv(DEFAULT_OUTPUT_DIR + "Spotify_artist_info_Random-Track-IDs.csv", usecols=["ids"])
        artist_ids_rand_tracks = artist_info_rand_tracks_df["ids"].tolist()
    except FileNotFoundError:
        artist_ids_rand_tracks = []

    artist_ids = [x for x in artist_ids if x not in artist_ids_rand_tracks]
    if num_to_scrape is not None:
        artist_ids = artist_ids[:num_to_scrape]
    logger.info("Total number of artist tracks to retrieve: {:d}".format(len(artist_ids)))

    artist_random_tracks = []
    i = 0
    for artist_id in artist_ids:
        artist_random_tracks.append(aih.get_artist_random_track_id(artist_id))
        i += 1
        if i % 100 == 0:
            artist_random_tracks_print = [x for x in artist_random_tracks if x is not None]
            logger.info("Pulled random tracks for {:d} artists.".format(i))

    '''Append the artist IDs and track IDs to Spotify_artist_info_Random-Track-IDs file.'''
    artist_info_to_add_df = pd.DataFrame({"ids": artist_ids, "track_ids": artist_random_tracks})
    artist_info_to_add_df.to_csv(DEFAULT_OUTPUT_DIR + "Spotify_artist_info_Random-Track-IDs.csv", mode='a', header=False, index=False)

def generate_tracks_for_artists(num_to_scrape: int=None) -> None:
    '''Generate track info for a number of artists by reading in the track IDs for those artists
    and then retrieving the full track info structure for each track ID from Spotify. If num_to_scrape
    is None, scrape all.'''

    tracks_info_file = "Spotify_artist_info_tracks.csv"
    tracks_id_file = "Spotify_artist_info_Random-Track-IDs.csv"

    '''Generate track info, by reading in the track IDs, selecting the ones to scrape,
    and then retrieving the full track info structure for each track ID.'''
    track_ids_df = pd.read_csv(DEFAULT_OUTPUT_DIR + tracks_id_file, usecols=["track_ids"])
    track_ids = track_ids_df["track_ids"].tolist()
    track_ids = [x for x in track_ids if x == x]
    if(len(track_ids) != len(list(set(track_ids)))): # should be no duplicates
        logger.error("Duplicates in track IDs.")
        return
    '''Remove tracks already scraped'''
    try:
        track_info_df = pd.read_csv(DEFAULT_OUTPUT_DIR + tracks_info_file)
        track_ids_scraped = track_info_df["ids"].tolist()
    except FileNotFoundError:
        track_ids_scraped = []
    track_ids = [x for x in track_ids if x not in track_ids_scraped]
    if num_to_scrape is not None:
        track_ids = track_ids[:num_to_scrape]
    
    '''Retrieve full info structure for each track, from Spotify'''
    tracks_info = tih.get_tracks_info(track_ids)
    tracks_audio_info = tih.get_tracks_audio_info(track_ids)
    track_info_dict = tih.TrackInfoDict(tracks_info, tracks_audio_info)

    '''Append to the Spotify_track_info.csv file as a new row.'''
    track_info_df = pd.DataFrame({key: track_info_dict[key] for key in track_info_dict.keys})
    track_info_df.to_csv(DEFAULT_OUTPUT_DIR + tracks_info_file, mode='a', index=False, header=False)

if __name__ == "__main__":
    #get_artist_info(num_to_scrape=2000)
    get_artist_monthly_listeners()
    #get_artist_random_track_ids(num_to_scrape=1000)
    #generate_tracks_for_artists()