import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import numpy as np

from typing import List, Dict, Tuple
import time, os
from datetime import datetime
import pickle, json

from Webscrapers import scrape_monthly_listeners, scrape_genres_from_bio
import artist_info_helper as aih
import track_info_helper as tih

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
ARTIST_IDS_FILE = config['filenames']['artist_ids']
ARTIST_FILE = config['filenames']['artist_info']
ARTIST_MNTH_LSTNRS_FILE = config['filenames']['artist_info_mnth_lstnrs']
CLEAN_MNTH_LSTNRS_FILE = config['filenames']['clean_artist_info_mnth_lstnrs']
BIO_GENRES = config['filenames']['bio_genres']
MISSING_BIO_GENRES = config['filenames']['missing_bio_genres']
RAND_TRACK_IDS_FILE = config['filenames']['rand_track_ids']
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
USER_ID = 'spotify'
TIMEOUT = 10*60

def get_artist_info(num_to_scrape: int) -> None:
    '''Get artist information for all artist IDs that are in artist_ids.csv but not
    in Spotify_artist_info.csv. If num_to_scrape is None, scrape all that have not
    yet been scraped.'''

    artist_ids_df = pd.read_csv(OUTPUT_DIR + ARTIST_IDS_FILE, usecols=["ids"])
    artist_ids_total = artist_ids_df["ids"].tolist()

    artist_ids_info_df = pd.read_csv(OUTPUT_DIR + ARTIST_FILE, usecols=["ids"])
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
    artist_info_df.to_csv(OUTPUT_DIR + ARTIST_FILE, mode='a', header=False, index=False)

def get_artist_monthly_listeners() -> None:
    '''Scrape monthly listeners for artist IDs that appear in Spotify_artist_info.csv
    but not in Spotify_artist_info_Mnth-Lstnrs.csv.'''

    artist_info_df = pd.read_csv(OUTPUT_DIR + ARTIST_FILE)
    artist_ids = artist_info_df["ids"].tolist()

    artist_info_mnth_lstnrs_df = pd.read_csv(OUTPUT_DIR + ARTIST_MNTH_LSTNRS_FILE, usecols=["ids"])
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
    artist_info_df = pd.read_csv(OUTPUT_DIR + ARTIST_FILE)
    artist_info_df = artist_info_df[artist_info_df["ids"].isin(artist_ids)]
    artist_info_df["monthly_listeners"] = artist_monthly_listeners
    artist_info_df.to_csv(OUTPUT_DIR + ARTIST_MNTH_LSTNRS_FILE, mode='a', header=False, index=False)

def clean_artist_info_mnth_lstnrs(req_features: List[str]) -> None:
    '''Clean the Spotify_artist_info_Mnth-Lstnrs.csv file, and save it to a temporary pickle.
    Cleaning involves removing rows that have -1 or NaN values in any of the specified required
    features.'''

    artist_info_df = pd.read_csv(OUTPUT_DIR + ARTIST_MNTH_LSTNRS_FILE)

    for_logging = artist_info_df.dropna(subset=[column for column in artist_info_df.columns if column in req_features])
    for_logging = for_logging.drop(for_logging[(for_logging[req_features] == -1).any(axis=1)].index)
    logger.info("Number of artists with no genre, before bio-scraping: {:d}".format(len(for_logging)))

    '''First add the genres scraped from the Spotify bios'''
    if 'genres' in req_features:
        bio_genres_df = pd.read_csv(OUTPUT_DIR + BIO_GENRES)
        artist_info_df = artist_info_df.set_index('ids')
        bio_genres_df = bio_genres_df.set_index('ids')
        artist_info_df['genres'] = artist_info_df['genres'].combine_first(bio_genres_df['genres'])
        artist_info_df = artist_info_df.reset_index()

    '''Clean any NaN or -1 values in the required features that are still missing'''
    artist_info_df = artist_info_df.dropna(subset=[column for column in artist_info_df.columns if column in req_features])
    artist_info_df = artist_info_df.drop(artist_info_df[(artist_info_df[req_features] == -1).any(axis=1)].index)
    logger.info("Number of artists after cleaning, with bio-scraping: {:d}".format(len(artist_info_df)))

    '''Save to a new csv file with a date-stamp for cleaning time'''
    artist_info_df.to_csv(OUTPUT_DIR + CLEAN_MNTH_LSTNRS_FILE, index=False)
    logger.info("Saved cleaned artist info to {:s}.".format(CLEAN_MNTH_LSTNRS_FILE))

def get_artist_random_track_ids(num_to_scrape: int=None) -> None:
    '''Scrape monthly listeners for artist IDs that appear in CLEANED_Spotify_artist_info_Mnth-Lstnrs.csv
    but not in Spotify_artist_info_Random-Track-IDs.csv. If num_to_scrape is None, scrape all.'''

    try:
        artist_info_df = pd.read_csv(OUTPUT_DIR + CLEAN_MNTH_LSTNRS_FILE)
        artist_ids = artist_info_df["ids"].tolist()
        artist_names = artist_info_df["names"].tolist()
    except FileNotFoundError:
        logger.error("Cleaned artist info file not found.")
        return

    try:
        artist_info_rand_tracks_df = pd.read_csv(OUTPUT_DIR + RAND_TRACK_IDS_FILE, usecols=["ids"])
        artist_ids_rand_tracks = artist_info_rand_tracks_df["ids"].tolist()
    except FileNotFoundError:
        artist_ids_rand_tracks = []

    artist_names = [x for _, x in zip(artist_ids, artist_names) if _ not in artist_ids_rand_tracks]
    artist_ids = [x for x in artist_ids if x not in artist_ids_rand_tracks]
    if num_to_scrape is not None:
        artist_ids = artist_ids[:num_to_scrape]
        artist_names = artist_names[:num_to_scrape]
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
    artist_info_to_add_df = pd.DataFrame({"ids": artist_ids, "names": artist_names, "track_ids": artist_random_tracks})
    if not os.path.exists(OUTPUT_DIR + RAND_TRACK_IDS_FILE):
        artist_info_to_add_df.to_csv(OUTPUT_DIR + RAND_TRACK_IDS_FILE, index=False)
    else:
        artist_info_to_add_df.to_csv(OUTPUT_DIR + RAND_TRACK_IDS_FILE, mode='a', header=False, index=False)

def generate_track_info_for_artists(num_to_scrape: int=None) -> None:
    '''Generate track info for a number of artists by reading in the track IDs for those artists
    and then retrieving the full track info structure for each track ID from Spotify. If num_to_scrape
    is None, scrape all.'''

    tracks_info_file = "Spotify_artist_info_tracks.csv"
    tracks_id_file = "Spotify_artist_info_Random-Track-IDs.csv"

    '''Generate track info, by reading in the track IDs, selecting the ones to scrape,
    and then retrieving the full track info structure for each track ID.'''
    track_ids_df = pd.read_csv(OUTPUT_DIR + tracks_id_file, usecols=["track_ids"])
    track_ids = track_ids_df["track_ids"].tolist()
    track_ids = [x for x in track_ids if x == x]
    if(len(track_ids) != len(list(set(track_ids)))): # should be no duplicates
        logger.error("Duplicates in track IDs.")
        return
    '''Remove tracks already scraped'''
    try:
        track_info_df = pd.read_csv(OUTPUT_DIR + tracks_info_file)
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
    track_info_df.to_csv(OUTPUT_DIR + tracks_info_file, mode='a', index=False, header=False)

def generate_artist_genres_from_bio(num_to_scrape: int=None) -> None:
    genres_to_search = config["genres"]

    artist_info_df = pd.read_csv(OUTPUT_DIR + ARTIST_MNTH_LSTNRS_FILE, usecols=["ids", "names", "genres"])
    artist_ids = artist_info_df[artist_info_df["genres"].isnull()]["ids"].tolist()
    logger.info(f"Number of artists with no genres: {len(artist_ids)}")

    '''take only those that don't appear in BIO_GENRES or MISSING_BIO_GENRES files'''
    if os.path.exists(OUTPUT_DIR + BIO_GENRES):
        bio_genres_df = pd.read_csv(OUTPUT_DIR + BIO_GENRES, usecols=["ids"])
        bio_genres_ids = bio_genres_df["ids"].tolist()
        artist_ids = [x for x in artist_ids if x not in bio_genres_ids]
    if os.path.exists(OUTPUT_DIR + MISSING_BIO_GENRES):
        missing_bio_genres_df = pd.read_csv(OUTPUT_DIR + MISSING_BIO_GENRES, usecols=["ids"])
        missing_bio_genres_ids = missing_bio_genres_df["ids"].tolist()
        artist_ids = [x for x in artist_ids if x not in missing_bio_genres_ids]
    logger.info(f"Total number of artists to scrape: {len(artist_ids)}")

    if num_to_scrape is not None:
        artist_ids = artist_ids[:num_to_scrape]
    logger.info(f"Scraping number of artists: {len(artist_ids)}")

    '''Divide artist IDs into batches of 100'''
    artist_ids_batches = [artist_ids[i:i + 100] for i in range(0, len(artist_ids), 100)]
    for i, artist_ids_batch in enumerate(artist_ids_batches):
        artist_ids_genres, artist_genres, artist_names, missing_ids_genres = [], [], [], []
        for j, artist_id in enumerate(artist_ids_batch):
            artist_name = artist_info_df[artist_info_df["ids"] == artist_id]["names"].values[0]
            genres = scrape_genres_from_bio(artist_id, artist_name, genres_to_search)
            if genres:
                genres = ','.join(genres)
                artist_ids_genres.append(artist_id)
                artist_genres.append(genres)
                artist_names.append(artist_name)
                logger.info(f"{i*100 + j}: Genres {genres} found for: {artist_name}, {artist_id}.")
            else:
                missing_ids_genres.append(artist_id)
                logger.info(f"{i*100 + j}: Genres not found for artist id: {artist_id} and artist name: {artist_name}, {artist_id}.")
    
        artist_info_genres_df = pd.DataFrame({"ids": artist_ids_genres, "names": artist_names, "genres": artist_genres})
        if os.path.exists(OUTPUT_DIR + BIO_GENRES):
            artist_info_genres_df.to_csv(OUTPUT_DIR + BIO_GENRES, mode='a', header=False, index=False)
            logger.info(f'{i*100 + j}: Appended {len(artist_info_genres_df)} artist genres to existing file: {OUTPUT_DIR + BIO_GENRES}')
        else:
            artist_info_genres_df.to_csv(OUTPUT_DIR + BIO_GENRES, index=False)
            logger.info(f'Saved {len(artist_info_genres_df)} artist genres to new file: {OUTPUT_DIR + BIO_GENRES}')

        missing_names_df = pd.DataFrame({"ids": missing_ids_genres})
        if os.path.exists(OUTPUT_DIR + MISSING_BIO_GENRES):
            missing_names_df.to_csv(OUTPUT_DIR + MISSING_BIO_GENRES, mode='a', header=False, index=False)
            logger.info(f'{i*100 + j}: Appended {len(missing_names_df)} missing artist genres to existing file: {OUTPUT_DIR + MISSING_BIO_GENRES}')
        else:
            missing_names_df.to_csv(OUTPUT_DIR + MISSING_BIO_GENRES, index=False)
            logger.info(f'{i*100 + j}: Saved {len(missing_names_df)} missing artist genres to new file: {OUTPUT_DIR + MISSING_BIO_GENRES}')
        logger.info(f"{i*100 + j}: Sleeping for 10 seconds.")
        time.sleep(10)

if __name__ == "__main__":
    pass