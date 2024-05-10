import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

import pandas as pd
import numpy as np

from typing import List, Dict, Tuple
import time, os
from datetime import datetime
import pickle, json

from Webscrapers import scrape_monthly_listeners
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

with open('config.json') as f:
    config = json.load(f)
OUTPUT_DIR = config['paths']['editorial_output_dir']
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
DEFAULT_DATAFRAME = "Playlist_names-IDs_{:s}.csv".format(CURRENT_DATE)
USER_ID = 'spotify'
TIMEOUT = 10*60

client_credentials_manager = SpotifyClientCredentials()
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

def store_available_spotify_playlists() -> None:
    '''Store Spotify public playlists that are directly available via the user_playlists
    endpoint with the USER_ID "spotify". This is limited by market, but is a good starting
    point.'''

    playlists = sp.user_playlists(USER_ID, limit=50)

    '''Continue fetching until no more results left'''
    playlist_names, playlist_ids, playlist_owners, playlist_public = [], [], [], []
    while playlists['next']:
        for playlist in playlists['items']:
            if playlist['owner']['id'] != USER_ID or not playlist['public']:
                continue
            playlist_names.append(playlist['name'])
            playlist_ids.append(playlist['id'])
            playlist_owners.append(playlist['owner']['id'])
            playlist_public.append(playlist['public'])
        playlists = sp.next(playlists)
        logger.info("Saved batch of {:d} playlists.".format(len(playlist_names)))

    playlist_ids, unique_indices = np.unique(playlist_ids, return_index=True)
    logger.info("Total number of unique playlists: {:d}".format(len(playlist_ids)))
    playlist_df = pd.DataFrame({
        'playlist_name': [playlist_names[i] for i in unique_indices],
        'playlist_id': playlist_ids,
        'playlist_owner': [playlist_owners[i] for i in unique_indices],
        'public': [playlist_public[i] for i in unique_indices]
    })
    playlist_df.to_csv(OUTPUT_DIR + DEFAULT_DATAFRAME, index=False)

def get_category_ids_for_market(market: str) -> Tuple[List[str], List[str]]:
    '''Get all available categories for a particular market.'''

    categories = sp.categories(country=market, limit=50)
    category_ids, category_names = [], []

    while categories['categories']['next']:
        for category in categories['categories']['items']:
            category_ids.append(category['id'])
            category_names.append(category['name'])
        categories = sp.next(categories['categories'])
        logger.info("Saved batch of {:d} categories.".format(len(category_ids)))

    return category_ids, category_names

def get_category_playlists_for_market(market: str, category_id: str) -> Tuple[List[str], List[str]]:
    '''Get all available playlists for a particular category in a particular market.'''

    playlists = sp.category_playlists(category_id=category_id, country=market)
    playlists_fullinfo = []
    while playlists['playlists']['next']:
        for playlist in playlists['playlists']['items']:
            if playlist['owner']['id'] != USER_ID or not playlist['public']:
                continue
            playlists_fullinfo.append(playlist)
        playlists = sp.next(playlists['playlists'])
        logger.info("Saved batch of {:d} playlists.".format(len(playlists_fullinfo)))

    return playlists_fullinfo

def store_spotify_playlist_selection_for_market(market: str) -> None:
    '''Store Spotify public playlists that are available via browsing categories in
    a particular market. This is also not comprehensive because the results are just a subset
    of the total and are ordered by popularity and other metrics. However it seems
    to be the only way to browse markets now that the market is not an input parameter
    for the user_playlists endpoint.'''

    logger.info("Entering Market: {:s}".format(market))
    category_ids, category_names = get_category_ids_for_market(market)

    playlist_names, playlist_ids, playlist_owners, playlist_public = [], [], [], []
    playlist_category_ids, playlist_category_names = [], []
    for category_id, category_name in zip(category_ids, category_names):
        logger.info("Entering Category: {:s}".format(category_name))
        playlists_fullinfo = get_category_playlists_for_market(market, category_id)
        for playlist in playlists_fullinfo:
            if playlist['owner']['id'] != USER_ID or not playlist['public']: # just to be sure
                continue
            playlist_names.append(playlist['name'])
            playlist_ids.append(playlist['id'])
            playlist_owners.append(playlist['owner']['id'])
            playlist_public.append(playlist['public'])
            playlist_category_ids.append(category_id)
            playlist_category_names.append(category_name)

    playlist_ids, unique_indices = np.unique(playlist_ids, return_index=True)
    logger.info("Total number of unique playlists: {:d}".format(len(playlist_ids)))
    playlist_df = pd.DataFrame({
        'playlist_name': [playlist_names[i] for i in unique_indices],
        'playlist_id': playlist_ids,
        'playlist_owner': [playlist_owners[i] for i in unique_indices],
        'public': [playlist_public[i] for i in unique_indices],
        'category_id': [playlist_category_ids[i] for i in unique_indices],
        'category_name': [playlist_category_names[i] for i in unique_indices]
    })
    filename = OUTPUT_DIR + DEFAULT_DATAFRAME.split(".csv")[0] + \
        "_{:s}".format(market) + ".csv"
    playlist_df.to_csv(filename, index=False)
    logger.info("Saved playlists for market: {:s} to {:s}.".format(market, filename))

def find_playlists_across_markets() -> None:
    '''Store Spotify public playlists that are available via browsing categories and
    markets. This is also not comprehensive because the results are just a subset
    of the total and are ordered by popularity and other metrics. However it seems
    to be the only way to browse markets now that the market is not an input parameter
    for the user_playlists endpoint.'''

    markets = sp.available_markets()
    for market in markets['markets']:
        store_spotify_playlist_selection_for_market(market)
        logger.info("Finished Market: {:s}. Pausing for 30s...".format(market))
        time.sleep(TIMEOUT)

def get_artists_last_24hrs(playlist_id: str) -> Tuple[List[str], List[str]]:
    '''Get the tracks added to a playlist in the last 24 hours and store
    the IDs of their artists in a temporary pickle.'''

    playlist = sp.playlist(playlist_id)
    tracks = playlist['tracks']
    artist_ids, track_ids = [], []
    for track in tracks['items']:
        added_at = track['added_at']
        added_at = datetime.strptime(added_at, "%Y-%m-%dT%H:%M:%SZ")
        if (datetime.now() - added_at).days < 1:
            try:
                for artist in track['track']['artists']:
                    artist_ids.append(artist['id'])
                    track_ids.append(track['track']['id'])
            except TypeError or KeyError:
                logger.info("No artist information available for that track.")

    return artist_ids, track_ids

def pickle_1000_artists_last_24hrs() -> None:
    '''Store a random selection of 1000 of all artists that have been added to
    featured playlists in the last 24 hours, in a temporary pickle.'''

    playlist_df = pd.read_csv(OUTPUT_DIR + DEFAULT_DATAFRAME)

    artist_ids, track_ids, playlists_found = [], [], []
    for playlist_id, playlist_name in zip(playlist_df['playlist_id'], playlist_df['playlist_name']):
        if "This Is" in playlist_name: # these explicitly highlight a popular artist
            continue
        if (
            "Hits" in playlist_name or "hits" in playlist_name or
            "Top" in playlist_name or "top" in playlist_name or
            "Official" in playlist_name or "official" in playlist_name
        ): # these are a priori popular songs
            continue
        logger.info("Entering playlist: {:s}".format(playlist_name))
        artists_to_add, tracks_to_add = get_artists_last_24hrs(playlist_id)
        artist_ids.extend(artists_to_add)
        track_ids.extend(tracks_to_add)
        playlists_found.extend([playlist_name]*len(artists_to_add))
        logger.info("Added {:d} artists and tracks to the list.".format(len(artists_to_add)))

    '''Find unique artists and their track IDs and store info as temporary pickle'''
    artist_ids, unique_indices = np.unique(artist_ids, return_index=True)
    track_ids = [track_ids[i] for i in unique_indices]
    playlists_found = [playlists_found[i] for i in unique_indices]
    artist_dict = {'ids': artist_ids, 'playlists_found': playlists_found}

    logger.info("Total number of unique artists added to playlists in the last 24 hours: {:d}".format(len(artist_ids)))
    with open(OUTPUT_DIR + "num_featured_artists.csv", "a") as f:
        f.write("{:s},{:d}\n".format(CURRENT_DATE, len(artist_ids)))
    f.close()

    if len(artist_ids) > 1000:
        random_indices = np.random.choice(len(artist_ids), 1000, replace=False)
        artist_dict = {key: [artist_dict[key][i] for i in random_indices] for key in artist_dict.keys()}
        track_ids = [track_ids[i] for i in random_indices]
        artist_ids = [artist_ids[i] for i in random_indices]

    with open(OUTPUT_DIR + "artists_last_24hrs.pkl", "wb") as f:
        pickle.dump(artist_dict, f)

    '''Also store track IDs in a temporary pickle'''
    dict_artist_tracks = {'artist_ids': artist_ids, 'track_ids': track_ids}
    with open(OUTPUT_DIR + "tracks_last_24hrs.pkl", "wb") as f:
        pickle.dump(dict_artist_tracks, f)

def scrape_monthly_listeners_for_pickled_artists() -> Dict[str, int]:
    '''Scrape monthly listeners for all artists that have been added to playlists
    in the last 24 hours. This uses the temporary pickle of artist IDs generated
    by the function pickle_all_artists_last_24hrs, and produces another
    temporary pickle of artist IDs and their monthly listeners.'''

    with open(OUTPUT_DIR + "artists_last_24hrs.pkl", "rb") as f:
        artists_dict = pickle.load(f)
        if any (len(artists_dict['ids']) != len(artists_dict[key]) for key in artists_dict.keys()):
            logger.error("Length of artist IDs and other arrays in the dictionary are not the same.")
            return

    artist_ids = artists_dict['ids']

    artist_monthly_listeners = []
    i = 0
    logger.info("Total number of artists to scrape: {:d}".format(len(artist_ids)))
    for artist_id in artist_ids:
        artist_monthly_listeners.append(scrape_monthly_listeners(artist_id, "temp"))
        i += 1
        if i % 100 == 0:
            artist_monthly_listeners_print = [x for x in artist_monthly_listeners if x is not None]
            logger.info("Scraped monthly listeners for {:d} artists. Current average is {:f}.".format(i, sum(artist_monthly_listeners_print)/i))

    artists_dict['monthly_listeners'] = artist_monthly_listeners
    with open(OUTPUT_DIR + "artists_last_24hrs_monthly_listeners.pkl", "wb") as f:
        pickle.dump(artists_dict, f)

def get_artist_info_for_pickled_artists() -> Dict[str, Dict[str, str]]:
    '''Get artist information for all artists that have been added to playlists
    in the last 24 hours. This uses the temporary pickle of artist IDs generated
    by the function pickle_all_artists_last_24hrs, and produces another
    temporary pickle of artist IDs and their information.'''

    with open(OUTPUT_DIR + "artists_last_24hrs.pkl", "rb") as f:
        artists_dict = pickle.load(f)
        if any (len(artists_dict['ids']) != len(artists_dict[key]) for key in artists_dict.keys()):
            logger.error("Length of artist IDs and other arrays in the dictionary are not the same.")
            return
    artist_ids = artists_dict['ids']

    '''Retrieve full info structure for each artist, from Spotify'''
    artists_info = aih.get_spotify_artists_info(artist_ids)

    '''Generate all info in the ArtistInfoDict structure, which involves
    retrieving artist album information via the artist_albums endpoint.'''
    artist_info_dict = aih.generate_artist_info_dict(artists_info)

    with open(OUTPUT_DIR + "artists_last_24hrs_info_dict.pkl", "wb") as f:
        pickle.dump(artist_info_dict, f)

def get_save_track_info_for_pickled_tracks() -> None:
    '''Retrieve and save the track information for all tracks that have been added
    to playlists in the last 24 hours. This uses the temporary pickle of track IDs
    generated by the function pickle_all_artists_last_24hrs.'''

    with open(OUTPUT_DIR + "tracks_last_24hrs.pkl", "rb") as f:
        dict_artist_tracks = pickle.load(f)
        track_ids = dict_artist_tracks['track_ids']
        artist_ids = dict_artist_tracks['artist_ids']

    '''First check if track info already processed and in a csv file, if so, return.'''
    if os.path.exists(OUTPUT_DIR + "track_info_last_24hrs_{:s}.csv".format(CURRENT_DATE)):
        logger.info("Track info already processed for date {:s}.".format(CURRENT_DATE))
        return

    '''Find unique track IDs and one artist for each track ID'''
    track_ids_utrack, unique_indices = np.unique(track_ids, return_index=True)
    artist_ids_utrack = [artist_ids[i] for i in unique_indices]
    logger.info("Total number of unique tracks added to playlists in the last 24 hours: {:d}".format(len(track_ids_utrack)))
    
    '''Retrieve full info structure for each track, from Spotify'''
    tracks_info = tih.get_tracks_info(track_ids_utrack)
    tracks_audio_info = tih.get_tracks_audio_info(track_ids_utrack)
    track_info_dict = tih.TrackInfoDict(tracks_info, tracks_audio_info)

    '''Append to the Spotify_track_info.csv file as a new row. Note that this must be a new file.'''
    track_info_df = pd.DataFrame({key: track_info_dict[key] for key in track_info_dict.keys})
    track_info_df.to_csv(OUTPUT_DIR + "track_info_last_24hrs_{:s}.csv".format(CURRENT_DATE), index=False)

def gather_artist_info_last_24hrs() -> None:
    '''Gather artist information from all temporary pickles, consolidate in a csv
    file and delete the temporary pickles.'''

    with open(OUTPUT_DIR + "artists_last_24hrs_info_dict.pkl", "rb") as f:
        artist_info_dict = pickle.load(f)

    with open(OUTPUT_DIR + "artists_last_24hrs_monthly_listeners.pkl", "rb") as f:
        monthly_listeners_dict = pickle.load(f)

    '''Combine the dicts and write all info to a csv'''
    total_info_dict = monthly_listeners_dict.copy()
    for key in artist_info_dict.keys:
        if key not in monthly_listeners_dict.keys():
            total_info_dict[key] = artist_info_dict[key].copy()

    total_info_df = pd.DataFrame(total_info_dict)
    total_info_df.to_csv(OUTPUT_DIR + "artists_last_24hrs_{:s}_info.csv".format(CURRENT_DATE), index=False)
    logger.info(
        "Saved all artist information to csv file: {:s}".format(
            OUTPUT_DIR + "artists_last_24hrs_{:s}_info.csv".format(CURRENT_DATE)
        ))

def delete_pickles() -> None:
    '''Delete all temporary pickles generated in the process of gathering
    artist information for the last 24 hours.'''

    try:
        os.remove(OUTPUT_DIR + "artists_last_24hrs_monthly_listeners.pkl")
        logger.info("Deleted monthly listeners pickle.")
    except FileNotFoundError:
        logger.info("Monthly listeners pickle not found: {:s}".format(OUTPUT_DIR + "artists_last_24hrs_monthly_listeners.pkl"))
    try:
        os.remove(OUTPUT_DIR + "artists_last_24hrs_info_dict.pkl".format(CURRENT_DATE))
        logger.info("Deleted info dict pickle.")
    except FileNotFoundError:
        logger.info("Info dict pickle not found: {:s}".format(OUTPUT_DIR + "artists_last_24hrs_info_dict.pkl"))
    try:
        os.remove(OUTPUT_DIR + "artists_last_24hrs.pkl")
        logger.info("Deleted artist IDs pickle.")
    except:
        logger.info("Artist IDs pickle not found: {:s}".format(OUTPUT_DIR + "artists_last_24hrs.pkl"))
    logger.info("Deleted all temporary pickles.")

if __name__ == "__main__":
    pass