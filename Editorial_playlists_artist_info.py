import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
import numpy as np

from typing import List, Dict, Tuple
import time, os
from datetime import datetime
import pickle

from Monthly_Listeners_webscraper import scrape_monthly_listeners
import artist_info_helper as aih

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
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
DEFAULT_DATAFRAME = "Playlist_names-IDs_{:s}.csv".format(CURRENT_DATE)
USER_ID = 'spotify'
TIMEOUT = 10*60

scope = "user-library-read playlist-read-private"
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))

def store_available_spotify_playlists() -> None:
    '''Store Spotify public playlists that are directly available via the user_playlists
    endpoint with the USER_ID "spotify". This is limited by market, but is a good starting
    point.'''

    playlists = sp.user_playlists(USER_ID, limit=50)

    '''Initial store'''
    playlist_names, playlist_ids, playlist_owners, playlist_public = [], [], [], []
    for playlist in playlists['items']:
        if playlist['owner']['id'] != USER_ID or not playlist['public']: # just to be sure
            continue
        playlist_names.append(playlist['name'])
        playlist_ids.append(playlist['id'])
        playlist_owners.append(playlist['owner']['id'])
        playlist_public.append(playlist['public'])

    '''Continue fetching until no more results left'''
    while playlists['next']:
        playlists = sp.next(playlists)
        for playlist in playlists['items']:
            if playlist['owner']['id'] != USER_ID or not playlist['public']:
                continue
            playlist_names.append(playlist['name'])
            playlist_ids.append(playlist['id'])
            playlist_owners.append(playlist['owner']['id'])
            playlist_public.append(playlist['public'])
        logger.info("Saved batch of {:d} playlists.".format(len(playlist_names)))

    playlist_df = pd.DataFrame({
        'playlist_name': playlist_names,
        'playlist_id': playlist_ids,
        'playlist_owner': playlist_owners,
        'public': playlist_public
    })
    playlist_df.to_csv(DEFAULT_OUTPUT_DIR + DEFAULT_DATAFRAME, index=False)

def store_spotify_playlist_selection_for_market(market: str) -> None:
    '''Store Spotify public playlists that are available via browsing categories in
    a particular market. This is also not comprehensive because the results are just a subset
    of the total and are ordered by popularity and other metrics. However it seems
    to be the only way to browse markets now that the market is not an input parameter
    for the user_playlists endpoint.'''

    logger.info("Entering Market: {:s}".format(market))
    categories = sp.categories(country=market)

    playlist_names, playlist_ids, playlist_owners, playlist_public = [], [], [], []
    playlist_category_ids, playlist_category_names = [], []
    for category in categories['categories']['items']:
        logger.info("Entering Category: ", category['name'])
        playlists = sp.category_playlists(category_id=category['id'], country=market)
        for playlist in playlists['playlists']['items']:
            if playlist['owner']['id'] != USER_ID or not playlist['public']: # just to be sure
                continue
            playlist_names.append(playlist['name'])
            playlist_ids.append(playlist['id'])
            playlist_owners.append(playlist['owner']['id'])
            playlist_public.append(playlist['public'])
            playlist_category_ids.append(category['id'])
            playlist_category_names.append(category['name'])
    playlist_df = pd.DataFrame({
        'playlist_name': playlist_names,
        'playlist_id': playlist_ids,
        'playlist_owner': playlist_owners,
        'public': playlist_public,
        'category_id': playlist_category_ids,
        'category_name': playlist_category_names
    })
    filename = DEFAULT_OUTPUT_DIR + DEFAULT_DATAFRAME.split(".csv")[0] + \
        "_{:s}".format(market) + ".csv"
    playlist_df.to_csv(filename, index=False)
    logger.info("Saved playlists for market: {:s} to {:s}.".format(market, filename))

def store_spotify_playlist_selection_all_markets() -> None:
    '''Store Spotify public playlists that are available via browsing categories and
    markets. This is also not comprehensive because the results are just a subset
    of the total and are ordered by popularity and other metrics. However it seems
    to be the only way to browse markets now that the market is not an input parameter
    for the user_playlists endpoint.'''

    markets = sp.markets()
    for market in markets['markets']:
        store_spotify_playlist_selection_for_market(market)

def get_artists_last_24hrs(playlist_id: str) -> List[str]:
    '''Get the tracks added to a playlist in the last 24 hours and store
    the IDs of their artists in a temporary pickle.'''

    playlist = sp.playlist(playlist_id)
    tracks = playlist['tracks']
    artist_ids = []
    for track in tracks['items']:
        added_at = track['added_at']
        added_at = datetime.strptime(added_at, "%Y-%m-%dT%H:%M:%SZ")
        if (datetime.now() - added_at).days < 1:
            try:
                for artist in track['track']['artists']:
                    artist_ids.append(artist['id'])
            except TypeError or KeyError:
                logger.info("No artist information available for that track.")
    artist_ids = list(set(artist_ids))

    return artist_ids

def pickle_1000_artists_last_24hrs(playlist_df: pd.DataFrame) -> None:
    '''Store a random selection of 1000 of all artists that have been added to
    featured playlists in the last 24 hours, in a temporary pickle.'''

    artist_ids, playlists_found = [], []
    for playlist_id, playlist_name in zip(playlist_df['playlist_id'], playlist_df['playlist_name']):
        if "This Is" in playlist_name: # these explicitly highlight a popular artist
            continue
        if (
            "Hits" in playlist_name or "hits" in playlist_name or
            "Top" in playlist_name or "top" in playlist_name
        ): # these are a priori popular songs
            continue
        logger.info("Entering playlist: {:s}".format(playlist_name))
        artists_to_add = get_artists_last_24hrs(playlist_id)
        artist_ids.extend(artists_to_add)
        playlists_found.extend([playlist_name]*len(artists_to_add))
        logger.info("Added {:d} artists to the list.".format(len(artists_to_add)))

    artist_ids, unique_indices = np.unique(artist_ids, return_index=True)
    playlists_found = [playlists_found[i] for i in unique_indices]
    artist_dict = {'ids': artist_ids, 'playlists_found': playlists_found}

    if len(artist_ids) > 1000:
        random_indices = np.random.choice(len(artist_ids), 1000, replace=False)
        artist_dict = {key: [artist_dict[key][i] for i in random_indices] for key in artist_dict.keys()}

    logger.info("Total number of unique artists added to playlists in the last 24 hours: {:d}".format(len(artist_ids)))

    with open(DEFAULT_OUTPUT_DIR + "artists_last_24hrs_{:s}.pkl".format(CURRENT_DATE), "wb") as f:
        pickle.dump(artist_dict, f)

def scrape_monthly_listeners_for_pickled_artists() -> Dict[str, int]:
    '''Scrape monthly listeners for all artists that have been added to playlists
    in the last 24 hours. This uses the temporary pickle of artist IDs generated
    by the function pickle_all_artists_last_24hrs, and produces another
    temporary pickle of artist IDs and their monthly listeners.'''

    with open(DEFAULT_OUTPUT_DIR + "artists_last_24hrs_{:s}.pkl".format(CURRENT_DATE), "rb") as f:
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
    with open(DEFAULT_OUTPUT_DIR + "artists_last_24hrs_{:s}_monthly_listeners.pkl".format(CURRENT_DATE), "wb") as f:
        pickle.dump(artists_dict, f)

def get_artist_info_for_pickled_artists() -> Dict[str, Dict[str, str]]:
    '''Get artist information for all artists that have been added to playlists
    in the last 24 hours. This uses the temporary pickle of artist IDs generated
    by the function pickle_all_artists_last_24hrs, and produces another
    temporary pickle of artist IDs and their information.'''

    with open(DEFAULT_OUTPUT_DIR + "artists_last_24hrs_{:s}.pkl".format(CURRENT_DATE), "rb") as f:
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

    with open(DEFAULT_OUTPUT_DIR + "artists_last_24hrs_{:s}_info_dict.pkl".format(CURRENT_DATE), "wb") as f:
        pickle.dump(artist_info_dict, f)

def gather_artist_info_last_24hrs() -> None:
    '''Gather artist information from all temporary pickles, consolidate in a csv
    file and delete the temporary pickles.'''

    with open(DEFAULT_OUTPUT_DIR + "artists_last_24hrs_{:s}_info_dict.pkl".format(CURRENT_DATE), "rb") as f:
        artist_info_dict = pickle.load(f)

    with open(DEFAULT_OUTPUT_DIR + "artists_last_24hrs_{:s}_monthly_listeners.pkl".format(CURRENT_DATE), "rb") as f:
        monthly_listeners_dict = pickle.load(f)

    '''Combine the dicts and write all info to a csv'''
    total_info_dict = monthly_listeners_dict.copy()
    for key in artist_info_dict.keys:
        if key not in monthly_listeners_dict.keys():
            total_info_dict[key] = artist_info_dict[key].copy()

    total_info_df = pd.DataFrame(total_info_dict)
    total_info_df.to_csv(DEFAULT_OUTPUT_DIR + "artists_last_24hrs_{:s}_info.csv".format(CURRENT_DATE), index=False)

def delete_pickles() -> None:
    '''Delete all temporary pickles generated in the process of gathering
    artist information for the last 24 hours.'''

    os.remove(DEFAULT_OUTPUT_DIR + "artists_last_24hrs_{:s}.pkl".format(CURRENT_DATE))
    os.remove(DEFAULT_OUTPUT_DIR + "artists_last_24hrs_{:s}_monthly_listeners.pkl".format(CURRENT_DATE))
    os.remove(DEFAULT_OUTPUT_DIR + "artists_last_24hrs_{:s}_info_dict.pkl".format(CURRENT_DATE))
    logger.info("Deleted all temporary pickles.")

if __name__ == "__main__":
    # store_available_spotify_playlists()
    # pickle_1000_artists_last_24hrs(pd.read_csv(DEFAULT_OUTPUT_DIR + DEFAULT_DATAFRAME))
    # scrape_monthly_listeners_for_pickled_artists()
    # get_artist_info_for_pickled_artists()
    # gather_artist_info_last_24hrs()
    delete_pickles()

