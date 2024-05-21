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

with open('config.json') as f:
    config = json.load(f)
OUTPUT_DIR = config['paths']['editorial_output_dir']
BIO_GENRES = config['filenames']['bio_genres']
MISSING_BIO_GENRES = config['filenames']['missing_bio_genres']
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

    with open(OUTPUT_DIR + "artists_last_24hrs_{:s}.pkl".format(CURRENT_DATE), "wb") as f:
        pickle.dump(artist_dict, f)

    '''Also store track IDs in a temporary pickle'''
    dict_artist_tracks = {'artist_ids': artist_ids, 'track_ids': track_ids}
    with open(OUTPUT_DIR + "tracks_last_24hrs_{:s}.pkl".format(CURRENT_DATE), "wb") as f:
        pickle.dump(dict_artist_tracks, f)

def scrape_monthly_listeners_for_pickled_artists() -> Dict[str, int]:
    '''Scrape monthly listeners for all artists that have been added to playlists
    in the last 24 hours. This uses the temporary pickle of artist IDs generated
    by the function pickle_all_artists_last_24hrs, and produces another
    temporary pickle of artist IDs and their monthly listeners.'''

    with open(OUTPUT_DIR + "artists_last_24hrs_{:s}.pkl".format(CURRENT_DATE), "rb") as f:
        artists_dict = pickle.load(f)
        if any (len(artists_dict['ids']) != len(artists_dict[key]) for key in artists_dict.keys()):
            logger.error("Length of artist IDs and other arrays in the dictionary are not the same.")
            return

    artist_ids = artists_dict['ids']

    artist_monthly_listeners = []
    i = 0
    logger.info("Monthly Listeners: Total number of artists to scrape: {:d}".format(len(artist_ids)))
    for artist_id in artist_ids:
        artist_monthly_listeners.append(scrape_monthly_listeners(artist_id, "temp"))
        i += 1
        print(i)
        if i % 50 == 0:
            artist_monthly_listeners_print = [x for x in artist_monthly_listeners if x is not None]
            logger.info("Scraped monthly listeners for {:d} artists. Current average is {:f}.".format(i, sum(artist_monthly_listeners_print)/i))
            logger.info("Pausing for 5 minutes...")
            with open (OUTPUT_DIR + "artists_last_24hrs_monthly_listeners_{:s}_{:d}.pkl".format(CURRENT_DATE, i), "wb") as f:
                pickle.dump(artist_monthly_listeners, f)
            time.sleep(300)

    artists_dict['monthly_listeners'] = artist_monthly_listeners
    # gather up the pickles
    # monthly_listeners_dict = []
    # for i in range(50, len(artist_ids)+1, 50):
    #     with open(OUTPUT_DIR + "artists_last_24hrs_monthly_listeners_{:s}_{:d}.pkl".format(CURRENT_DATE, i), "rb") as f:
    #         monthly_listeners_dict.extend(pickle.load(f))
    # with open(OUTPUT_DIR + "artists_last_24hrs_monthly_listeners_{:s}.pkl".format(CURRENT_DATE), "wb") as f:
    #     pickle.dump(artists_dict, f)

def get_artist_info_for_pickled_artists() -> Dict[str, Dict[str, str]]:
    '''Get artist information for all artists that have been added to playlists
    in the last 24 hours. This uses the temporary pickle of artist IDs generated
    by the function pickle_all_artists_last_24hrs, and produces another
    temporary pickle of artist IDs and their information.'''

    with open(OUTPUT_DIR + "artists_last_24hrs_{:s}.pkl".format(CURRENT_DATE), "rb") as f:
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

    with open(OUTPUT_DIR + "artists_last_24hrs_info_dict_{:s}.pkl".format(CURRENT_DATE), "wb") as f:
        pickle.dump(artist_info_dict, f)

def get_save_track_info_for_pickled_tracks(date: str=None) -> None:
    '''Retrieve and save the track information for all tracks that have been added
    to playlists in the last 24 hours. This uses the temporary pickle of track IDs
    generated by the function pickle_all_artists_last_24hrs.'''

    if date is None:
        date = CURRENT_DATE

    with open(OUTPUT_DIR + "tracks_last_24hrs_{:s}.pkl".format(date), "rb") as f:
        dict_artist_tracks = pickle.load(f)
        track_ids = dict_artist_tracks['track_ids']
        artist_ids = dict_artist_tracks['artist_ids']

    '''First check if track info already processed and in a csv file, if so, return.'''
    if os.path.exists(OUTPUT_DIR + "track_info_last_24hrs_{:s}.csv".format(date)):
        logger.info("Track info already processed for date {:s}.".format(date))
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
    track_info_df.to_csv(OUTPUT_DIR + "track_info_last_24hrs_{:s}.csv".format(date), index=False)

def get_save_track_info_for_trackids(trackid_filename: str, num_to_scrape: int=None) -> None:
    '''Retrieve and save the track information for the master list of unique tracks. Assumes
    that the track IDs are unique in this list.'''

    if trackid_filename is None:
        logger.error("No filename provided.")

    '''List of unique track IDs'''
    track_ids_df = pd.read_csv(OUTPUT_DIR + trackid_filename)

    '''Track info file, load if it exists, otherwise create a new one'''
    stored_ids = []
    if os.path.exists(OUTPUT_DIR + "featured_Spotify_track_info.csv"):
        track_info_df = pd.read_csv(OUTPUT_DIR + "featured_Spotify_track_info.csv")
        stored_ids.extend(track_info_df['ids'])
    else:
        track_info_df = pd.DataFrame()
    logger.info("Total number of unique track IDs found: {:d}".format(len(track_ids_df)))
    logger.info("Total number of unique track IDs already assigned info in the csv: {:d}".format(len(track_info_df)))
    
    '''Get the IDs that have not yet been computed'''
    print(len(track_ids_df['feat_track_ids']), len(stored_ids))
    track_ids = [track_id for track_id in track_ids_df['feat_track_ids'] if track_id not in stored_ids]
    print(len(track_ids))
    if num_to_scrape is not None:
        track_ids = track_ids[:num_to_scrape]
    logger.info("Computing information for {:d} tracks.".format(len(track_ids)))

    '''Retrieve full info structure for each track, from Spotify'''
    tracks_info = tih.get_tracks_info(track_ids)
    tracks_audio_info = tih.get_tracks_audio_info(track_ids)
    track_info_dict = tih.TrackInfoDict(tracks_info, tracks_audio_info)

    '''Add the dates, counts and playlists_featured from the track_ids_df (artists updated separately in get_tracks_info)'''
    track_info_dict['dates'] = [
        track_ids_df[track_ids_df['feat_track_ids'] == track_id]['dates'].values[0] for track_id in track_ids]
    track_info_dict['count'] = [
        track_ids_df[track_ids_df['feat_track_ids'] == track_id]['count'].values[0] for track_id in track_ids]
    track_info_dict['playlists_found'] = [
        track_ids_df[track_ids_df['feat_track_ids'] == track_id]['playlists_found'].values[0] for track_id in track_ids]

    '''Append to the featured_Spotify_track_info.csv file as new rows.'''
    track_info_df = pd.concat(
        [track_info_df, pd.DataFrame({key: track_info_dict[key] for key in
        track_info_dict.keys + ['count', 'dates', 'playlists_found']})], ignore_index=True
    )
    track_info_df.to_csv(OUTPUT_DIR + "featured_Spotify_track_info.csv", index=False)

def gather_artist_info_last_24hrs() -> None:
    '''Gather artist information from all temporary pickles, consolidate in a csv
    file and delete the temporary pickles.'''

    with open(OUTPUT_DIR + "artists_last_24hrs_info_dict_{:s}.pkl".format(CURRENT_DATE), "rb") as f:
        artist_info_dict = pickle.load(f)

    with open(OUTPUT_DIR + "artists_last_24hrs_monthly_listeners_{:s}.pkl".format(CURRENT_DATE), "rb") as f:
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
        os.remove(OUTPUT_DIR + "artists_last_24hrs_monthly_listeners_{:s}.pkl".format(CURRENT_DATE))
        logger.info("Deleted monthly listeners pickle.")
    except FileNotFoundError:
        logger.info("Monthly listeners pickle not found: {:s}".format(OUTPUT_DIR + "artists_last_24hrs_monthly_listeners_{:s}.pkl".format(CURRENT_DATE)))
    try:
        os.remove(OUTPUT_DIR + "artists_last_24hrs_info_dict_{:s}.pkl".format(CURRENT_DATE))
        logger.info("Deleted info dict pickle.")
    except FileNotFoundError:
        logger.info("Info dict pickle not found: {:s}".format(OUTPUT_DIR + "artists_last_24hrs_info_dict_{:s}.pkl".format(CURRENT_DATE)))
    try:
        os.remove(OUTPUT_DIR + "artists_last_24hrs_{:s}.pkl".format(CURRENT_DATE))
        logger.info("Deleted artist IDs pickle.")
    except:
        logger.info("Artist IDs pickle not found: {:s}".format(OUTPUT_DIR + "artists_last_24hrs_{:s}.pkl".format(CURRENT_DATE)))
    logger.info("Deleted all temporary pickles.")

def generate_artist_genres_from_bio(num_to_scrape: int=None) -> None:
    genres_to_search = config["genres"]

    artist_info_df = pd.read_csv(OUTPUT_DIR + "featured_Spotify_artist_info.csv", usecols=["ids", "names", "genres"])
    artist_ids = artist_info_df[artist_info_df["genres"].isnull()]["ids"].tolist()
    artist_ids = list(set(artist_ids))
    logger.info(f"Number of unique artists with no genres: {len(artist_ids)}")

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

def clean_save_artist_info(req_features: List[str]) -> None:
    '''Clean the Spotify_artist_info_Mnth-Lstnrs.csv file, and save it to a new csv file.'''

    artist_info_df = pd.read_csv(OUTPUT_DIR + "featured_Spotify_artist_info.csv")

    for_logging = artist_info_df.dropna(subset=[column for column in artist_info_df.columns if column in req_features])
    for_logging = for_logging.drop(for_logging[(for_logging[req_features] == -1).any(axis=1)].index)
    logger.info("Number of artists with no genre, before bio-scraping: {:d}".format(len(for_logging)))

    '''First add the genres scraped from the Spotify bios'''
    if 'genres' in req_features:
        bio_genres_df = pd.read_csv(OUTPUT_DIR + BIO_GENRES)
        artist_info_df = artist_info_df.merge(bio_genres_df[['ids', 'genres']], on='ids', how='left', suffixes=('', '_new'))
        artist_info_df['genres'] = artist_info_df['genres'].combine_first(artist_info_df['genres_new'])
        artist_info_df = artist_info_df.drop(columns=['genres_new'])

    '''Clean any NaN or -1 values in the required features that are still missing'''
    artist_info_df = artist_info_df.dropna(subset=[column for column in artist_info_df.columns if column in req_features])
    artist_info_df = artist_info_df.drop(artist_info_df[(artist_info_df[req_features] == -1).any(axis=1)].index)
    logger.info("Number of artists after cleaning, with bio-scraping: {:d}".format(len(artist_info_df)))

    '''Save to a new csv file with a date-stamp for cleaning time'''
    artist_info_df.to_csv(OUTPUT_DIR + "CLEANED_featured_Spotify_artist_info.csv", index=False)
    logger.info("Saved cleaned artist info to {:s}.".format("CLEANED_featured_Spotify_artist_info.csv"))

if __name__ == "__main__":
    pass
    # for i in range(30):
    #     get_save_track_info_for_trackids('featured_Spotify_tracks.csv', num_to_scrape=500)
    #     logger.info("Finished batch {:d} of 32. Pausing for 1 minute...".format(i+1))
    #     time.sleep(60)