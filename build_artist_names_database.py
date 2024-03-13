import musicbrainzngs
import argparse, logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import pickle

DEFAULT_OUTPUT_DIR = "/n/holystore01/LABS/itc_lab/Users/sjeffreson/serch/artist-database/"

def get_all_artist_names_musicbrainz(start_offset=0, num_artists=1000):
    '''Get num_artists artist names from the MusicBrainz database, starting at
    an offset of start_offset. As of March 2024, this adheres to rate-limiting
    restrictions and does not require an API key.'''

    musicbrainzngs.set_useragent("MusicBrainz All Artists", "0.1", "https://sjeffreson.github.io/serch")

    all_artist_names = []

    try:
        query = "*"  # Empty query to fetch all artists
        offset = start_offset
        limit = 100  # Adjust the limit as needed
        while offset < start_offset + num_artists:
            artists = musicbrainzngs.search_artists(query=query, limit=limit, offset=offset)
            all_artist_names.extend(artist["name"] for artist in artists["artist-list"])

            # Check if there are more results
            if len(artists["artist-list"]) == limit:
                offset += limit
            else:
                break

    except musicbrainzngs.ResponseError as e:
        print("Error:", e)

    return all_artist_names

def save_all_artist_names_musicbrainz():
    '''Save batches of artist names from the MusicBrainz database to a file.
    Will continue until interrupted or until there are no more names left to
    fetch in the database.'''

    start_offset=790000
    num_artists_batch=10000
    num_artist_names = num_artists_batch
    while num_artist_names == num_artists_batch: # retrieved a full batch, might be more
        all_artist_names = get_all_artist_names_musicbrainz(start_offset=start_offset, num_artists=num_artists_batch)

        filename = DEFAULT_OUTPUT_DIR + "/artist_names_{:d}.pkl".format(start_offset)
        with open(filename, "wb") as f:
            pickle.dump(all_artist_names, f)
        logger.info(f"Saved artist names to {filename}")

        num_artist_names = len(all_artist_names)
        start_offset += num_artists_batch

if __name__ == "__main__":
    save_all_artist_names_musicbrainz()
