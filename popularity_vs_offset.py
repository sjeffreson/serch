import spotipy
from spotipy.oauth2 import SpotifyOAuth
import time
import random
import pickle
import argparse, logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

'''
Remember environment variables:
export SPOTIPY_CLIENT_ID='your-spotify-client-id'
export SPOTIPY_CLIENT_SECRET='your-spotify-client-secret'
export SPOTIPY_REDIRECT_URI='your-app-redirect-url'
'''

DEFAULT_OUTPUT_DIR = "./data-output/"

scope = "playlist-read-private playlist-read-collaborative"
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))

def save_offset_vs_pop(num_artists=100, max_offset=1000, random_seed=0, q="a"):

    '''using a random query character q in the search endpoint, select a
    number of artists num_artists < 1000, with uniformly-sampled offsets,
    to see how the popularity score varies with offset'''

    offset = 0
    artist_offset_dict = {'artist_ids': [], 'artist_names': [], 'artist_pop': [], 'offsets': []}
    for offset in range(0, max_offset, max_offset//num_artists):
        results = sp.search(q=q, type='artist', limit=1, offset=offset)
        artists = results['artists']['items']
        for artist in artists:
            artist_offset_dict['artist_ids'].append(artist['id'])
            artist_offset_dict['artist_names'].append(artist['name'])
            artist_offset_dict['artist_pop'].append(artist['popularity'])
            artist_offset_dict['offsets'].append(offset)

    # save the dictionary as a pickle file
    filename = DEFAULT_OUTPUT_DIR + "artist_offset_dict_{:s}.pkl".format(q).replace("\"", "").replace(" ", "")
    with open(filename, "wb") as f:
        pickle.dump(artist_offset_dict, f)

    logger.info(f"Saved artist_offset_dict to {filename}")

if __name__ == "__main__":

    # completely general/unrestricted query
    save_offset_vs_pop(q='artist:""', num_artists=1000)

    # # single-letter queries
    # qs = [
    #     "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k",
    #     "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w",
    #     "x", "y", "z"
    # ]    
    # for q in qs:
    #     save_offset_vs_pop(q='artist:"{:s}"'.format(q), num_artists=50)

    # single-letter starting letter queries
    # qs = [
    #     "a%", "b%", "c%", "d%", "e%", "f%", "g%", "h%", "i%", "j%", "k%",
    #     "l%", "m%", "n%", "o%", "p%", "q%", "r%", "s%", "t%", "u%", "v%", "w%",
    #     "x%", "y%", "z%"
    # ]
    # for q in qs:
    #     save_offset_vs_pop(q='artist:"{:s}"'.format(q), num_artists=50)

    # q1s = qs[:len(qs)//2]
    # q2s = qs[len(qs)//2:]
    # qpairs = [(q1, q2) for q1 in q1s for q2 in q2s]
    # random.shuffle(qpairs)
    # qpair_strs = [q1 + " " + q2 for q1, q2 in qpairs]
    # for q in qpair_strs:
    #     save_offset_vs_pop(q='artist:"{:s}"'.format(q), num_artists=50)


    '''to think about: a smaller pool (less common query) sampled from the same
    exponential distribution is exponentially less likely to sample the top
    values in that exponential distribution?'''