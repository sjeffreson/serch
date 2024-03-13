from ArtistNames_analysis import ArtistNames
import argparse, logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import sys, os, glob, re
regex = re.compile(r"\d+")
import random
import numpy as np

# time this function
data = ArtistNames(rand_num_artist_names=100, random_seed=100)
np.save('artist_info_rand_{:d}.npy'.format(rand_num_artist_names), data.artist_info)
print(data.artist_info['popularity'])

legacy_artists = data.get_famous_legacy_artists()
print(legacy_artists['popularity'])

# active_artist_data = data.get_active_artists()
# print(active_artist_data['popularity'])

# data.clean_unproductive_artists()
# print(data.artist_info['popularity'])