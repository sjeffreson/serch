from ArtistNames_analysis import ArtistNames
import argparse, logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import sys, os, glob, re
regex = re.compile(r"\d+")
import random
import numpy as np
import time

total_num_artist_names = 850000
for num_artist_names in range(1000, total_num_artist_names, 1000):
    start_time = time.time()
    data = ArtistNames(rand_num_artist_names=num_artist_names, random_seed=42)
    data.get_artist_info()
    print(f"Time to load data: {time.time() - start_time}. Pausing for 2 minutes...")
    time.sleep(120)
    del data

# data = ArtistNames(rand_num_artist_names=50, random_seed=42)
# data.get_artist_info()
# print(data.artist_info)