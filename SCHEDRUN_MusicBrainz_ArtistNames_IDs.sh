#!/bin/bash

# load modules
module --force purge
module load Mambaforge/22.11.1-fasrc01

/n/sw/Mambaforge-22.11.1-4/bin/python3 ./MusicBrainz_ArtistNames_IDs.py