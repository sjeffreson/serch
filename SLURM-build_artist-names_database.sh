#!/bin/bash
#SBATCH -J clouds
#SBATCH --mail-user=sarah.jeffreson@cfa.harvard.edu
#SBATCH --mail-type=END
#SBATCH -p hernquist # queue (partition)
#SBATCH -e ./err_log.%j
#SBATCH -o ./out_log.%j
#SBATCH -t 08:00:00 # h:min:s
#SBATCH --ntasks 1
#SBATCH --nodes 1
#SBATCH --mem=0

cd $SLURM_SUBMIT_DIR

# load the modules
module --force purge
module load Mambaforge/22.11.1-fasrc01

# allow large core dumps
ulimit -c unlimited

# run
python3 build_rand_artist_database.py