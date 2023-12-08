#!/bin/bash

#SBATCH --job-name=postcode_finder
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --time=00:10:00
#SBATCH --mem=256G
#SBATCH --account=geog029585
#SBATCH --mail-type=ALL 


cd "${SLURM_SUBMIT_DIR}"

echo Running on host "$(hostname)"
echo Time is "$(date)"
echo Start Time is "$(date)"
echo Directory is "$(pwd)"
echo Slurm job ID is "${SLURM_JOBID}"
echo This jobs runs on the following machines:
echo "${SLURM_JOB_NODELIST}"

#~/.conda/envs/cc_project/bin/python
#module load lang/python/anaconda/3.9.7-2021.12-tensorflow.2.7.0
#source activate 
source activate cc_project

#export OMP_NUM_THREADS=1

python debugging_2024.py

echo End Time is"$(date)"