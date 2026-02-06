#!/bin/bash
#SBATCH -J crawl
#SBATCH -N 1
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH -t 04:00:00
#SBATCH -o logs/slurm-%j.out

set -euo pipefail
cd ~/26W_MDIL_Intern/paper_search/paper_download

source .venv/bin/activate
export PYTHONUNBUFFERED=1

python parallel_download.py