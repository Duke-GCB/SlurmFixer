# SlurmFixer
Find and fix bad jobs in a slurm cluster
Assumes mysql slurm database, you are running on a server with ssh access to nodes, and access to slurmdbd.conf file.

## Setup
`pip install -r requirements.txt`

## Run
Find jobs that are not running(according to squeue) but unfinished in the slurm database(sacct).
```
python slurmfixer.py find_bad
```

Print SQL to fix jobs that are not running(according to squeue) but unfinished in the slurm database(sacct).
```
python slurmfixer.py fix_bad
```

Find jobs running on nodes by non-service accounts but not in squeue. These are orphans or rogue started jobs.
```
python slurmfixer.py find_orphans
```
