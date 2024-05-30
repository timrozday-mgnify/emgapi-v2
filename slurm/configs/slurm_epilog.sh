#!/bin/bash


# Remove the /nfs/public "mount" if it exists
# This would be at the end of a datamover job.
# Note that we can't check the slurm partition in this epilog, because slurm doesn't populate the env var
# (at least in older slurm versions...)
NFS_TARGET="/nfs/public"

if [ -e "$NFS_TARGET" ]; then
    rm -rf "$NFS_TARGET"
fi

exit 0
