#!/bin/bash

PARTITION_NAME=$SLURM_JOB_PARTITION

# If this is a datamover job (datamover partition) then fake the mounting of additional nfs filesystems
# In reality we are just making symlinks, because this is a dev env after all.
HIDDEN_NFS_SOURCE="/publicnfs"
NFS_TARGET="/nfs/public"

if [ "$PARTITION_NAME" == "datamover" ]; then
    if [ -e "$NFS_TARGET" ]; then
        rm -rf "$NFS_TARGET"
    fi
    # Create the symbolic link
    ln -s "$HIDDEN_NFS_SOURCE" "$NFS_TARGET"
fi

exit 0
