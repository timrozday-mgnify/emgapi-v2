#!/bin/bash
set -e

echo "ℹ️ Start munged for auth"
gosu munge /usr/sbin/munged

echo "ℹ️ Start slurm dbd for job persistence"
exec slurmdbd -D

exec "$@"
