#!/bin/bash
set -e

echo "ℹ️ Start munged for auth"
gosu munge /usr/sbin/munged

echo "ℹ️ Start slurm worker daemon"
exec slurmd -Dvvv

exec "$@"