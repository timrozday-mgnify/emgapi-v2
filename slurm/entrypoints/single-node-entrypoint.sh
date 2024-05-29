#!/bin/bash
set -e

echo "ℹ️ Start dbus"
service dbus start

echo "ℹ️ Ensure ownership of munge key"
chown -R munge:munge /etc/munge

echo "ℹ️ Start munged for auth"
gosu munge /usr/sbin/munged

echo "ℹ️ Start slurm dbd for job persistence"
slurmdbd
echo "slurm dbd started"

while ! nc -z localhost 6819; do
    echo "ℹ️ Waiting for slurm dbd to be ready..."
    sleep 2
done

echo "ℹ️ Start slurm controller"
slurmctld

while ! nc -z localhost 6817; do
    echo "ℹ️ Waiting for slurm ctl to be ready..."
    sleep 2
done

echo "ℹ️ Start slurm worker daemon"
slurmd

tail -f /var/log/slurm/slurmdbd.log /var/log/slurm/slurmd.log /var/log/slurm/slurmctld.log

exec "$@"
