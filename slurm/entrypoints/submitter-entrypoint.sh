#!/bin/bash
set -e

echo "ℹ️ Start munged for auth"
gosu munge /usr/sbin/munged

exec "$@"
