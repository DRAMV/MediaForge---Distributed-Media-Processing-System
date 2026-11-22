#!/usr/bin/env bash
# faults/restart_node.sh  (M2)
# Brings a stopped manager back. It rejoins as a follower and catches up
# from the leader's log.
#
#   ./faults/restart_node.sh manager-1

set -e

TARGET="$1"
if [ -z "$TARGET" ]; then
  echo "Usage: ./faults/restart_node.sh manager-1"
  exit 1
fi

echo "Restarting $TARGET at $(date +%H:%M:%S)"
docker compose start "$TARGET"
echo "Watch it rejoin: docker compose logs -f $TARGET"