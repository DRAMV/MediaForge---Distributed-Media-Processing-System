#!/usr/bin/env bash
# faults/kill_leader.sh  (M2)
# Stops the current leader manager so the others must elect a new one.
#
#   ./faults/kill_leader.sh              find the leader in the logs, then kill it
#   ./faults/kill_leader.sh manager-2    kill this one

set -e

TARGET="$1"

if [ -z "$TARGET" ]; then
  TARGET=$(docker compose logs manager-1 manager-2 manager-3 2>/dev/null \
    | grep "became LEADER" | tail -1 \
    | grep -o "\[manager-[0-9]*\]" | tr -d '[]')
fi

if [ -z "$TARGET" ]; then
  echo "Could not find a leader. Pass a name, e.g. ./faults/kill_leader.sh manager-1"
  exit 1
fi

echo "Killing leader: $TARGET at $(date +%H:%M:%S)"
docker compose kill "$TARGET"
echo "Watch the other managers: docker compose logs -f"