#!/usr/bin/env bash
# faults/demo_failover.sh  (M2)
# Kills the current leader and measures how long until a new leader appears.
#
#   ./faults/demo_failover.sh
#
# Needs the 3 managers running (see the top of docker-compose.yml).

cd "$(dirname "$0")/.."

now() { python3 -c 'import time; print(time.time())'; }
leaders() { docker compose logs manager-1 manager-2 manager-3 2>/dev/null | grep -c "became LEADER"; }

OLD=$(docker compose logs manager-1 manager-2 manager-3 2>/dev/null \
  | grep "became LEADER" | tail -1 \
  | grep -o "\[manager-[0-9]*\]" | tr -d '[]')

if [ -z "$OLD" ]; then
  echo "No leader found. Start the managers first."
  exit 1
fi

BEFORE=$(leaders)
START=$(now)
echo "Killing leader $OLD ..."
docker compose kill "$OLD"

for i in $(seq 1 100); do
  sleep 0.2
  if [ "$(leaders)" -gt "$BEFORE" ]; then
    END=$(now)
    python3 -c "print('New leader elected %.2f s after the kill' % ($END - $START))"
    exit 0
  fi
done

echo "No new leader within 20 s"
exit 1