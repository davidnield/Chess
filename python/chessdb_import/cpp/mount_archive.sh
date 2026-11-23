#!/bin/bash
set -euo pipefail
if mountpoint -q /mnt/chessdb-ro; then
  findmnt -n -o OPTIONS /mnt/chessdb-ro | grep -q '^ro,'
  exit 0
fi
test -f /var/lib/chessdb/COPY.DONE
test -d /var/lib/chessdb/chess-20260702/data
mkdir -p /mnt/chessdb-ro
mount --bind /var/lib/chessdb/chess-20260702/data /mnt/chessdb-ro
mount -o remount,ro,bind /mnt/chessdb-ro
