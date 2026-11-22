#!/bin/bash
# Read-only bind of the ChessDB snapshot at /mnt/chessdb-ro (WSL drops it when the VM idles).
# Prefers the verified native ext4 copy (/var/lib/chessdb, COPY.DONE) over the 9p path on H:.
NATIVE=/var/lib/chessdb/chess-20260702/data
SRC=$NATIVE; [ -f /var/lib/chessdb/COPY.DONE ] || SRC=/mnt/h/chess/chessdb/chess-20260702/data
if mountpoint -q /mnt/chessdb-ro; then
  cur=$(findmnt -n -o SOURCE /mnt/chessdb-ro)
  case "$cur" in *chessdb/chess-20260702/data*|*"$SRC"*) ;; esac
  umount /mnt/chessdb-ro
fi
mkdir -p /mnt/chessdb-ro
mount --bind "$SRC" /mnt/chessdb-ro && mount -o remount,ro,bind /mnt/chessdb-ro
echo "chessdb-ro -> $SRC"
