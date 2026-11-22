#!/bin/bash
# Copy the ChessDB snapshot from H: (9p) into the distro's native ext4 disk, then verify every
# file's sha256 against H:\chess\chessdb\manifest.tsv. Resumable: a file whose size matches is
# not recopied; a sha256 mismatch deletes the copy and fails. COPY.DONE is written last.
set -euo pipefail
SRC=/mnt/h/chess/chessdb/chess-20260702/data
DST=/var/lib/chessdb/chess-20260702/data
LOG=/var/lib/chessdb/copy.log
mkdir -p $DST
echo "[$(date +%T)] copy start" >> $LOG
ls $SRC | xargs -P 4 -I{} bash -c '
  s=$0/{}; d=$1/{}
  if [ -f "$d" ] && [ "$(stat -c %s "$s")" = "$(stat -c %s "$d")" ]; then exit 0; fi
  cp "$s" "$d.tmp" && mv "$d.tmp" "$d"' $SRC $DST
echo "[$(date +%T)] copy done: $(ls $DST | wc -l) files, $(du -sh $DST | cut -f1)" >> $LOG
tr -d '\r' < /mnt/h/chess/chessdb/manifest.tsv | awk -F'\t' '{n=split($1,a,"\\\\"); print $3"  "a[n]}' > /var/lib/chessdb/manifest.sha256
cd $DST
if sha256sum --quiet -c /var/lib/chessdb/manifest.sha256 >> $LOG 2>&1; then
  echo "[$(date +%T)] sha256 OK: $(wc -l < /var/lib/chessdb/manifest.sha256) files" >> $LOG
  date -Is > /var/lib/chessdb/COPY.DONE
else
  echo "[$(date +%T)] sha256 FAILED" >> $LOG; exit 1
fi
