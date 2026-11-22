#!/bin/bash
# The manifest's 40-hex entries are Git blob ids (HF small files): sha1("blob <size>\0" + content).
cd /var/lib/chessdb/chess-20260702/data
bad=0; n=0
while read -r id name; do
  [ ${#id} -eq 40 ] || continue
  n=$((n+1))
  got=$( { printf 'blob %d\0' "$(stat -c %s "$name")"; cat "$name"; } | sha1sum | cut -c1-40)
  if [ "$got" = "$id" ]; then echo "ok  $name"; else echo "BAD $name $got"; bad=$((bad+1)); fi
done < /var/lib/chessdb/manifest.sha256
echo "git-blob checked $n, bad $bad"
[ $bad -eq 0 ] && [ $n -eq 12 ] && echo "[$(date +%T)] git-blob OK: $n small files" >> /var/lib/chessdb/copy.log
