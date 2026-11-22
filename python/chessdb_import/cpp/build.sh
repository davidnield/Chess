#!/bin/bash
# Build the ChessDB import tools against the pilot's cdbdirect + TerarkDB (run inside WSL as root).
set -e
SRC=/mnt/c/Users/David/Documents/Chess/scratch/chessdb_import/cpp
DST=/opt/chessdb-pilot/import
T=/opt/chessdb-pilot/terarkdb
mkdir -p $DST
for tool in "$@"; do
  cp $SRC/$tool.cpp $DST/
  g++ -O3 -march=native -flto=auto -DCHESSDB_PATH=\"/mnt/chessdb-ro\" -I/opt/chessdb-pilot/cdbdirect \
      -o $DST/$tool $DST/$tool.cpp /opt/chessdb-pilot/cdbdirect/libcdbdirect.a -L$T/output/lib \
      -lterarkdb -lterark-zip-r -lboost_fiber -lboost_context -ljemalloc -pthread -lgcc -lrt -ldl \
      -ltbb -lgomp -lsnappy -llz4 -lz -lbz2 -latomic 2>&1 | grep -E "error" || true
  ls -la $DST/$tool
done
