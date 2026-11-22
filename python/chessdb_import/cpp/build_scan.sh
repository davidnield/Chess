#!/bin/bash
# Build cdb_scan (run inside WSL as root). Needs the rocksdb/terark headers, unlike the lookup tools.
set -e
SRC=/mnt/c/Users/David/Documents/Chess/scratch/chessdb_import/cpp
DST=/opt/chessdb-pilot/import
T=/opt/chessdb-pilot/terarkdb
C=/opt/chessdb-pilot/cdbdirect
cp $SRC/cdb_scan.cpp $SRC/z128_keys.h $DST/
g++ -O3 -march=native -flto=auto -std=c++17 -DCHESSDB_PATH=\"/mnt/chessdb-ro\" -I$C \
    -I$T/output/include -I$T/third-party/terark-zip/src -I$T/include \
    -o $DST/cdb_scan $DST/cdb_scan.cpp $C/libcdbdirect.a -L$T/output/lib \
    -lterarkdb -lterark-zip-r -lboost_fiber -lboost_context -ljemalloc -pthread -lgcc -lrt -ldl \
    -ltbb -lgomp -lsnappy -llz4 -lz -lbz2 -latomic
ls -la $DST/cdb_scan
