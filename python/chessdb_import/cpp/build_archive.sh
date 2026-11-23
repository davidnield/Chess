#!/bin/bash
set -euo pipefail
P=${1:?usage: build_archive.sh RELEASE_DIRECTORY}
T=/opt/chessdb-pilot/terarkdb
C=/opt/chessdb-pilot/cdbdirect
g++ -O3 -std=c++17 -I"$C" -I"$T/output/include" -I"$T/third-party/terark-zip/src" -I"$T/include" \
  -o "$P/cdb_archive" "$P/cdb_archive.cpp" "$C/libcdbdirect.a" -L"$T/output/lib" \
  -lterarkdb -lterark-zip-r -lboost_fiber -lboost_context -ljemalloc -pthread -lgcc -lrt -ldl \
  -ltbb -lgomp -lsnappy -llz4 -lz -lbz2 -latomic /usr/lib/x86_64-linux-gnu/libcrypto.so.3
{ sha256sum "$C/cdbdirect.cpp" "$C/fen2cdb.cpp" "$C/libcdbdirect.a" "$P/cdb_archive.cpp" "$P/cdb_archive" /usr/lib/x86_64-linux-gnu/libcrypto.so.3
  git -C "$C" rev-parse HEAD
  git -C "$T" rev-parse HEAD
} > "$P/reader-provenance.txt"
