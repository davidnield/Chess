#!/bin/bash
set -euo pipefail
dest=/tmp/chessdb-query-oracle-20261125
mkdir -p "$dest"
tar -xzf /mnt/d/chess/bin/chessdb-archive-20261123-v6/decoder-source.tar.gz -C "$dest"
g++ -O2 -std=c++17 -I"$dest" "$(dirname "$0")/key_oracle.cpp" "$dest/fen2cdb.cpp" -o "$dest/key-oracle"
sha256sum "$dest/key-oracle" "$dest/fen2cdb.cpp"
