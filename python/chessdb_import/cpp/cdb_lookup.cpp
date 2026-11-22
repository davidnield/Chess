// ChessDB lookups for the move-menu import: cdb_lookup IN.tsv OUT.tsv THREADS
//
// IN:  one position per line: lo \t hi \t epd   (our 128-bit key as two signed decimals; the book's
//      EPD, legal en passant only, 4 fields, as cdbdirect requires)
// OUT: lo \t hi \t ply \t moves, same order as IN, one line per input line:
//      ply   = cdbdirect's distance for THIS position's side (-2 not in ChessDB, -1 unknown, >= 0 plies)
//      moves = "uci:score;uci:score..." best first (cdbdirect's order), scores from the side to move's
//              view after cdbdirect's backprop: |s| >= 15000 is a mate or tablebase-win claim
//              (30000 - |s| plies to mate where it is a mate), 0 includes tablebase draws/stalemates.
//              Empty when not in ChessDB.
// Written to OUT.tmp, renamed when complete (an OUT that exists is complete). Any snapshot read
// error exits 2 (cdbdirect_get verifies block checksums in this build).
#include <atomic>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include "cdbdirect.h"

int main(int argc, char *argv[]) {
  if (argc < 4) { std::cerr << "usage: cdb_lookup IN.tsv OUT.tsv THREADS\n"; return 2; }
  std::string in_path = argv[1], out_path = argv[2];
  size_t threads = std::stoul(argv[3]);
  std::ifstream in(in_path);
  if (!in) { std::cerr << "cannot open " << in_path << "\n"; return 2; }
  std::vector<std::string> keys, epds;
  std::string line;
  while (std::getline(in, line)) {
    if (!line.empty() && line.back() == '\r') line.pop_back();
    size_t a = line.find('\t'), b = a == std::string::npos ? a : line.find('\t', a + 1);
    if (b == std::string::npos) { std::cerr << "bad input line: " << line << "\n"; return 2; }
    keys.push_back(line.substr(0, b));
    epds.push_back(line.substr(b + 1));
  }
  std::uintptr_t h = cdbdirect_initialize(CHESSDB_PATH);
  std::vector<std::string> out(keys.size());
  std::atomic<size_t> known(0), next(0);
  const size_t CHUNK = 4096;
  std::vector<std::thread> ws;
  for (size_t t = 0; t < threads; ++t)
    ws.emplace_back([&] {
      for (size_t lo; (lo = next.fetch_add(CHUNK)) < keys.size();) {
        size_t hi = std::min(keys.size(), lo + CHUNK);
        for (size_t i = lo; i < hi; ++i) {
          auto r = cdbdirect_get(h, epds[i]);
          int ply = r.back().second;
          std::string s = keys[i] + "\t" + std::to_string(ply) + "\t";
          for (size_t k = 0; k + 1 < r.size(); ++k) {
            if (k) s += ';';
            s += r[k].first + ":" + std::to_string(r[k].second);
          }
          out[i] = std::move(s);
          if (ply > -2) known++;
        }
      }
    });
  for (auto &w : ws) w.join();
  cdbdirect_finalize(h);
  std::string tmp = out_path + ".tmp";
  {
    std::ofstream o(tmp, std::ios::binary);
    for (auto &s : out) o << s << '\n';
    o.flush();
    if (!o) { std::cerr << "write failed " << tmp << "\n"; return 2; }
  }
  if (std::rename(tmp.c_str(), out_path.c_str()) != 0) { std::cerr << "rename failed\n"; return 2; }
  std::cout << "positions " << keys.size() << " known " << known << "\n";
  return 0;
}
