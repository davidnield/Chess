// Timing bench for ChessDB point lookups: probe_bench FILE THREADS SORTED(0|1)
// SORTED=1 orders the FENs by ChessDB's own key (min of the hexfen and its BW mirror's) and gives
// each thread one contiguous key range, so lookups walk the snapshot in order.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include "cdbdirect.h"
#include "fen2cdb.h"

int main(int argc, char *argv[]) {
  if (argc < 4) { std::cerr << "usage: probe_bench FILE THREADS SORTED\n"; return 2; }
  size_t threads = std::stoul(argv[2]);
  bool sorted = std::string(argv[3]) == "1";
  std::ifstream in(argv[1]);
  std::vector<std::pair<std::string, std::string>> fens;  // (sort key, fen)
  std::string line;
  while (std::getline(in, line)) {
    std::istringstream iss(line); std::string w, fen; int k = 0;
    while (k < 4 && iss >> w) { fen += (k ? " " : "") + w; ++k; }
    if (k == 4) fens.push_back({"", fen});
  }
  auto t0 = std::chrono::steady_clock::now();
  if (sorted) {
    for (auto &p : fens) {
      std::string a = cbfen2hexfen(p.second), b = cbfen2hexfen(cbgetBWfen(p.second));
      p.first = std::min(a, b);
    }
    std::sort(fens.begin(), fens.end());
  }
  auto t1 = std::chrono::steady_clock::now();
  std::uintptr_t h = cdbdirect_initialize(CHESSDB_PATH);
  std::atomic<size_t> known(0), moves(0);
  auto t2 = std::chrono::steady_clock::now();
  std::vector<std::thread> ws;
  size_t per = (fens.size() + threads - 1) / threads;
  for (size_t t = 0; t < threads; ++t)
    ws.emplace_back([&, t] {
      size_t lo = t * per, hi = std::min(fens.size(), lo + per);
      for (size_t i = lo; i < hi; ++i) {
        auto r = cdbdirect_get(h, fens[i].second);
        if (r.back().second > -2) { known++; moves += r.size() - 1; }
      }
    });
  for (auto &w : ws) w.join();
  auto t3 = std::chrono::steady_clock::now();
  auto s = [](auto a, auto b) { return std::chrono::duration<double>(b - a).count(); };
  std::cout << "fens " << fens.size() << " threads " << threads << " sorted " << sorted
            << " | sort " << s(t0, t1) << "s open " << s(t1, t2) << "s probe " << s(t2, t3)
            << "s = " << fens.size() / s(t2, t3) << " /s | known " << known << " moves " << moves << "\n";
  cdbdirect_finalize(h);
}
