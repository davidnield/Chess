// Our 128-bit key for FENs (the Lichess cloud PV import; docs/move-menus-plan-2026-11-21.md, step 3).
// Key code shared with cdb_scan through z128_pos.h (generated from it; verified by keytest).
//
//   fen_keys keytest IN.tsv     IN = lo \t hi \t epd (a book export): our key vs the book's key
//   fen_keys fenkeys IN OUT     IN = one FEN per line (>= 4 fields; move counters ignored)
//       OUT = fen \t lo \t hi \t flag, one line per input line, written OUT.tmp then renamed:
//         0 plain; 3 ep square without a pseudo-legal capture (the key ignores it, as polyglot does);
//         1 no ep square but a pseudo-legal one is possible (a double push just happened?): python decides
//           whether the position with it restored is the same position (capture illegal);
//         2 ep square with a pseudo-legal capture: python decides whether it is legal;
//         8 castling field differs from the cleaned rights (not a standard-chess position: dropped);
//         9 unparseable (dropped).
#include <cstdio>
#include <fstream>
#include <iostream>
#include "z128_pos.h"

static int keytest(const char *path) {
  std::ifstream in(path);
  if (!in) { std::cerr << "cannot open " << path << "\n"; return 2; }
  std::string line;
  size_t n = 0, ok = 0, lossy = 0, bad = 0;
  while (std::getline(in, line)) {
    if (!line.empty() && line.back() == '\r') line.pop_back();
    size_t a = line.find('\t'), b = line.find('\t', a + 1);
    int64_t lo = std::stoll(line.substr(0, a)), hi = std::stoll(line.substr(a + 1, b - a - 1));
    Pos p;
    ++n;
    if (!parse_fen(line.substr(b + 1), p)) { ++bad; continue; }
    uint64_t l, h;
    key_of(p, l, h);
    if (int64_t(l) == lo && int64_t(h) == hi) { ++ok; continue; }
    bool found = false;
    if (p.ep < 0)
      for (int sq : ep_candidates(p))
        if (int64_t(l ^ Z_LO[772 + (sq & 7)]) == lo && int64_t(h ^ Z_HI[772 + (sq & 7)]) == hi) found = true;
    if (found) ++lossy; else if (bad++ < 5) std::cerr << "BAD " << line << "\n";
  }
  std::cout << "rows " << n << " ok " << ok << " ep_lossy " << lossy << " bad " << bad << "\n";
  return bad ? 1 : 0;
}

static int fenkeys(const char *in_path, const std::string &out_path) {
  std::ifstream in(in_path);
  if (!in) { std::cerr << "cannot open " << in_path << "\n"; return 2; }
  std::string tmp = out_path + ".tmp";
  FILE *f = std::fopen(tmp.c_str(), "wb");
  if (!f) { std::cerr << "cannot write " << tmp << "\n"; return 2; }
  std::string line, buf;
  size_t n = 0, cnt[10] = {0};
  char num[80];
  while (std::getline(in, line)) {
    if (!line.empty() && line.back() == '\r') line.pop_back();
    ++n;
    Pos p;
    int flag;
    uint64_t lo = 0, hi = 0;
    if (!parse_fen(line, p)) flag = 9;
    else if (cleaned_castling(p) != p.castling) flag = 8;
    else {
      key_of(p, lo, hi);
      if (p.ep >= 0) flag = ep_pseudo(p, p.ep) ? 2 : 3;
      else flag = ep_candidates(p).empty() ? 0 : 1;
    }
    cnt[flag]++;
    std::snprintf(num, sizeof num, "\t%lld\t%lld\t%d\n", (long long)int64_t(lo), (long long)int64_t(hi), flag);
    buf += line;
    buf += num;
    if (buf.size() > (8 << 20)) { std::fwrite(buf.data(), 1, buf.size(), f); buf.clear(); }
  }
  if (!buf.empty() && std::fwrite(buf.data(), 1, buf.size(), f) != buf.size()) { std::cerr << "write failed\n"; return 2; }
  if (std::fclose(f) != 0 || std::rename(tmp.c_str(), out_path.c_str()) != 0) { std::cerr << "close/rename failed\n"; return 2; }
  std::cout << "fens " << n;
  for (int i : {0, 1, 2, 3, 8, 9}) std::cout << " flag" << i << " " << cnt[i];
  std::cout << "\n";
  return 0;
}

int main(int argc, char *argv[]) {
  std::string mode = argc > 1 ? argv[1] : "";
  if (mode == "keytest" && argc == 3) return keytest(argv[2]);
  if (mode == "fenkeys" && argc == 4) return fenkeys(argv[2], argv[3]);
  std::cerr << "usage: fen_keys keytest IN.tsv | fen_keys fenkeys IN OUT\n";
  return 2;
}
