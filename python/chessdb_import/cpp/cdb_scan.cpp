// ChessDB full scan for the move-menu import (docs/move-menus-plan-2026-11-21.md, path B).
//
// Point lookups cost ~200 us of CPU each and the export's EPD pass ~90% of its time; one sequential
// scan of the snapshot reads each entry once. Every ChessDB entry holds a position and its colour
// mirror; for each orientation this tool computes OUR 128-bit key (polyglot rules, as python-chess /
// shakmaty: cleaned castling rights, ep hashed when a capture is pseudo-legal) and keeps the
// orientation when the key is in a Bloom filter of the book's keys. Moves, scores and the distance
// come from cdbdirect's own value_to_scoredMoves with the orientation's side to move: exactly what
// cdbdirect_get returns when probed with that FEN.
//
// The lookup probed the book's EPD (ep written only when the capture is LEGAL), so to give each book
// key the entry the lookup would have hit:
//   - a FEN with an ep square and no pseudo-legal capture is dropped (the EPD never has that ep);
//   - a FEN with an ep square and a pseudo-legal capture is flagged 2 (kept downstream iff legal);
//   - a FEN without ep also emits, flagged 1, the key with each pseudo-legal ep square restored
//     (the book key of a position whose EPD dropped an illegal ep; kept downstream iff illegal);
//   - a FEN whose castling field differs from its cleaned rights is dropped (counted).
// Flag 4 marks a deterministic ~1/2048 sample whose key is recomputed downstream from the FEN.
//
//   cdb_scan keytest IN.tsv              IN = lo \t hi \t epd (the importer's export): C++ key vs book key
//   cdb_scan scan KEYS_DIR OUT_DIR THREADS UNITS [FIRST LAST]
//       KEYS_DIR/bkt{000..511}.bin: the book's keys, (lo, hi) int64 pairs
//       OUT_DIR/u{NNNN}.tsv: bkt \t lo \t hi \t ply \t moves \t flags \t fen(if flags)
//       one file per unit (a key range of the snapshot), written .tmp then renamed; a unit whose file
//       exists is skipped, so a rerun resumes. Read errors exit 2. OUT_DIR/u{NNNN}.stats: counts.
#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <sys/stat.h>
#include <thread>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "cdbdirect.h"
#include "fen2cdb.h"
#include "z128_keys.h"

using namespace TERARKDB_NAMESPACE;

// cdbdirect internals (cdbdirect.cpp, external linkage)
enum class STM { WHITE, BLACK, NONE };
enum class MinPlyType { INIT, SINGLE, DUAL, NONE };
struct CDB { DB *db; MinPlyType min_ply_type; };
std::pair<std::string, std::string> key_to_fens(const std::string &key);
std::vector<std::pair<std::string, int>> value_to_scoredMoves(const std::string &value, STM key_stm, STM &fen_stm,
                                                             MinPlyType min_ply_type);

// ── position + key ─────────────────────────────────────────────────────────────
struct Pos {
  int8_t pc[64];          // polyglot kind 2*(type-1)+white, or -1; square = 8*rank + file
  bool white;
  std::string castling;   // FEN field
  int ep;                 // -1 or square
};

static int kind_of(char c) {
  const char *t = "pnbrqk";
  const char *f = std::strchr(t, std::tolower(c));
  if (!f || !*f) return -1;
  return 2 * int(f - t) + (std::isupper(c) ? 1 : 0);
}

// cdbdirect's key_to_fens with its stringstream bin2hex replaced by a table (same lowercase output)
static std::pair<std::string, std::string> fast_key_to_fens(const std::string &key) {
  static const char *d = "0123456789abcdef";
  std::string hex(2 * (key.size() - 1), '0');
  for (size_t i = 1; i < key.size(); ++i) {
    unsigned char c = key[i];
    hex[2 * (i - 1)] = d[c >> 4];
    hex[2 * (i - 1) + 1] = d[c & 15];
  }
  std::string fen = cbhexfen2fen(hex);
  return {fen, cbgetBWfen(fen)};
}

// Direct decode of a ChessDB key into the position cbhexfen2fen would print (fen2cdb.cpp's nibble code:
// squares in FEN order, '0'/'1'/'2' = 1/2/3 empties, '8' x = x+4 empties, pieces 3-7,9 black and a-f
// white; then side to move, castling letters until '9' (space; 'e' escapes an X-FEN file letter), then
// the ep file and its raw rank digit). Returns false on anything unusual; the caller falls back to the
// string path, and samples are checked against it.
static bool fast_decode(const std::string &key, Pos &p) {
  static const char *hexd = "0123456789abcdef";
  const size_t N = 2 * (key.size() - 1);
  auto nib = [&](size_t i) -> int { return (uint8_t(key[1 + i / 2]) >> (i % 2 ? 0 : 4)) & 15; };
  static const int8_t KIND[16] = {-1, -1, -1, 0, 2, 4, 6, 8, -1, 10, 1, 3, 5, 7, 9, 11};
  std::memset(p.pc, -1, sizeof p.pc);
  size_t ni = 0;
  for (int sq = 0; sq < 64; ++sq) {
    int t = ni < N ? nib(ni++) : 0;
    if (t == 1) sq += 1;
    else if (t == 2) sq += 2;
    else if (t == 8) {
      if (ni >= N) return false;
      int t2 = nib(ni++);
      if (t2 > 4) return false;
      sq += t2 + 3;
    } else if (t != 0) {
      if (KIND[t] < 0) return false;
      if (sq > 63) return false;
      p.pc[8 * (7 - sq / 8) + sq % 8] = KIND[t];
    }
    if (sq > 63) return false;
  }
  if (ni >= N) return false;
  p.white = nib(ni++) == 0;
  p.castling.clear();
  for (;;) {
    if (ni >= N) return false;
    char c = hexd[nib(ni++)];
    char e;
    if (c == 'e') {
      if (ni >= N) return false;
      char c2 = hexd[nib(ni++)];
      e = (c2 >= '1' && c2 <= '8') ? char(std::toupper('a' + (c2 - '1'))) : 0;
      if (!e) return false;
    } else if (c == '0') e = '-';
    else if (c >= 'a' && c <= 'd') e = "KQkq"[c - 'a'];
    else if (c >= '1' && c <= '8') e = char('a' + (c - '1'));
    else if (c == '9') break;
    else return false;
    p.castling += e;
  }
  if (p.castling.empty()) return false;
  p.ep = -1;
  if (ni < N) {
    char c = hexd[nib(ni++)];
    if (c != '0') {
      if (c < '1' || c > '8' || ni >= N) return false;
      char r = hexd[nib(ni)];
      if (r < '1' || r > '8') return false;
      p.ep = 8 * (r - '1') + (c - '1');
    }
  }
  return true;
}

// cbgetBWfen on a decoded position: ranks reversed, colours swapped, side to move flipped, castling
// letters swapped in case (black's first, as cbgetBWfen writes them), ep rank mirrored
static void mirror(const Pos &a, Pos &b) {
  for (int sq = 0; sq < 64; ++sq) b.pc[sq ^ 56] = a.pc[sq] < 0 ? -1 : int8_t(a.pc[sq] ^ 1);
  b.white = !a.white;
  std::string lowers, uppers;
  for (char c : a.castling) {
    if (std::isupper(c)) uppers += char(std::tolower(c));
    else lowers += (c == '-' ? '-' : char(std::toupper(c)));
  }
  b.castling = lowers + uppers;
  b.ep = a.ep < 0 ? -1 : (a.ep ^ 56);
}

static bool same_pos(const Pos &a, const Pos &b) {
  return std::memcmp(a.pc, b.pc, 64) == 0 && a.white == b.white && a.castling == b.castling && a.ep == b.ep;
}

static bool parse_fen(const std::string &fen, Pos &p) {
  std::string f[4] = {"", "", "-", "-"};
  size_t n = 0, i = 0;
  while (i < fen.size() && n < 4) {
    while (i < fen.size() && fen[i] == ' ') ++i;
    size_t j = i;
    while (j < fen.size() && fen[j] != ' ') ++j;
    if (j > i) f[n++] = fen.substr(i, j - i);
    i = j;
  }
  if (n < 2) return false;
  const std::string &board = f[0], &stm = f[1], &cas = f[2], &ep = f[3];
  std::memset(p.pc, -1, sizeof p.pc);
  int rank = 7, file = 0;
  for (char c : board) {
    if (c == '/') { rank--; file = 0; continue; }
    if (c >= '1' && c <= '8') { file += c - '0'; continue; }
    int k = kind_of(c);
    if (k < 0 || rank < 0 || file > 7) return false;
    p.pc[8 * rank + file] = k;
    file++;
  }
  if (rank != 0) return false;
  if (stm != "w" && stm != "b") return false;
  p.white = stm == "w";
  p.castling = cas;
  p.ep = -1;
  if (ep != "-") {
    if (ep.size() != 2 || ep[0] < 'a' || ep[0] > 'h' || ep[1] < '1' || ep[1] > '8') return false;
    p.ep = 8 * (ep[1] - '1') + (ep[0] - 'a');
  }
  return true;
}

enum { BP = 0, WP = 1, BR = 6, WR = 7, BK = 10, WK = 11 };

// cleaned castling rights as a FEN field (python-chess clean_castling_rights, standard chess)
static std::string cleaned_castling(const Pos &p) {
  std::string s;
  bool wk = p.pc[4] == WK, bk = p.pc[60] == BK;
  auto has = [&](char c) { return p.castling.find(c) != std::string::npos; };
  if (has('K') && wk && p.pc[7] == WR) s += 'K';
  if (has('Q') && wk && p.pc[0] == WR) s += 'Q';
  if (has('k') && bk && p.pc[63] == BR) s += 'k';
  if (has('q') && bk && p.pc[56] == BR) s += 'q';
  return s.empty() ? "-" : s;
}

// a side-to-move pawn could capture onto ep square sq (polyglot's rule; legality irrelevant)
static bool ep_pseudo(const Pos &p, int sq) {
  int f = sq & 7, from_rank = p.white ? 4 : 3, pawn = p.white ? WP : BP;
  if ((sq >> 3) != (p.white ? 5 : 2)) return false;
  for (int df : {-1, 1}) {
    int ff = f + df;
    if (ff >= 0 && ff < 8 && p.pc[8 * from_rank + ff] == pawn) return true;
  }
  return false;
}

static void key_of(const Pos &p, uint64_t &lo, uint64_t &hi) {
  lo = hi = 0;
  for (int sq = 0; sq < 64; ++sq)
    if (p.pc[sq] >= 0) { int i = 64 * p.pc[sq] + sq; lo ^= Z_LO[i]; hi ^= Z_HI[i]; }
  std::string c = cleaned_castling(p);
  const char *cc = "KQkq";
  for (int i = 0; i < 4; ++i)
    if (c.find(cc[i]) != std::string::npos) { lo ^= Z_LO[768 + i]; hi ^= Z_HI[768 + i]; }
  if (p.ep >= 0 && ep_pseudo(p, p.ep)) { lo ^= Z_LO[772 + (p.ep & 7)]; hi ^= Z_HI[772 + (p.ep & 7)]; }
  if (p.white) { lo ^= Z_LO[780]; hi ^= Z_HI[780]; }
}

// ep squares a just-made double push could have left, with a pseudo-legal capture (p has no ep)
static std::vector<int> ep_candidates(const Pos &p) {
  std::vector<int> out;
  int them = p.white ? BP : WP;
  int r_pawn = p.white ? 4 : 3, r_ep = p.white ? 5 : 2, r_from = p.white ? 6 : 1;
  for (int f = 0; f < 8; ++f) {
    if (p.pc[8 * r_pawn + f] != them || p.pc[8 * r_ep + f] >= 0 || p.pc[8 * r_from + f] >= 0) continue;
    if (ep_pseudo(p, 8 * r_ep + f)) out.push_back(8 * r_ep + f);
  }
  return out;
}

// ── keytest ────────────────────────────────────────────────────────────────────
static int keytest(const std::string &path) {
  std::ifstream in(path);
  if (!in) { std::cerr << "cannot open " << path << "\n"; return 2; }
  std::string line;
  size_t n = 0, ok = 0, lossy = 0, bad = 0, unparsed = 0;
  while (std::getline(in, line)) {
    if (!line.empty() && line.back() == '\r') line.pop_back();
    size_t a = line.find('\t'), b = line.find('\t', a + 1);
    int64_t lo = std::stoll(line.substr(0, a)), hi = std::stoll(line.substr(a + 1, b - a - 1));
    std::string epd = line.substr(b + 1);
    ++n;
    Pos p;
    if (!parse_fen(epd, p)) { ++unparsed; continue; }
    uint64_t l, h;
    key_of(p, l, h);
    if (int64_t(l) == lo && int64_t(h) == hi) { ++ok; continue; }
    bool found = false;
    if (p.ep < 0)
      for (int sq : ep_candidates(p)) {
        uint64_t l2 = l ^ Z_LO[772 + (sq & 7)], h2 = h ^ Z_HI[772 + (sq & 7)];
        if (int64_t(l2) == lo && int64_t(h2) == hi) found = true;
      }
    if (found) { ++lossy; continue; }
    if (bad++ < 5) std::cerr << "BAD " << line << "\n";
  }
  std::cout << "rows " << n << " ok " << ok << " ep_lossy " << lossy << " bad " << bad << " unparsed " << unparsed
            << "\n";
  return bad || unparsed ? 1 : 0;
}

// ── Bloom filter over the book's keys ──────────────────────────────────────────
static const int BLOOM_LOG2_BLOCKS = 27;           // 2^27 blocks x 512 bits = 8 GiB
static uint64_t *bloom = nullptr;

static inline uint64_t mix(uint64_t x) {
  x ^= x >> 30; x *= 0xbf58476d1ce4e5b9ULL; x ^= x >> 27; x *= 0x94d049bb133111ebULL; x ^= x >> 31;
  return x;
}
static inline void bloom_pos(uint64_t lo, uint64_t hi, uint64_t &block, uint16_t bit[8]) {
  block = hi >> (64 - BLOOM_LOG2_BLOCKS);
  uint64_t m = mix(lo ^ (hi * 0x9e3779b97f4a7c15ULL));
  for (int i = 0; i < 7; ++i) bit[i] = (m >> (9 * i)) & 511;
  bit[7] = hi & 511;
}
static inline void bloom_add(uint64_t lo, uint64_t hi) {
  uint64_t block; uint16_t bit[8];
  bloom_pos(lo, hi, block, bit);
  uint64_t *w = bloom + block * 8;
  for (int i = 0; i < 8; ++i) __atomic_fetch_or(&w[bit[i] >> 6], 1ULL << (bit[i] & 63), __ATOMIC_RELAXED);
}
static inline bool bloom_has(uint64_t lo, uint64_t hi) {
  uint64_t block; uint16_t bit[8];
  bloom_pos(lo, hi, block, bit);
  const uint64_t *w = bloom + block * 8;
  for (int i = 0; i < 8; ++i)
    if (!(w[bit[i] >> 6] & (1ULL << (bit[i] & 63)))) return false;
  return true;
}

static bool exists(const std::string &p) { struct stat st; return stat(p.c_str(), &st) == 0; }

static size_t load_bloom(const std::string &keys_dir, int threads) {
  size_t bytes = (size_t(1) << BLOOM_LOG2_BLOCKS) * 64;
  bloom = static_cast<uint64_t *>(std::aligned_alloc(64, bytes));
  if (!bloom) { std::cerr << "bloom alloc failed\n"; std::exit(2); }
  std::memset(bloom, 0, bytes);
  std::atomic<int> next(0);
  std::atomic<size_t> total(0);
  std::vector<std::thread> ws;
  for (int t = 0; t < threads; ++t)
    ws.emplace_back([&] {
      std::vector<int64_t> buf(1 << 20);
      for (int b; (b = next.fetch_add(1)) < 512;) {
        char name[64]; std::snprintf(name, sizeof name, "/bkt%03d.bin", b);
        FILE *f = std::fopen((keys_dir + name).c_str(), "rb");
        if (!f && std::getenv("CDB_SCAN_PARTIAL_KEYS")) continue;      // tests only: a subset of buckets
        if (!f) { std::cerr << "missing keys " << keys_dir << name << "\n"; std::exit(2); }
        size_t got;
        while ((got = std::fread(buf.data(), 16, buf.size() / 2, f)) > 0) {
          for (size_t i = 0; i < got; ++i) bloom_add(uint64_t(buf[2 * i]), uint64_t(buf[2 * i + 1]));
          total += got;
        }
        std::fclose(f);
      }
    });
  for (auto &w : ws) w.join();
  return total;
}

// ── scan ───────────────────────────────────────────────────────────────────────
struct Unit { std::string start, limit; };

static std::vector<Unit> make_units(DB *db, size_t units) {
  std::vector<LiveFileMetaData> files;
  db->GetLiveFilesMetaData(&files);
  const Comparator *cmp = db->GetOptions().comparator;
  std::sort(files.begin(), files.end(), [&](const LiveFileMetaData &a, const LiveFileMetaData &b) {
    return cmp->Compare(a.smallestkey, b.smallestkey) < 0;
  });
  ReadOptions ro;
  std::unique_ptr<Iterator> it(db->NewIterator(ro));
  it->SeekToLast();
  std::string last = it->key().ToString() + '\xff';
  uint64_t total = 0;
  for (auto &f : files) total += f.size;
  std::vector<Unit> out;
  uint64_t acc = 0, target = total / units + 1;
  std::string start = files[0].smallestkey;
  for (size_t i = 0; i < files.size(); ++i) {
    acc += files[i].size;
    bool lastfile = i + 1 == files.size();
    if (lastfile || (acc >= target && cmp->Compare(files[i + 1].smallestkey, start) > 0)) {
      std::string limit = lastfile ? last : files[i + 1].smallestkey;
      out.push_back({start, limit});
      start = limit;
      acc = 0;
    }
  }
  std::cerr << files.size() << " SST files, " << total / (1 << 30) << " GiB, " << out.size() << " units\n";
  return out;
}

static std::atomic<size_t> g_entries(0);

static void scan_unit(CDB *cdb, const Unit &u, const std::string &path, const std::string &stats_path) {
  std::string tmp = path + ".tmp";
  FILE *f = std::fopen(tmp.c_str(), "wb");
  if (!f) { std::cerr << "cannot write " << tmp << "\n"; std::exit(2); }
  std::string buf;
  buf.reserve(16 << 20);
  size_t entries = 0, kept = 0, variants = 0, epcheck = 0, ep_dropped = 0, cas_dropped = 0, unparsed = 0, slow = 0;
  const Comparator *cmp = cdb->db->GetOptions().comparator;
  ReadOptions ro;
  ro.verify_checksums = true;
  ro.fill_cache = false;
  std::unique_ptr<Iterator> it(cdb->db->NewIterator(ro));
  char num[96];
  const char *cap_env = std::getenv("CDB_SCAN_MAX_ENTRIES");    // tests only: stop each unit early
  size_t cap = cap_env ? std::stoull(cap_env) : ~size_t(0);
  const bool noop = std::getenv("CDB_SCAN_NOOP") != nullptr;
  for (it->Seek(u.start); it->Valid() && cmp->Compare(it->key(), u.limit) < 0 && entries < cap; it->Next()) {
    ++entries;
    if ((entries & 0xFFFF) == 0) g_entries += 0x10000;
    if (noop) continue;                                        // tests only: iteration cost alone
    std::string key = it->key().ToString();
    if (key.size() < 2 || key[0] != 'h') continue;           // not a position entry
    // both orientations, decoded straight from the key; the string path (cdbdirect's FENs) is the
    // fallback and the check: first 2000 entries of a unit and every 4096th
    Pos P[2];
    bool fast = fast_decode(key, P[0]);
    if (fast) mirror(P[0], P[1]);
    std::pair<std::string, std::string> fens;
    bool have_fens = false;
    auto get_fens = [&]() {
      if (!have_fens) { fens = fast_key_to_fens(key); have_fens = true; }
      return fens;
    };
    if (!fast || entries <= 2000 || (entries & 4095) == 0) {
      get_fens();
      if (entries <= 2000 && fens != key_to_fens(key)) {
        std::cerr << "fast_key_to_fens differs from cdbdirect's key_to_fens: " << fens.first << "\n";
        std::exit(2);
      }
      Pos S[2];
      bool ok0 = parse_fen(fens.first, S[0]), ok1 = parse_fen(fens.second, S[1]);
      if (!ok0 || !ok1) { ++unparsed; continue; }
      if (fast && !(same_pos(P[0], S[0]) && same_pos(P[1], S[1]))) {
        std::cerr << "fast_decode differs from cdbdirect: [" << fens.first << "] / [" << fens.second << "]\n";
        std::exit(2);
      }
      if (!fast) { P[0] = S[0]; P[1] = S[1]; ++slow; }
    }
    STM key_stm = P[0].white ? STM::WHITE : STM::BLACK;
    std::string value;                                         // decoded lazily, once per orientation
    for (int o = 0; o < 2; ++o) {
      const Pos &p = P[o];
      if (cleaned_castling(p) != (p.castling.empty() ? "-" : p.castling)) {
        if (std::getenv("CDB_SCAN_DEBUG") && cas_dropped < 8)
          std::cerr << "castling drop: [" << (o ? get_fens().second : get_fens().first) << "] cleaned ["
                    << cleaned_castling(p) << "]\n";
        ++cas_dropped;
        continue;
      }
      uint64_t lo, hi;
      key_of(p, lo, hi);
      int base_flag = 0;
      if (p.ep >= 0) {
        if (!ep_pseudo(p, p.ep)) { ++ep_dropped; continue; }
        base_flag = 2;
      }
      // (key, flag) candidates for this orientation
      std::pair<std::pair<uint64_t, uint64_t>, int> cands[9];
      int nc = 0;
      cands[nc++] = {{lo, hi}, base_flag};
      if (p.ep < 0)
        for (int sq : ep_candidates(p))
          cands[nc++] = {{lo ^ Z_LO[772 + (sq & 7)], hi ^ Z_HI[772 + (sq & 7)]}, 1};
      std::vector<std::pair<std::string, int>> scored;
      bool decoded = false;
      for (int c = 0; c < nc; ++c) {
        uint64_t l = cands[c].first.first, h = cands[c].first.second;
        if (!bloom_has(l, h)) continue;
        if (!decoded) {
          if (value.empty()) value = it->value().ToString();
          STM fen_stm = p.white ? STM::WHITE : STM::BLACK;
          scored = value_to_scoredMoves(value, key_stm, fen_stm, cdb->min_ply_type);
          decoded = true;
        }
        int flags = cands[c].second | ((mix(l) & 2047) == 0 ? 4 : 0);
        std::snprintf(num, sizeof num, "%u\t%lld\t%lld\t%d\t", unsigned(l & 511), (long long)int64_t(l),
                      (long long)int64_t(h), scored.back().second);
        buf += num;
        for (size_t k = 0; k + 1 < scored.size(); ++k) {
          if (k) buf += ';';
          buf += scored[k].first;
          buf += ':';
          buf += std::to_string(scored[k].second);
        }
        buf += '\t';
        buf += std::to_string(flags);
        buf += '\t';
        if (flags) buf += o ? get_fens().second : get_fens().first;
        buf += '\n';
        ++kept;
        variants += cands[c].second == 1;
        epcheck += cands[c].second == 2;
      }
    }
    if (buf.size() > (12 << 20)) {
      if (std::fwrite(buf.data(), 1, buf.size(), f) != buf.size()) { std::cerr << "write failed\n"; std::exit(2); }
      buf.clear();
    }
  }
  if (!it->status().ok()) { std::cerr << "snapshot read failed: " << it->status().ToString() << "\n"; std::exit(2); }
  if (!buf.empty() && std::fwrite(buf.data(), 1, buf.size(), f) != buf.size()) { std::cerr << "write failed\n"; std::exit(2); }
  if (std::fclose(f) != 0) { std::cerr << "close failed\n"; std::exit(2); }
  {
    std::ofstream s(stats_path + ".tmp");
    s << "entries " << entries << "\nkept " << kept << "\nvariants " << variants << "\nepcheck " << epcheck
      << "\nep_dropped " << ep_dropped << "\ncastling_dropped " << cas_dropped << "\nunparsed " << unparsed
      << "\nslow_decodes " << slow << "\n";
    s.close();
    if (!s) { std::cerr << "stats write failed\n"; std::exit(2); }
    std::rename((stats_path + ".tmp").c_str(), stats_path.c_str());
  }
  if (unparsed) { std::cerr << "unit has " << unparsed << " unparsed FENs\n"; std::exit(2); }
  if (std::rename(tmp.c_str(), path.c_str()) != 0) { std::cerr << "rename failed\n"; std::exit(2); }
}

static int scan(const std::string &keys_dir, const std::string &out_dir, int threads, size_t units, int first,
                int last) {
  std::uintptr_t h = cdbdirect_initialize(CHESSDB_PATH);
  CDB *cdb = reinterpret_cast<CDB *>(h);
  auto us = make_units(cdb->db, units);
  {
    std::ofstream s(out_dir + "/_units.tmp");
    s << us.size() << "\n";
    s.close();
    if (!s) { std::cerr << "cannot write _units\n"; std::exit(2); }
    std::rename((out_dir + "/_units.tmp").c_str(), (out_dir + "/_units").c_str());
  }
  if (last < 0 || last >= int(us.size())) last = int(us.size()) - 1;
  std::vector<int> todo;
  for (int i = first; i <= last; ++i) {
    char name[64]; std::snprintf(name, sizeof name, "/u%04d.tsv", i);
    if (!exists(out_dir + name)) todo.push_back(i);
  }
  std::cerr << todo.size() << " units to do of " << (last - first + 1) << "\n";
  if (todo.empty()) return 0;
  auto t0 = std::chrono::steady_clock::now();
  size_t nkeys = load_bloom(keys_dir, threads);
  std::cerr << "bloom: " << nkeys << " keys in "
            << std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count() << " s\n";
  std::atomic<size_t> next(0), done(0);
  std::mutex log_mu;
  auto t1 = std::chrono::steady_clock::now();
  std::vector<std::thread> ws;
  for (int t = 0; t < threads; ++t)
    ws.emplace_back([&] {
      for (size_t j; (j = next.fetch_add(1)) < todo.size();) {
        int i = todo[j];
        char name[64]; std::snprintf(name, sizeof name, "/u%04d", i);
        scan_unit(cdb, us[i], out_dir + name + ".tsv", out_dir + name + ".stats");
        size_t d = ++done;
        double s = std::chrono::duration<double>(std::chrono::steady_clock::now() - t1).count();
        std::lock_guard<std::mutex> g(log_mu);
        std::cout << "unit " << i << " done (" << d << "/" << todo.size() << "), " << g_entries.load() / 1e6
                  << "M entries in " << int(s) << " s" << std::endl;
      }
    });
  for (auto &w : ws) w.join();
  cdbdirect_finalize(h);
  return 0;
}

int main(int argc, char *argv[]) {
  std::string mode = argc > 1 ? argv[1] : "";
  if (mode == "keytest" && argc == 3) return keytest(argv[2]);
  if (mode == "scan" && (argc == 6 || argc == 8))
    return scan(argv[2], argv[3], std::stoi(argv[4]), std::stoul(argv[5]), argc == 8 ? std::stoi(argv[6]) : 0,
                argc == 8 ? std::stoi(argv[7]) : -1);
  std::cerr << "usage: cdb_scan keytest IN.tsv | cdb_scan scan KEYS_DIR OUT_DIR THREADS UNITS [FIRST LAST]\n";
  return 2;
}
