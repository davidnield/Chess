// GENERATED from cdb_scan.cpp lines 60-74 and 167-249 (position + key code, verified by keytest). Do not edit.
#pragma once
#include <cctype>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include "z128_keys.h"
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
