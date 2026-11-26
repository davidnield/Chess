// Compile against the pinned, unmodified fen2cdb.cpp, independently of Rust.
#include <iostream>
#include <string>
#include "fen2cdb.h"
int main() {
  std::string fen;
  while (std::getline(std::cin, fen)) {
    if (!fen.empty() && fen.back() == '\r') fen.pop_back();
    auto a = cbfen2hexfen(fen), b = cbfen2hexfen(cbgetBWfen(fen));
    std::cout << "68" << (a < b ? a : b) << '\t' << (a >= b) << '\n';
  }
}
