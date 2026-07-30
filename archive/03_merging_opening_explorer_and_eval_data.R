library(conflicted)
library(tidyverse)
library(arrow)

conflicts_prefer(dplyr::filter)
conflicts_prefer(dplyr::collect)
conflicts_prefer(dplyr::mutate)

source_path <- "D:/data/chess/opening-explorer/"

# --- Configuration ---
filter_event        <- "Rapid"
filter_year         <- 2022
filter_month        <- 1
filter_rating_class <- "2000_2200"

df <- open_dataset(source_path, format = "parquet") |>
  filter(
    event        == filter_event,
    year         == filter_year,
    month        == filter_month,
    rating_class == filter_rating_class
  ) |>
  collect()

tic(msg = "Whole thing")

test <- df %>%
  group_by(W1:B10) %>%
  summarise(b10_white_wins = sum(white_wins),
            b10_draws      = sum(draws),
            b10_black_wins = sum(black_wins),
            b10_total_games = sum(total_games)) %>%
  group_by(W1:W10) %>%
  mutate(w10_white_wins  = sum(b10_white_wins),
         w10_draws       = sum(b10_draws),
         w10_black_wins  = sum(b10_black_wins),
         w10_total_games = sum(b10_total_games)) %>%
  group_by(W1:B9) %>%
  mutate(b9_white_wins  = sum(b10_white_wins),
         b9_draws       = sum(b10_draws),
         b9_black_wins  = sum(b10_black_wins),
         b9_total_games = sum(b10_total_games)) %>%
  group_by(W1:W9) %>%
  mutate(w9_white_wins  = sum(b10_white_wins),
         w9_draws       = sum(b10_draws),
         w9_black_wins  = sum(b10_black_wins),
         w9_total_games = sum(b10_total_games)) %>%
  group_by(W1:B8) %>%
  mutate(b8_white_wins  = sum(b10_white_wins),
         b8_draws       = sum(b10_draws),
         b8_black_wins  = sum(b10_black_wins),
         b8_total_games = sum(b10_total_games)) %>%
  group_by(W1:W8) %>%
  mutate(w8_white_wins  = sum(b10_white_wins),
         w8_draws       = sum(b10_draws),
         w8_black_wins  = sum(b10_black_wins),
         w8_total_games = sum(b10_total_games)) %>%
  group_by(W1:B7) %>%
  mutate(b7_white_wins  = sum(b10_white_wins),
         b7_draws       = sum(b10_draws),
         b7_black_wins  = sum(b10_black_wins),
         b7_total_games = sum(b10_total_games)) %>%
  group_by(W1:W7) %>%
  mutate(w7_white_wins  = sum(b10_white_wins),
         w7_draws       = sum(b10_draws),
         w7_black_wins  = sum(b10_black_wins),
         w7_total_games = sum(b10_total_games)) %>%
  group_by(W1:B6) %>%
  mutate(b6_white_wins  = sum(b10_white_wins),
         b6_draws       = sum(b10_draws),
         b6_black_wins  = sum(b10_black_wins),
         b6_total_games = sum(b10_total_games)) %>%
  group_by(W1:W6) %>%
  mutate(w6_white_wins  = sum(b10_white_wins),
         w6_draws       = sum(b10_draws),
         w6_black_wins  = sum(b10_black_wins),
         w6_total_games = sum(b10_total_games)) %>%
  group_by(W1:B5) %>%
  mutate(b5_white_wins  = sum(b10_white_wins),
         b5_draws       = sum(b10_draws),
         b5_black_wins  = sum(b10_black_wins),
         b5_total_games = sum(b10_total_games)) %>%
  group_by(W1:W5) %>%
  mutate(w5_white_wins  = sum(b10_white_wins),
         w5_draws       = sum(b10_draws),
         w5_black_wins  = sum(b10_black_wins),
         w5_total_games = sum(b10_total_games)) %>%
  group_by(W1:B4) %>%
  mutate(b4_white_wins  = sum(b10_white_wins),
         b4_draws       = sum(b10_draws),
         b4_black_wins  = sum(b10_black_wins),
         b4_total_games = sum(b10_total_games)) %>%
  group_by(W1:W4) %>%
  mutate(w4_white_wins  = sum(b10_white_wins),
         w4_draws       = sum(b10_draws),
         w4_black_wins  = sum(b10_black_wins),
         w4_total_games = sum(b10_total_games)) %>%
  group_by(W1:B3) %>%
  mutate(b3_white_wins  = sum(b10_white_wins),
         b3_draws       = sum(b10_draws),
         b3_black_wins  = sum(b10_black_wins),
         b3_total_games = sum(b10_total_games)) %>%
  group_by(W1:W3) %>%
  mutate(w3_white_wins  = sum(b10_white_wins),
         w3_draws       = sum(b10_draws),
         w3_black_wins  = sum(b10_black_wins),
         w3_total_games = sum(b10_total_games)) %>%
  group_by(W1:B2) %>%
  mutate(b2_white_wins  = sum(b10_white_wins),
         b2_draws       = sum(b10_draws),
         b2_black_wins  = sum(b10_black_wins),
         b2_total_games = sum(b10_total_games)) %>%
  group_by(W1:W2) %>%
  mutate(w2_white_wins  = sum(b10_white_wins),
         w2_draws       = sum(b10_draws),
         w2_black_wins  = sum(b10_black_wins),
         w2_total_games = sum(b10_total_games)) %>%
  group_by(W1:B1) %>%
  mutate(b1_white_wins  = sum(b10_white_wins),
         b1_draws       = sum(b10_draws),
         b1_black_wins  = sum(b10_black_wins),
         b1_total_games = sum(b10_total_games)) %>%
  group_by(W1) %>%
  mutate(w1_white_wins  = sum(b10_white_wins),
         w1_draws       = sum(b10_draws),
         w1_black_wins  = sum(b10_black_wins),
         w1_total_games = sum(b10_total_games)) %>%
  ungroup() %>%
  mutate(
    b10_move_pct = b10_total_games / w10_total_games,
    w10_move_pct = w10_total_games / b9_total_games,
    b9_move_pct  = b9_total_games  / w9_total_games,
    w9_move_pct  = w9_total_games  / b8_total_games,
    b8_move_pct  = b8_total_games  / w8_total_games,
    w8_move_pct  = w8_total_games  / b7_total_games,
    b7_move_pct  = b7_total_games  / w7_total_games,
    w7_move_pct  = w7_total_games  / b6_total_games,
    b6_move_pct  = b6_total_games  / w6_total_games,
    w6_move_pct  = w6_total_games  / b5_total_games,
    b5_move_pct  = b5_total_games  / w5_total_games,
    w5_move_pct  = w5_total_games  / b4_total_games,
    b4_move_pct  = b4_total_games  / w4_total_games,
    w4_move_pct  = w4_total_games  / b3_total_games,
    b3_move_pct  = b3_total_games  / w3_total_games,
    w3_move_pct  = w3_total_games  / b2_total_games,
    b2_move_pct  = b2_total_games  / w2_total_games,
    w2_move_pct  = w2_total_games  / b1_total_games,
    b1_move_pct  = b1_total_games  / w1_total_games,
    w1_move_pct  = w1_total_games  / sum(b10_total_games),
    b10_line_pct = b10_move_pct * b9_move_pct  * b8_move_pct  * b7_move_pct  * b6_move_pct  *
                   b5_move_pct  * b4_move_pct  * b3_move_pct  * b2_move_pct  * b1_move_pct,
    w10_line_pct = w10_move_pct * w9_move_pct  * w8_move_pct  * w7_move_pct  * w6_move_pct  *
                   w5_move_pct  * w4_move_pct  * w3_move_pct  * w2_move_pct  * w1_move_pct,
    b9_line_pct  = b9_move_pct  * b8_move_pct  * b7_move_pct  * b6_move_pct  * b5_move_pct  *
                   b4_move_pct  * b3_move_pct  * b2_move_pct  * b1_move_pct,
    w9_line_pct  = w9_move_pct  * w8_move_pct  * w7_move_pct  * w6_move_pct  * w5_move_pct  *
                   w4_move_pct  * w3_move_pct  * w2_move_pct  * w1_move_pct,
    b8_line_pct  = b8_move_pct  * b7_move_pct  * b6_move_pct  * b5_move_pct  * b4_move_pct  *
                   b3_move_pct  * b2_move_pct  * b1_move_pct,
    w8_line_pct  = w8_move_pct  * w7_move_pct  * w6_move_pct  * w5_move_pct  * w4_move_pct  *
                   w3_move_pct  * w2_move_pct  * w1_move_pct,
    b7_line_pct  = b7_move_pct  * b6_move_pct  * b5_move_pct  * b4_move_pct  * b3_move_pct  *
                   b2_move_pct  * b1_move_pct,
    w7_line_pct  = w7_move_pct  * w6_move_pct  * w5_move_pct  * w4_move_pct  * w3_move_pct  *
                   w2_move_pct  * w1_move_pct,
    b6_line_pct  = b6_move_pct  * b5_move_pct  * b4_move_pct  * b3_move_pct  * b2_move_pct  * b1_move_pct,
    w6_line_pct  = w6_move_pct  * w5_move_pct  * w4_move_pct  * w3_move_pct  * w2_move_pct  * w1_move_pct,
    b5_line_pct  = b5_move_pct  * b4_move_pct  * b3_move_pct  * b2_move_pct  * b1_move_pct,
    w5_line_pct  = w5_move_pct  * w4_move_pct  * w3_move_pct  * w2_move_pct  * w1_move_pct,
    b4_line_pct  = b4_move_pct  * b3_move_pct  * b2_move_pct  * b1_move_pct,
    w4_line_pct  = w4_move_pct  * w3_move_pct  * w2_move_pct  * w1_move_pct,
    b3_line_pct  = b3_move_pct  * b2_move_pct  * b1_move_pct,
    w3_line_pct  = w3_move_pct  * w2_move_pct  * w1_move_pct,
    b2_line_pct  = b2_move_pct  * b1_move_pct,
    w2_line_pct  = w2_move_pct  * w1_move_pct,
    b1_line_pct  = b1_move_pct,
    w1_line_pct  = w1_move_pct
  )

toc()

write_parquet(test, glue(
  "D:/data/chess/opening-explorer-results/{filter_event}_{filter_year}_{sprintf('%02d', filter_month)}_{filter_rating_class}.parquet"
))
