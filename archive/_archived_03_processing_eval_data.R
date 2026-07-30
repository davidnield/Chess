pacman::p_load(tidyverse, bigchess, tictoc, data.table, arrow, tidytable, bench)

tic()

extract_moves <- function(moves, N = 20){
  r <- data.frame(t(sapply(moves,function(i){
    return(  i[1:(N*2)])
  } )),stringsAsFactors = FALSE)
  
  
  colnames(r) <- paste0(rep(c("w","b"),times = N), rep(1:N,each = 2))
  rownames(r) <- NULL
  
  return(r)
}

extract_evals <- function(moves, N = 20){
  r <- data.frame(t(sapply(moves,function(i){
    return(  i[1:(N*2)])
  } )),stringsAsFactors = FALSE)
  
  
  colnames(r) <- paste0(rep(c("w","b"),times = N), rep(1:N,each = 2), "_eval")
  rownames(r) <- NULL
  
  return(r)
}

toc()

# bad_openings <- paste(c("^1\\. a3",
#                         "^1\\. h3",
#                         "^1\\. Na3",
#                         "^1\\. Nh3",
#                         "^1\\. e4 a6",
#                         "^1\\. e4 a5",
#                         "^1\\. e4 h6",
#                         "^1\\. e4 h5",
#                         "^1\\. e4 Na6",
#                         "^1\\. e4 Nh6",
#                         "^1\\. e4 e5 2\\. Ke2",
#                         "^1\\. e4 g5",
#                         "^1\\. e4 b5",
#                         "^1\\. e4 e5 2\\. g4",
#                         "^1\\. e4 e5 2\\. d4 f5",
#                         "^1\\. d4 Na6",
#                         "^1\\. d4 Nh6",
#                         "^1\\. d4 a6",
#                         "^1\\. d4 a5",
#                         "^1\\. d4 h6",
#                         "^1\\. d4 h5",
#                         "^1\\. d4 g5",
#                         "^1\\. e4 e5 2\\. Nf3 Ke7"),
#                       collapse = "|")

opening_set <- paste(c("^1\\. e4 e5 2\\. Nf3 Nc6 3\\. Bc4 Bc5",
                       "^1\\. e4 e5 2\\. Nf3 Nc6 3\\. Bc4 Nf6",
                       "^1\\. e4 e5 2\\. Nf3 Nc6 3\\. Bc4 h6",
                       "^1\\. e4 e5 2\\. Nf3 Nc6 3\\. Bc4 d6",
                       "^1\\. e4 e5 2\\. Nf3 Nc6 3\\. Bc4 Be7",
                       "^1\\. e4 e5 2\\. Nf3 Nc6 3\\. Bc4 Nd4",
                       "^1\\. e4 e5 2\\. Nf3 Nc6 3\\. Bc4 f5",
                       "^1\\. e4 e5 2\\. Nf3 Nc6 3\\. Bb5 a6",
                       "^1\\. e4 e5 2\\. Nf3 Nc6 3\\. Bb5 d6",
                       "^1\\. e4 e5 2\\. Nf3 Nc6 3\\. Bb5 Nf6",
                       "^1\\. e4 e5 2\\. Nf3 Nc6 3\\. Bb5 Bc5",
                       "^1\\. e4 e5 2\\. Nf3 Nc6 3\\. Bb5 Nge7",
                       "^1\\. e4 e5 2\\. Nf3 Nc6 3\\. Bb5 f5",
                       "^1\\. e4 e5 2\\. Nf3 Nc6 3\\. Bb5 g6",
                       "^1\\. e4 e5 2\\. Nf3 Nc6 3\\. Bb5 Nd4",
                       "^1\\. e4 e5 2\\. Nf3 Nc6 3\\. d4 exd4",
                       "^1\\. e4 e5 2\\. Nf3 Nc6 3\\. d4 d6",
                       "^1\\. e4 e5 2\\. Nf3 Nc6 3\\. d4 Nf6",
                       "^1\\. e4 e5 2\\. Nf3 Nc6 3\\. d4 Nxd4",
                       "^1\\. e4 e5 2\\. Nf3 Nc6 3\\. Nc3",
                       "^1\\. e4 e5 2\\. Nf3 Nc6 3\\. c3",
                       "^1\\. e4 e5 2\\. Nf3 Nf6 3\\. d4 exd4",
                       "^1\\. e4 e5 2\\. Nf3 Nf6 3\\. d4 Nxe4",
                       "^1\\. e4 e5 2\\. Nf3 Nf6 3\\. d4 d5",
                       "^1\\. e4 e5 2\\. Nf3 Nf6 3\\. Nxe5",
                       "^1\\. e4 e5 2\\. Nf3 Nf6 3\\. Bc4",
                       "^1\\. e4 e5 2\\. Nf3 Nf6 3\\. Nc3",
                       "^1\\. e4 e5 2\\. Nf3 Nf6 3\\. d3",
                       "^1\\. e4 e5 2\\. Nf3 d6 3\\. Bc4",
                       "^1\\. e4 e5 2\\. Nf3 d6 3\\. d4",
                       "^1\\. e4 e5 2\\. Nf3 d6 3\\. Nc3",
                       "^1\\. e4 e5 2\\. Nf3 d6 3\\. c3",
                       "^1\\. e4 e5 2\\. Nf3 d5 3\\. exd5",
                       "^1\\. e4 e5 2\\. Nf3 d5 3\\. Nxe5",
                       "^1\\. e4 e5 2\\. Nf3 d5 3\\. d4",
                       "^1\\. e4 e5 2\\. Nf3 f5 3\\. Nxe5",
                       "^1\\. e4 e5 2\\. Nf3 f5 3\\. exf5",
                       "^1\\. e4 e5 2\\. Nf3 f5 3\\. d4",
                       "^1\\. e4 e5 2\\. Nf3 f5 3\\. Nc3",
                       "^1\\. e4 e5 2\\. f4 exf4",
                       "^1\\. e4 e5 2\\. f4 d5",
                       "^1\\. e4 e5 2\\. f4 Nc6",
                       "^1\\. e4 e5 2\\. f4 Bc5",
                       "^1\\. e4 e5 2\\. f4 d6",
                       "^1\\. e4 e5 2\\. f4 Nf6",
                       "^1\\. e4 e5 2\\. f4 Qh4+",
                       "^1\\. e4 e5 2\\. d4 exd4",
                       "^1\\. e4 e5 2\\. d4 Nc6",
                       "^1\\. e4 e5 2\\. d4 d6",
                       "^1\\. e4 e5 2\\. d4 Nf6",
                       "^1\\. e4 e5 2\\. d4 d5",
                       "^1\\. e4 e5 2\\. Nc3 Nf6",
                       "^1\\. e4 e5 2\\. Nc3 Nc6",
                       "^1\\. e4 e5 2\\. Nc3 Bc5",
                       "^1\\. e4 e5 2\\. Nc3 d6",
                       "^1\\. e4 e5 2\\. Nc3 Bb4",
                       "^1\\. e4 e5 2\\. Bc4 Bc5",
                       "^1\\. e4 e5 2\\. Bc4 Nf6",
                       "^1\\. e4 e5 2\\. Bc4 Nc6",
                       "^1\\. e4 e5 2\\. Bc4 c6",
                       "^1\\. e4 e5 2\\. Bc4 d6",
                       "^1\\. e4 e5 2\\. Qh5 Nc6",
                       "^1\\. e4 e5 2\\. Qh5 d6",
                       "^1\\. e4 e5 2\\. Qh5 Qf6",
                       "^1\\. e4 e5 2\\. Qh5 Nf6",
                       "^1\\. e4 e5 2\\. Qh5 Qe7",
                       "^1\\. e4 d5 2\\. exd5 Qxd5",
                       "^1\\. e4 d5 2\\. exd5 Nf6",
                       "^1\\. e4 d5 2\\. exd5 c6",
                       "^1\\. e4 d5 2\\. Nc3",
                       "^1\\. e4 d6 2\\. f4",
                       "^1\\. e4 d6 2\\. g3",
                       "^1\\. e4 d6 2\\. Ne2",
                       "^1\\. e4 d6 2\\. d4 Nf6",
                       "^1\\. e4 d6 2\\. d4 g6",
                       "^1\\. e4 d6 2\\. d4 e5",
                       "^1\\. e4 d6 2\\. d4 c6",
                       "^1\\. e4 d6 2\\. d4 e6",
                       "^1\\. e4 d6 2\\. d4 Nd7",
                       "^1\\. e4 d6 2\\. Nc3 Nf6",
                       "^1\\. e4 d6 2\\. Nc3 g6",
                       "^1\\. e4 d6 2\\. Nc3 e5",
                       "^1\\. e4 d6 2\\. Nc3 c6",
                       "^1\\. e4 d6 2\\. Nc3 Nd7",
                       "^1\\. e4 d6 2\\. Nf3 Nf6",
                       "^1\\. e4 d6 2\\. Nf3 c5",
                       "^1\\. e4 d6 2\\. Nf3 e5",
                       "^1\\. e4 d6 2\\. Nf3 g6",
                       "^1\\. e4 e6 2\\. d4 d5",
                       "^1\\. e4 e6 2\\. d4 d6",
                       "^1\\. e4 e6 2\\. d4 c5",
                       "^1\\. e4 e6 2\\. d4 b6",
                       "^1\\. e4 e6 2\\. d4 a6",
                       "^1\\. e4 e6 2\\. d4 Be7",
                       "^1\\. e4 e6 2\\. d4 c6",
                       "^1\\. e4 e6 2\\. d4 Nc6",
                       "^1\\. e4 e6 2\\. d3",
                       "^1\\. e4 e6 2\\. Nf3",
                       "^1\\. e4 e6 2\\. Qe2",
                       "^1\\. e4 e6 2\\. f4",
                       "^1\\. e4 e6 2\\. Nc3 d5",
                       "^1\\. e4 e6 2\\. Nc3 c5",
                       "^1\\. e4 e6 2\\. Nc3 b6",
                       "^1\\. e4 e6 2\\. Nc3 Be7",
                       "^1\\. e4 e6 2\\. Nc3 Nf6",
                       "^1\\. e4 e6 2\\. Nf3 d5",
                       "^1\\. e4 e6 2\\. Nf3 c5",
                       "^1\\. e4 e6 2\\. Nf3 b6",
                       "^1\\. e4 e6 2\\. Nf3 Be7",
                       "^1\\. e4 Nf6 2\\. e5 Nd5 3\\. d4 d6 4\\. c4 Nb6",
                       "^1\\. e4 Nf6 2\\. e5 Nd5 3\\. d4 d6 4\\. Nf3",
                       "^1\\. e4 Nf6 2\\. e5 Nd5 3\\. c4 Nb6",
                       "^1\\. e4 Nc6 2\\. d4 e5",
                       "^1\\. e4 Nc6 2\\. d4 d5",
                       "^1\\. e4 Nc6 2\\. d4 d6",
                       "^1\\. e4 Nc6 2\\. d4 e6",
                       "^1\\. e4 Nc6 2\\. Nf3",
                       "^1\\. e4 Nc6 2\\. Nc3",
                       "^1\\. e4 c6 2\\. d4 d5 3\\. e5 Bf5",
                       "^1\\. e4 c6 2\\. d4 d5 3\\. e5 c5",
                       "^1\\. e4 c6 2\\. d4 d5 3\\. e5 g6",
                       "^1\\. e4 c6 2\\. d4 d5 3\\. exd5",
                       "^1\\. e4 c6 2\\. d4 d5 3\\. Nc3",
                       "^1\\. e4 c6 2\\. d4 d5 3\\. Nd2",
                       "^1\\. e4 c6 2\\. d4 d5 3\\. f3",
                       "^1\\. e4 c6 2\\. c4",
                       "^1\\. e4 c6 2\\. Nc3",
                       "^1\\. e4 c6 2\\. c3",
                       "^1\\. e4 c6 2\\. Nf3",
                       "^1\\. e4 g6 2\\. d4 Bg7",
                       "^1\\. e4 g6 2\\. Nf3 Bg7",
                       "^1\\. e4 g6 2\\. Nc3 Bg7",
                       "^1\\. e4 g6 2\\. h4",
                       "^1\\. e4 b6 2\\. d4",
                       "^1\\. e4 b6 2\\. Nf3",
                       "^1\\. e4 b6 2\\. Nc3",
                       "^1\\. e4 c5 2\\. Nf3 Nc6",
                       "^1\\. e4 c5 2\\. Nf3 d6",
                       "^1\\. e4 c5 2\\. Nf3 e6",
                       "^1\\. e4 c5 2\\. Nf3 g6",
                       "^1\\. e4 c5 2\\. Nf3 a6",
                       "^1\\. e4 c5 2\\. Nf3 Nf6 3\\. e5 Nd5",
                       "^1\\. e4 c5 2\\. Nf3 Nf6 3\\. Nc3",
                       "^1\\. e4 c5 2\\. Nc3",
                       "^1\\. e4 c5 2\\. c3",
                       "^1\\. e4 c5 2\\. d4",
                       "^1\\. e4 c5 2\\. b4",
                       "^1\\. d4 d5 2\\. c4",
                       "^1\\. d4 d5 2\\. Nf3",
                       "^1\\. d4 d5 2\\. Bf4",
                       "^1\\. d4 d5 2\\. e3",
                       "^1\\. d4 d5 2\\. g3",
                       "^1\\. d4 d5 2\\. Bg5",
                       "^1\\. d4 d5 2\\. e4",
                       "^1\\. d4 Nf6 2\\. c4",
                       "^1\\. d4 Nf6 2\\. Nf3",
                       "^1\\. d4 Nf6 2\\. Bf4",
                       "^1\\. d4 Nf6 2\\. Bg5",
                       "^1\\. d4 Nf6 2\\. e3",
                       "^1\\. d4 Nf6 2\\. g3",
                       "^1\\. d4 e6 2\\. c4",
                       "^1\\. d4 e6 2\\. e4 c5",
                       "^1\\. d4 e5 2\\. dxe5",
                       "^1\\. d4 e5 2\\. c4",
                       "^1\\. d4 d6",
                       "^1\\. d4 f5",
                       "^1\\. d4 g6",
                       "^1\\. d4 c5",
                       "^1\\. d4 c6",
                       "^1\\. c4 Nf6",
                       "^1\\. c4 e5",
                       "^1\\. c4 e6",
                       "^1\\. c4 c5",
                       "^1\\. c4 c6",
                       "^1\\. c4 g6",
                       "^1\\. b3 e5",
                       "^1\\. b3 d5",
                       "^1\\. b3 c5",
                       "^1\\. b3 c6",
                       "^1\\. b3 Nf5",
                       "^1\\. Nf3 d5",
                       "^1\\. Nf3 c5",
                       "^1\\. Nf3 c6",
                       "^1\\. Nf3 e6",
                       "^1\\. Nf3 Nf6",
                       "^1\\. g3 d5",
                       "^1\\. g3 e5",
                       "^1\\. g3 c5",
                       "^1\\. g3 e6",
                       "^1\\. g3 Nf6",
                       "^1\\. g3 g6"),
                     collapse = "|")

# df <- open_dataset("Data/Lichess Data/Processed PGNs/", format = "feather")

df <- read_feather("Data/Consolidated PGNs/month=2022-01/time_control=Classical/rating_class=1000-1200/part-0.arrow",
                   as_data_frame = FALSE)

test <- df %>% 
  dplyr::mutate(clean_pgn = str_replace_all(move_text, " \\{[^\\}]*\\}", ""),
                move_text =
                  if_else(str_detect(clean_pgn, opening_set),
                          str_replace_all(move_text, " 21\\. .*", ""),
                          str_replace_all(move_text, " 11\\. .*", "")),
                move_text = str_replace_all(move_text, "eval ", ""),
                move_text = str_replace_all(move_text, "\\{\\}", "{NA}"),
                move_text = str_replace_all(move_text, "(\\d+\\. [KQRBN]?[a-h]?[1-8]?x?[a-h][1-8](\\=[QRBN])?#) \\{NA\\}", "\\1 {#0}"),
                move_text = str_replace_all(move_text, "([KQRBN]?[a-h]?[1-8]?x?[a-h][1-8](\\=[QRBN])?#) \\{NA\\}", "\\1 {#-0}")) %>% 
  collect()

test <- test %>% 
  mutate(eval = str_extract_all(move_text, "(?<=\\{)[^\\}]+(?=\\})"),
         clean_pgn =
           if_else(str_detect(clean_pgn, opening_set),
                   str_replace_all(clean_pgn, " 21\\. .*", ""),
                   str_replace_all(clean_pgn, " 11\\. .*", "")
           ),
         clean_pgn = str_replace_all(clean_pgn, "([0-9+]{0,}\\.) ", ""),
         move = str_split(clean_pgn," "),
         move_length = lengths(move),
         eval_length = lengths(eval))

test <- bind_cols(test, extract_moves(test$move))

test <- bind_cols(test, extract_evals(test$eval))

test <- test %>% 
  mutate(
    across(w1_eval:b20_eval,
    ~ if_else(.x == "NA", NA_character_, .x)),
    across(w1_eval:b20_eval,
         ~ if_else(str_detect(.x, "#"),
                 case_when(
                   .x == "#0" ~ "600",
                   .x == "#1" ~ "500",
                   .x == "#2" ~ "400",
                   .x == "#3" ~ "300",
                   .x == "#-0" ~ "-600",
                   .x == "#-1" ~ "-500",
                   .x == "#-2" ~ "-400",
                   .x == "#-3" ~ "-300",
                   as.numeric(str_remove(.x, "#")) >= 4 ~ "200",
                   as.numeric(str_remove(.x, "#")) <= -4 ~ "-200"),
                 .x
         )),
         across(w1_eval:b20_eval,
                as.numeric
                ))


test2 <- test %>% 
  group_by(w1:b20, result) %>% 
  summarise(num_games = n(),
            across(w1_eval:b20_eval, ~mean(.x, na.rm = TRUE)),
            .groups = "drop") %>% 
  pivot_wider(names_from = "result",
              names_glue = "b20_{result}",
              values_from = "num_games") %>% 
  rename(b20_draws = b20_draw) %>% 
  mutate(b20_white_wins = if_else(is.na(b20_white_wins), 0, b20_white_wins),
         b20_black_wins = if_else(is.na(b20_black_wins), 0, b20_black_wins),
         b20_draws = if_else(is.na(b20_draws), 0, b20_draws),
         b20_total_games = b20_white_wins + b20_black_wins + b20_draws) %>% 
  # W20
  group_by(w1:w20) %>%
  mutate(w20_white_wins = sum(b20_white_wins),
         w20_black_wins = sum(b20_black_wins),
         w20_draws = sum(b20_draws),
         w20_total_games = sum(b20_total_games),
         w20_eval = mean(w20_eval, na.rm = TRUE)) %>% 
  # B19
  group_by(w1:b19) %>%
  mutate(b19_white_wins = sum(b20_white_wins),
         b19_black_wins = sum(b20_black_wins),
         b19_draws = sum(b20_draws),
         b19_total_games = sum(b20_total_games),
         b19_eval = mean(b19_eval, na.rm = TRUE)) %>%
  # W19
  group_by(w1:w19) %>%
  mutate(w19_white_wins = sum(b20_white_wins),
         w19_black_wins = sum(b20_black_wins),
         w19_draws = sum(b20_draws),
         w19_total_games = sum(b20_total_games),
         w19_eval = mean(w19_eval, na.rm = TRUE)) %>%
    # B18
    group_by(w1:b18) %>%
    mutate(b18_white_wins = sum(b20_white_wins),
           b18_black_wins = sum(b20_black_wins),
           b18_draws = sum(b20_draws),
           b18_total_games = sum(b20_total_games),
           b18_eval = mean(b18_eval, na.rm = TRUE)) %>%
    # W18
    group_by(w1:w18) %>%
    mutate(w18_white_wins = sum(b20_white_wins),
           w18_black_wins = sum(b20_black_wins),
           w18_draws = sum(b20_draws),
           w18_total_games = sum(b20_total_games),
           w18_eval = mean(w18_eval, na.rm = TRUE)) %>%
    # B17
    group_by(w1:b17) %>%
    mutate(b17_white_wins = sum(b20_white_wins),
           b17_black_wins = sum(b20_black_wins),
           b17_draws = sum(b20_draws),
           b17_total_games = sum(b20_total_games),
           b17_eval = mean(b17_eval, na.rm = TRUE)) %>%
  # W17
  group_by(w1:w17) %>%
  mutate(w17_white_wins = sum(b20_white_wins),
         w17_black_wins = sum(b20_black_wins),
         w17_draws = sum(b20_draws),
         w17_total_games = sum(b20_total_games),
         w17_eval = mean(w17_eval, na.rm = TRUE)) %>%
  # B16
  group_by(w1:b16) %>%
  mutate(b16_white_wins = sum(b20_white_wins),
         b16_black_wins = sum(b20_black_wins),
         b16_draws = sum(b20_draws),
         b16_total_games = sum(b20_total_games),
         b16_eval = mean(b16_eval, na.rm = TRUE)) %>%
  # W16
  group_by(w1:w16) %>%
  mutate(w16_white_wins = sum(b20_white_wins),
         w16_black_wins = sum(b20_black_wins),
         w16_draws = sum(b20_draws),
         w16_total_games = sum(b20_total_games),
         w16_eval = mean(w16_eval, na.rm = TRUE)) %>%
  # B15
  group_by(w1:b15) %>%
  mutate(b15_white_wins = sum(b20_white_wins),
         b15_black_wins = sum(b20_black_wins),
         b15_draws = sum(b20_draws),
         b15_total_games = sum(b20_total_games),
         b15_eval = mean(b15_eval, na.rm = TRUE)) %>%
  # W15
  group_by(w1:w15) %>%
  mutate(w15_white_wins = sum(b20_white_wins),
         w15_black_wins = sum(b20_black_wins),
         w15_draws = sum(b20_draws),
         w15_total_games = sum(b20_total_games),
         w15_eval = mean(w15_eval, na.rm = TRUE)) %>%
  # B14
  group_by(w1:b14) %>%
  mutate(b14_white_wins = sum(b20_white_wins),
         b14_black_wins = sum(b20_black_wins),
         b14_draws = sum(b20_draws),
         b14_total_games = sum(b20_total_games),
         b14_eval = mean(b14_eval, na.rm = TRUE)) %>%
  # W14
  group_by(w1:w14) %>%
  mutate(w14_white_wins = sum(b20_white_wins),
         w14_black_wins = sum(b20_black_wins),
         w14_draws = sum(b20_draws),
         w14_total_games = sum(b20_total_games),
         w14_eval = mean(w14_eval, na.rm = TRUE)) %>%
  # B13
  group_by(w1:b13) %>%
  mutate(b13_white_wins = sum(b20_white_wins),
         b13_black_wins = sum(b20_black_wins),
         b13_draws = sum(b20_draws),
         b13_total_games = sum(b20_total_games),
         b13_eval = mean(b13_eval, na.rm = TRUE)) %>%
  # W13
  group_by(w1:w13) %>%
  mutate(w13_white_wins = sum(b20_white_wins),
         w13_black_wins = sum(b20_black_wins),
         w13_draws = sum(b20_draws),
         w13_total_games = sum(b20_total_games),
         w13_eval = mean(w13_eval, na.rm = TRUE)) %>% 
  # B12
  group_by(w1:b12) %>% 
  mutate(b12_white_wins = sum(b20_white_wins),
         b12_black_wins = sum(b20_black_wins),
         b12_draws = sum(b20_draws),
         b12_total_games = sum(b20_total_games),
         b12_eval = mean(b12_eval, na.rm = TRUE)) %>%
  # W12
  group_by(w1:w12) %>%
  mutate(w12_white_wins = sum(b20_white_wins),
         w12_black_wins = sum(b20_black_wins),
         w12_draws = sum(b20_draws),
         w12_total_games = sum(b20_total_games),
         w12_eval = mean(w12_eval, na.rm = TRUE)) %>%
  # B11
  group_by(w1:b11) %>%
  mutate(b11_white_wins = sum(b20_white_wins),
         b11_black_wins = sum(b20_black_wins),
         b11_draws = sum(b20_draws),
         b11_total_games = sum(b20_total_games),
         b11_eval = mean(b11_eval, na.rm = TRUE)) %>%
  # W11
  group_by(w1:w11) %>%
  mutate(w11_white_wins = sum(b20_white_wins),
         w11_black_wins = sum(b20_black_wins),
         w11_draws = sum(b20_draws),
         w11_total_games = sum(b20_total_games),
         w11_eval = mean(w11_eval, na.rm = TRUE)) %>%
  # B10
  group_by(w1:b10) %>%
  mutate(b10_white_wins = sum(b20_white_wins),
         b10_black_wins = sum(b20_black_wins),
         b10_draws = sum(b20_draws),
         b10_total_games = sum(b20_total_games),
         b10_eval = mean(b10_eval, na.rm = TRUE)) %>%
  # W10
  group_by(w1:w10) %>%
  mutate(w10_white_wins = sum(b20_white_wins),
         w10_black_wins = sum(b20_black_wins),
         w10_draws = sum(b20_draws),
         w10_total_games = sum(b20_total_games),
         w10_eval = mean(w10_eval, na.rm = TRUE)) %>%
  # B9
  group_by(w1:b9) %>%
  mutate(b9_white_wins = sum(b20_white_wins),
         b9_black_wins = sum(b20_black_wins),
         b9_draws = sum(b20_draws),
         b9_total_games = sum(b20_total_games),
         b9_eval = mean(b9_eval, na.rm = TRUE)) %>%
  # W9
  group_by(w1:w9) %>%
  mutate(w9_white_wins = sum(b20_white_wins),
         w9_black_wins = sum(b20_black_wins),
         w9_draws = sum(b20_draws),
         w9_total_games = sum(b20_total_games),
         w9_eval = mean(w9_eval, na.rm = TRUE)) %>%
  # B8
  group_by(w1:b8) %>%
  mutate(b8_white_wins = sum(b20_white_wins),
         b8_black_wins = sum(b20_black_wins),
         b8_draws = sum(b20_draws),
         b8_total_games = sum(b20_total_games),
         b8_eval = mean(b8_eval, na.rm = TRUE)) %>%
  # W8
  group_by(w1:w8) %>%
  mutate(w8_white_wins = sum(b20_white_wins),
         w8_black_wins = sum(b20_black_wins),
         w8_draws = sum(b20_draws),
         w8_total_games = sum(b20_total_games),
         w8_eval = mean(w8_eval, na.rm = TRUE)) %>% 
  # B7
  group_by(w1:b7) %>% 
  mutate(b7_white_wins = sum(b20_white_wins),
         b7_black_wins = sum(b20_black_wins),
         b7_draws = sum(b20_draws),
         b7_total_games = sum(b20_total_games),
         b7_eval = mean(b7_eval, na.rm = TRUE)) %>%
  # W7
  group_by(w1:w7) %>%
  mutate(w7_white_wins = sum(b20_white_wins),
         w7_black_wins = sum(b20_black_wins),
         w7_draws = sum(b20_draws),
         w7_total_games = sum(b20_total_games),
         w7_eval = mean(w7_eval, na.rm = TRUE)) %>%
  # B6
  group_by(w1:b6) %>%
  mutate(b6_white_wins = sum(b20_white_wins),
         b6_black_wins = sum(b20_black_wins),
         b6_draws = sum(b20_draws),
         b6_total_games = sum(b20_total_games),
         b6_eval = mean(b6_eval, na.rm = TRUE)) %>%
  # W6
  group_by(w1:w6) %>%
  mutate(w6_white_wins = sum(b20_white_wins),
         w6_black_wins = sum(b20_black_wins),
         w6_draws = sum(b20_draws),
         w6_total_games = sum(b20_total_games),
         w6_eval = mean(w6_eval, na.rm = TRUE)) %>%
  # B5
  group_by(w1:b5) %>%
  mutate(b5_white_wins = sum(b20_white_wins),
         b5_black_wins = sum(b20_black_wins),
         b5_draws = sum(b20_draws),
         b5_total_games = sum(b20_total_games),
         b5_eval = mean(b5_eval, na.rm = TRUE)) %>%
  # W5
  group_by(w1:w5) %>%
  mutate(w5_white_wins = sum(b20_white_wins),
         w5_black_wins = sum(b20_black_wins),
         w5_draws = sum(b20_draws),
         w5_total_games = sum(b20_total_games),
         w5_eval = mean(w5_eval, na.rm = TRUE)) %>%
  # B4
  group_by(w1:b4) %>%
  mutate(b4_white_wins = sum(b20_white_wins),
         b4_black_wins = sum(b20_black_wins),
         b4_draws = sum(b20_draws),
         b4_total_games = sum(b20_total_games),
         b4_eval = mean(b4_eval, na.rm = TRUE)) %>%
  # W4
  group_by(w1:w4) %>%
  mutate(w4_white_wins = sum(b20_white_wins),
         w4_black_wins = sum(b20_black_wins),
         w4_draws = sum(b20_draws),
         w4_total_games = sum(b20_total_games),
         w4_eval = mean(w4_eval, na.rm = TRUE)) %>%
  # B3
  group_by(w1:b3) %>%
  mutate(b3_white_wins = sum(b20_white_wins),
         b3_black_wins = sum(b20_black_wins),
         b3_draws = sum(b20_draws),
         b3_total_games = sum(b20_total_games),
         b3_eval = mean(b3_eval, na.rm = TRUE)) %>%
  # W3
  group_by(w1:w3) %>%
  mutate(w3_white_wins = sum(b20_white_wins),
         w3_black_wins = sum(b20_black_wins),
         w3_draws = sum(b20_draws),
         w3_total_games = sum(b20_total_games),
         w3_eval = mean(w3_eval, na.rm = TRUE)) %>% 
  # B2
  group_by(w1:b2) %>%
  mutate(b2_white_wins = sum(b20_white_wins),
         b2_black_wins = sum(b20_black_wins),
         b2_draws = sum(b20_draws),
         b2_total_games = sum(b20_total_games),
         b2_eval = mean(b2_eval, na.rm = TRUE)) %>%
  # W2
  group_by(w1:w2) %>%
  mutate(w2_white_wins = sum(b20_white_wins),
         w2_black_wins = sum(b20_black_wins),
         w2_draws = sum(b20_draws),
         w2_total_games = sum(b20_total_games),
         w2_eval = mean(w2_eval, na.rm = TRUE)) %>%
  # B1
  group_by(w1:b1) %>%
  mutate(b1_white_wins = sum(b20_white_wins),
         b1_black_wins = sum(b20_black_wins),
         b1_draws = sum(b20_draws),
         b1_total_games = sum(b20_total_games),
         b1_eval = mean(b1_eval, na.rm = TRUE)) %>%
  # W1
  group_by(w1) %>%
  mutate(w1_white_wins = sum(b20_white_wins),
         w1_black_wins = sum(b20_black_wins),
         w1_draws = sum(b20_draws),
         w1_total_games = sum(b20_total_games),
         w1_eval = mean(w1_eval, na.rm = TRUE),
         .groups = "drop")

test2

# Highest and lowest eval for Jan 2022 is -152.65 and 152.65
# Highest and lowest eval for Dec 2022 is -152.65 and 152.65

df <- df %>% 
  dplyr::filter(
    month == "2022-12",
                str_detect(Movetext, "eval"),
    Event %in% c("Rated Blitz game", "Rated Rapid game", "Rated Classical game", "Rated Correspondence game"),
    WhiteElo >= 1500,
    BlackElo >= 1500) %>%
  collect()

df <- df %>% 
  select(Site, Movetext, Result, clean_pgn) %>%
  mutate(Movetext =
                  if_else(str_detect(clean_pgn, opening_set),
                          str_replace_all(Movetext, "21\\. .*", ""),
                          str_replace_all(Movetext, "11\\. .*", "")
                  ),
                Movetext =
                  if_else(str_detect(clean_pgn, bad_openings),
                          str_replace_all(Movetext, "6\\. .*", ""),
                          Movetext
                  ),
                clean_pgn =
                  if_else(str_detect(clean_pgn, opening_set),
                          str_replace_all(clean_pgn, "21\\. .*", ""),
                          str_replace_all(clean_pgn, "11\\. .*", "")
                  ),
                clean_pgn =
                  if_else(str_detect(clean_pgn, bad_openings),
                          str_replace_all(clean_pgn, "6\\. .*", ""),
                          clean_pgn
                  ),
                Movetext = str_replace_all(Movetext, " \\[%clk \\d+:\\d+:\\d+] ", "")) %>%
  mutate(Movetext = str_remove_all(Movetext, Result),
         Movetext = str_replace_all(Movetext, "(\\d+\\. [:graph:]+#)", "\\1 [%eval #0]"),
         Movetext = str_replace_all(Movetext, "(\\d+\\.\\.\\. [:graph:]+#)", "\\1 [%eval #-0]"),
         # moves = str_extract_all(Movetext, "\\d+\\.\\s[Oo0](-[Oo0]){1,2}|\\d+\\.\\s([a-zA-Z0-9]+)|\\d+\\.\\.\\.\\s[Oo0](-[Oo0]){1,2}|\\d+\\.\\.\\.\\s([a-zA-Z0-9]+)"),
         tmp = str_replace_all(clean_pgn, "([0-9+]{0,}\\.)", ""),
         tmp = str_replace_all(tmp, "\\s+", " "),
         tmp = str_trim(tmp),
         move = str_split(tmp," "),
         eval = str_extract_all(Movetext, "\\[%eval [-]?[0-9]+\\.[0-9]+\\]|\\[%eval #[-]?[0-9]+\\]"),
         move_length = lengths(move),
         eval_length = lengths(eval)) %>% 
  filter(move_length == eval_length) %>% 
  select(Site, move, eval) %>%
  unnest() %>% 
  mutate(move = str_replace(move, "\\d+... ", ""),
         eval = str_replace_all(eval, "\\[%eval |\\]", ""),
         numeric_eval = as.numeric(eval),
         moves_to_mate = if_else(str_starts(eval, "#"),
                                 as.numeric(str_remove(eval, "#")),
                                 NA),
         numeric_eval = case_when(
           !is.na(numeric_eval) ~ numeric_eval,
           eval == "#0" ~ 600,
           moves_to_mate == 1 ~ 500,
           moves_to_mate == 2 ~ 400,
           moves_to_mate == 3 ~ 300,
           moves_to_mate >= 4 ~ 200,
           eval == "#-0" ~ -600,
           moves_to_mate == -1 ~ -500,
           moves_to_mate == -2 ~ -400,
           moves_to_mate == -3 ~ -300,
           moves_to_mate <= -4 ~ -200,
           TRUE ~ NA_real_
         )) %>% 
  group_by(Site) %>%
  mutate(pgn = accumulate(move, paste, sep = " ")) %>% 
  group_by(pgn) %>%
  summarise(numeric_eval = mean(numeric_eval, na.rm = TRUE)) %>% 
  ungroup() %>% 
  mutate(tmp = str_replace_all(pgn, "([0-9+]{0,}\\.)", ""),
         tmp = str_replace_all(tmp, "\\s+", " "),
         tmp = str_trim(tmp),
         moves = str_split(tmp," ")) %>% 
  select(-tmp)

nrow(df)

write_rds(df, "Data/Lichess Data/Processed Evals/test.rds")

toc()

gc()

tic()


df <- bind_cols(df, extract_moves(df$moves, N = 20))

df <- df %>% 
  select(-moves)

write_feather(df, "Data/Lichess Data/Processed Evals/evals.feather")

toc()