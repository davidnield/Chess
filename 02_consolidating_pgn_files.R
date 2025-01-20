pacman::p_load(tidyverse, bigchess, tictoc, data.table, arrow, tidytable)

tic()

# To create clean_pgn: str_replace_all(move_text, " \\{[^\\}]*\\}", "")

df <- open_dataset("E:/Chess/Data/Lichess Data/Processed PGNs/", format = "feather") %>% 
  select(site, result, termination, move_text, num_moves)


write_dataset(df, path = "Data/Consolidated PGNs", format = "feather", partitioning = c("month", "time_control", "rating_class"))

toc()