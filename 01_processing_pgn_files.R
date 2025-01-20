pacman::p_load(tidyverse, bigchess, tictoc, data.table, arrow, tidytable)

my_num_moves <- function(moves_string) {
  n_moves <- suppressWarnings(as.integer(str_remove(stringi::stri_extract_last(moves_string, regex = "([0-9+]{0,}\\.)"), "\\.")))
  return(n_moves)
}

file_list <- list.files("E:/Chess/Data/Lichess Data/Raw/", recursive = TRUE, pattern = "lichess_db_standard_rated")

file_list <- tibble(paths = file_list) %>% 
  filter(str_detect(paths, "^2022/2022-07/|^2022/2022-08/|^2022/2022-09/|^2022/2022-10/|^2022/2022-11/|^2022/2022-12/")) %>% 
  pluck(1)

tags <- c("Site", "Event","Result","Termination","ECO","Opening","WhiteElo","BlackElo", "Movetext")
s <- "^\\[([\\S]+)\\s\"([\\S\\s]+|\\B)\"\\]$"

for (i in seq_along(file_list)) {
  tic(msg = paste("Part", i))
  
  raw <- data.table::fread(paste0("E:/Chess/Data/Lichess Data/Raw/", file_list[i]), sep = "\n", header = FALSE)
  
  processed <- data.table(field = str_replace_all(raw$V1, s, "\\1"),
                          value = str_replace_all(raw$V1, s, "\\2"),
                          tmp = str_detect(raw$V1, "^\\[[^%]+\\]$"),
                          game_id = cumsum(str_detect(raw$V1, "\\[Event "))) %>% 
    mutate(field = if_else(tmp == FALSE, "Movetext", field)) %>% 
    select(-tmp) %>% 
    filter(field %in% tags,
           field != "Movetext" | value != "") %>% 
    pivot_wider(names_from = field,
                values_from = value) %>% 
    select(-game_id) %>% 
    filter(Result != "*",
           Termination %in% c("Normal", "Time forfeit"),
           !str_detect(Event, "tournament")) %>% 
    mutate(Movetext = str_replace_all(Movetext, "[\\?\\!]|[0-9]+\\.\\.\\.|1-0|0-1|1/2-1/2| \\[%clk \\d+:\\d+:\\d+] | \\[%|\\]", ""),
           Movetext = str_squish(Movetext),
           clean_pgn = str_replace_all(Movetext, " \\{[^}]*\\}", ""),
           num_moves = my_num_moves(clean_pgn)) %>% 
    filter(num_moves > 2) %>% 
    mutate(WhiteElo = as.numeric(WhiteElo),
           BlackElo = as.numeric(BlackElo),
           rating_diff = abs(WhiteElo - BlackElo),
           month = str_extract(file_list[i], "[[:digit:]]{4}-[[:digit:]]{2}")) %>% 
    filter(rating_diff <= 200) %>% 
    mutate(avg_rating = (WhiteElo + BlackElo)/2,
           time_control = str_replace_all(Event, "Rated | game", ""),
           rating_class = case_when(
             avg_rating < 1000 ~ "Under 1000",
             avg_rating >= 1000 & avg_rating < 1200 ~ "1000-1200",
             avg_rating >= 1200 & avg_rating < 1400 ~ "1200-1400",
             avg_rating >= 1400 & avg_rating < 1600 ~ "1400-1600",
             avg_rating >= 1600 & avg_rating < 1800 ~ "1600-1800",
             avg_rating >= 1800 & avg_rating < 2000 ~ "1800-2000",
             avg_rating >= 2000 & avg_rating < 2200 ~ "2000-2200",
             avg_rating >= 2200 & avg_rating < 2500 ~ "2200-2500",
             avg_rating >= 2500 ~ "Over 2500"
           ),
           Result = case_when(
             Result == "0-1" ~ "black_wins",
             Result == "1-0" ~ "white_wins",
             Result == "1/2-1/2" ~ "draw"
           )) %>% 
    select(site = Site, month, rating_class, time_control, result = Result, termination = Termination, move_text = Movetext, clean_pgn, num_moves)
  
  write_dataset(processed, "E:/Chess/Data/Lichess Data/Processed PGNs",
                basename_template = paste0(str_extract(file_list[i], "\\d{4}-\\d{2}.\\d+"), "-part-{i}.feather"),
                partitioning = c("month", "time_control", "rating_class"),
                hive_style = TRUE,
                format = "feather")
  
  toc()
  
}