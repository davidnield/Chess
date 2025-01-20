pacman::p_load(tidyverse, tictoc, data.table, arrow, tidytable)

extract_moves <- function(moves, N = 20){
  r <- data.frame(t(sapply(moves,function(i){
    return(  i[1:(N*2)])
  } )),stringsAsFactors = TRUE)
  
  
  colnames(r) <- paste0(rep(c("W","B"),times = N),rep(1:N,each = 2))
  rownames(r) <- NULL
  
  return(r)
}

parameters <- crossing(month = c(
  "2022-01",
  "2022-02",
  "2022-03",
  "2022-04",
  "2022-05",
  "2022-06",
  "2022-07",
  "2022-08",
  "2022-09",
  "2022-10",
  "2022-11",
  "2022-12"),
  time_control = c("UltraBullet",
                   "Bullet",
                   "Blitz",
                   "Rapid",
                   "Classical",
                   "Correspondence"),
  rating_class = c("Under 1000",
                   "1000-1200",
                   "1200-1400",
                   "1400-1600",
                   "1600-1800",
                   "1800-2000",
                   "2000-2200",
                   "2200-2500",
                   "Over 2500")
)

df <- open_dataset("Data/Consolidated PGNs/", format = "feather")

df <- df %>% 
  dplyr::filter(month == "2022-01",
         time_control == "Blitz",
         rating_class == "1000-1200") %>% 
  collect()

### PRODUCTION ###

tic(msg = "Whole thing")
  
for (i in 1:nrow(parameters)) {
  tic(msg = paste(parameters$month[[i]], parameters$time_control[[i]], parameters$rating_class[[i]]))
  
  temp <- df %>% 
    dplyr::filter(time_control == parameters$time_control[[i]],
                  rating_class == parameters$rating_class[[i]],
                  month == parameters$month[[i]]) %>% 
    dplyr::mutate(tmp = str_replace_all(clean_pgn, "([0-9+]{0,}\\.)", ""),
                  tmp = str_replace_all(tmp, "\\s+", " "),
                  tmp = str_trim(tmp),
                  moves = str_split(tmp, " ")) %>% 
    collect()
  
  temp <- bind_cols(temp, extract_moves(temp$moves)) %>% 
    select(-moves, -tmp) %>% 
    group_by(month, time_control, rating_class, W1:B20, Result) %>% 
    count() %>% 
    ungroup() %>% 
    pivot_wider(names_from = "Result",
                values_from = "n")
  
  if(!'white_wins' %in% names(temp)) temp <- temp %>% add_column(white_wins = NA)
  if(!'black_wins' %in% names(temp)) temp <- temp %>% add_column(black_wins = NA)
  if(!'draw' %in% names(temp)) temp <- temp %>% add_column(draw = NA)
  
  temp <- temp %>% 
    mutate(white_wins = if_else(is.na(white_wins), 0, white_wins),
           black_wins = if_else(is.na(black_wins), 0, black_wins),
           draw = if_else(is.na(draw), 0, draw)) %>% 
    mutate(total_games = white_wins + black_wins + draw) %>% 
    ungroup()
  
  write_dataset(temp, path = "Data/Opening Explorer/", format = "feather",
                partitioning = c("month", "time_control", "rating_class"))
  
  toc()
  
}

toc()