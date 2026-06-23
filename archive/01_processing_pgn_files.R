library(conflicted)
library(tidyverse)
library(arrow)
library(fs)
library(glue)

conflicts_prefer(dplyr::filter)
conflicts_prefer(dplyr::collect)
conflicts_prefer(dplyr::mutate)

# --- Configuration ---
source_path <- "E:/data/chess/standard-chess-games/"
output_path <- "D:/data/chess/standard-chess-games-compressed/"
log_file    <- path(output_path, "processing_log.txt")

# Ensure output directory exists
if (!dir_exists(output_path)) dir_create(output_path)

# Initialize Log File
cat(glue("Log started at {Sys.time()}\n"), file = log_file, append = FALSE)

# Force high concurrency for Arrow C++ backend
set_cpu_count(12) 
set_io_thread_count(12)

# --- Helper Function: Logger ---
log_progress <- function(message) {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  formatted_msg <- glue("[{timestamp}] {message}")
  
  # Print to console
  cat(formatted_msg, "\n")
  
  # Append to log file
  cat(formatted_msg, "\n", file = log_file, append = TRUE)
}

# --- Events Configuration (Used for Filtering & Cleaning) ---
events_config <- tribble(
  ~source_event, ~clean_event,
  "Rated UltraBullet game", "UltraBullet",
  "Rated Correspondence game", "Correspondence",
  "Rated Classical game", "Classical",
  "Rated Rapid game", "Rapid",
  "Rated Bullet game", "Bullet",
  "Rated Blitz game", "Blitz"
)

events_to_keep <- events_config$source_event

# --- Build Partition List ---
log_progress("Scanning source directory for partitions...")

partition_df <- dir_ls(source_path, type = "directory") |>
  map_dfr(\(year_dir) {
    year <- as.integer(str_extract(year_dir, "(?<=year=)\\d+"))
    dir_ls(year_dir, type = "directory") |>
      map_dfr(\(month_dir) {
        month <- as.integer(str_extract(month_dir, "(?<=month=)\\d+"))
        tibble(
          year = year,
          month = month,
          path = as.character(month_dir)
        )
      })
  }) |>
  arrange(desc(year), desc(month))

total_partitions <- nrow(partition_df)
log_progress(glue("Found {total_partitions} monthly partitions to process."))

total_start <- Sys.time()
completed_count <- 0

# --- Main Processing Loop ---
for (i in seq_len(total_partitions)) {
  part <- partition_df[i, ]
  
  # ETA Calculation
  avg_time_per_part <- if (completed_count > 0) {
    as.numeric(difftime(Sys.time(), total_start, units = "mins")) / completed_count
  } else {
    NA
  }
  
  eta_msg <- if (!is.na(avg_time_per_part)) {
    remaining_mins <- avg_time_per_part * (total_partitions - i + 1)
    glue("| Est. Remaining: {round(remaining_mins / 60, 1)} hours")
  } else {
    "| Est. Remaining: Calculating..."
  }

  log_progress(glue("Starting [{i}/{total_partitions}] {part$year}-{sprintf('%02d', part$month)} {eta_msg}"))
  loop_start <- Sys.time()
  
  tryCatch({
    ds <- open_dataset(part$path, format = "parquet")
    
    ds |>
      filter(Event %in% events_to_keep) |> 
      mutate(
        # --- 1. CLEANING & RENAME SOURCE COLUMNS (SNAKE_CASE) ---
        site = Site,
        white = White,
        black = Black,
        result = Result,
        white_title = WhiteTitle,
        black_title = BlackTitle,
        white_elo = cast(WhiteElo, int16()),
        black_elo = cast(BlackElo, int16()),
        white_rating_diff = WhiteRatingDiff, # Assuming this is the differential value
        black_rating_diff = BlackRatingDiff,
        utc_date = UTCDate,
        utc_time = UTCTime,
        eco = ECO,
        opening = Opening,
        termination = Termination,
        time_control = TimeControl,
        movetext = movetext,
        
        # New Partitioning Fields
        event = case_when(
          Event == "Rated UltraBullet game" ~ "UltraBullet",
          Event == "Rated Correspondence game" ~ "Correspondence",
          Event == "Rated Classical game" ~ "Classical",
          Event == "Rated Rapid game" ~ "Rapid",
          Event == "Rated Bullet game" ~ "Bullet",
          Event == "Rated Blitz game" ~ "Blitz"
        ),
        year = as.integer(part$year),
        month = as.integer(part$month),
        
        # --- 2. NEW CALCULATED METRICS ---
        
        # Game ID
        game_id = str_replace_all(Site, "https://lichess.org/", ""),
        
        # Elo Calculations
        mean_elo = cast(round((cast(WhiteElo, float64()) + cast(BlackElo, float64())) / 2), int16()),
        elo_band = cast(floor(mean_elo / 100) * 100, int16()),
        
        # Win Rates
        white_score = case_when(
          Result == "1-0" ~ 1.0,
          Result == "0-1" ~ 0.0,
          Result == "1/2-1/2" ~ 0.5,
          TRUE ~ NA_real_
        ),
        
        # Move Count
        move_count = cast(str_count(movetext, "\\d+\\.\\s"), int16()),
        
        # Opening Family & Class
        opening_family = str_replace_all(Opening, ":.*", ""),
        eco_class = substring(ECO, 1, 1),

        # Has Eval
        has_eval = str_detect(movetext, fixed("[%eval")),
        
        # --- 3. TIME CONTROL PARSING (Safe Method) ---
        base_time_str = case_when(
            grepl("\\+", TimeControl) ~ gsub("^(\\d+)\\+.*", "\\1", TimeControl),
            TRUE ~ NA_character_
        ),
        
        increment_str = case_when(
            grepl("\\+", TimeControl) ~ gsub(".*\\+(\\d+)$", "\\1", TimeControl),
            TRUE ~ NA_character_
        ),
        
        # Cast to integers
        base_time = cast(base_time_str, int32()),
        increment = cast(increment_str, int32())
      ) |>
      # Select only the new snake_case columns and calculated fields
      select(
        site, white, black, result, white_title, black_title, 
        white_elo, black_elo, white_rating_diff, black_rating_diff, 
        utc_date, utc_time, eco, opening, termination, time_control, movetext,
        event, year, month, # Partitioning fields
        
        # Calculated fields
        game_id, mean_elo, elo_band, white_score, move_count, 
        opening_family, eco_class, has_eval, base_time, increment
      ) |>
      write_dataset(
        path = output_path,
        format = "parquet",
        partitioning = c("year", "month", "event"),
        
        # --- OPTIMIZED SETTINGS ---
        compression = "zstd",
        compression_level = 3,  
        version = "2.6",
        max_rows_per_file = 2000000,
        existing_data_behavior = "overwrite"
      )
    
    loop_end <- Sys.time()
    duration <- round(as.numeric(difftime(loop_end, loop_start, units = "mins")), 1)
    
    log_progress(glue("Finished {part$year}-{part$month} in {duration} mins"))
    completed_count <- completed_count + 1
    
  }, error = function(e) {
    # Log detailed error object
    log_progress(glue("ERROR in {part$year}-{part$month}:"))
    print(e)
    # Log to file as well
    cat(as.character(e), "\n", file = log_file, append = TRUE)
  })
  
  # Memory Cleanup
  gc()
}

total_end <- Sys.time()
total_hours <- round(as.numeric(difftime(total_end, total_start, units = 'hours')), 2)
log_progress(glue("=== COMPLETE === Total Runtime: {total_hours} hours"))