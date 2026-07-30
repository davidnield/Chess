library(conflicted)
library(tidyverse)
library(arrow)
library(fs)
library(glue)

conflicts_prefer(dplyr::filter)
conflicts_prefer(dplyr::collect)
conflicts_prefer(dplyr::mutate)

# --- Configuration ---
source_path <- "D:/data/chess/standard-chess-games-compressed/"
output_path <- "D:/data/chess/opening-explorer/"
log_file    <- path(output_path, "processing_log.txt")

if (!dir_exists(output_path)) dir_create(output_path)
cat(glue("Log started at {Sys.time()}\n"), file = log_file, append = FALSE)

# --- Helpers ---
log_progress <- function(message) {
  msg <- glue("[{format(Sys.time(), '%Y-%m-%d %H:%M:%S')}] {message}")
  cat(msg, "\n")
  cat(msg, "\n", file = log_file, append = TRUE)
}

extract_moves <- function(moves, N = 20) {
  r <- data.frame(
    t(sapply(moves, function(i) i[seq_len(N * 2)])),
    stringsAsFactors = FALSE
  )
  colnames(r) <- paste0(rep(c("W", "B"), times = N), rep(seq_len(N), each = 2))
  rownames(r) <- NULL
  r
}

to_rating_class <- function(elo_band) {
  cut(
    as.integer(elo_band),
    breaks = c(-Inf, 1000, 1200, 1400, 1600, 1800, 2000, 2200, 2500, Inf),
    labels = c("under_1000", "1000_1200", "1200_1400", "1400_1600",
               "1600_1800", "1800_2000", "2000_2200", "2200_2500", "over_2500"),
    right = FALSE
  ) |> as.character()
}

# --- Open dataset and build partition list ---
ds <- open_dataset(source_path, format = "parquet")

partition_df <- ds |>
  select(year, month, event) |>
  distinct() |>
  collect() |>
  arrange(year, month, event)

total_partitions <- nrow(partition_df)
total_start      <- Sys.time()
completed_count  <- 0

log_progress(glue("Found {total_partitions} partitions to process."))

# --- Main loop ---
for (i in seq_len(total_partitions)) {
  part <- partition_df[i, ]

  avg_mins <- if (completed_count > 0) {
    as.numeric(difftime(Sys.time(), total_start, units = "mins")) / completed_count
  } else NA
  eta_msg <- if (!is.na(avg_mins)) {
    glue("| Est. remaining: {round(avg_mins * (total_partitions - i + 1) / 60, 1)} hours")
  } else "| Est. remaining: Calculating..."

  log_progress(glue(
    "Starting [{i}/{total_partitions}] {part$year}-{sprintf('%02d', part$month)} {part$event} {eta_msg}"
  ))

  tryCatch({
    chunk <- ds |>
      filter(year == part$year, month == part$month, event == part$event,
             !is.na(elo_band)) |>
      select(result, movetext, elo_band) |>
      collect() |>
      mutate(
        rating_class = to_rating_class(elo_band),
        clean_pgn    = str_replace_all(movetext, " \\{[^\\}]*\\}", ""),
        tmp          = str_replace_all(clean_pgn, "\\d+\\.{1,3}\\s*", ""),
        tmp          = str_squish(tmp),
        moves        = str_split(tmp, " ")
      )

    move_cols <- extract_moves(chunk$moves, N = 20)

    result_df <- bind_cols(chunk |> select(result, rating_class), move_cols) |>
      mutate(result = case_when(
        result == "1-0"     ~ "white_wins",
        result == "0-1"     ~ "black_wins",
        result == "1/2-1/2" ~ "draws",
        TRUE                ~ NA_character_
      )) |>
      filter(!is.na(result)) |>
      group_by(rating_class, across(W1:B20), result) |>
      count(name = "n") |>
      ungroup() |>
      pivot_wider(names_from = result, values_from = n, values_fill = 0L)

    # Ensure all result columns exist even if no games of that type occurred
    for (col in c("white_wins", "black_wins", "draws")) {
      if (!col %in% names(result_df)) result_df[[col]] <- 0L
    }

    result_df <- result_df |>
      mutate(
        total_games = white_wins + black_wins + draws,
        year  = part$year,
        month = part$month,
        event = part$event
      )

    write_dataset(
      result_df,
      path = output_path,
      format = "parquet",
      partitioning = c("year", "month", "event", "rating_class"),
      compression = "zstd",
      compression_level = 3,
      existing_data_behavior = "overwrite"
    )

    completed_count <- completed_count + 1
    log_progress(glue(
      "Finished [{i}/{total_partitions}] {part$year}-{sprintf('%02d', part$month)} {part$event}"
    ))

  }, error = function(e) {
    log_progress(glue(
      "ERROR in {part$year}-{sprintf('%02d', part$month)} {part$event}: {e$message}"
    ))
  })

  gc()
}

total_hours <- round(as.numeric(difftime(Sys.time(), total_start, units = "hours")), 2)
log_progress(glue("=== COMPLETE === Total runtime: {total_hours} hours"))
