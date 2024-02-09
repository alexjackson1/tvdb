library("tidyverse")

DATA_PATH <- "imdb/data/"
TITLE_BASICS_PATH <- paste0(DATA_PATH, "title.basics.tsv.gz")
TITLE_PRINCIPALS_PATH <- paste0(DATA_PATH, "title.principals.tsv.gz")
TITLE_RATINGS_PATH <- paste0(DATA_PATH, "title.ratings.tsv.gz")
NAME_BASICS_PATH <- paste0(DATA_PATH, "name.basics.tsv.gz")
TITLE_CREW_PATH <- paste0(DATA_PATH, "title.crew.tsv.gz")
TITLE_AKAS_PATH <- paste0(DATA_PATH, "title.akas.tsv.gz")

# Read in the title basics data
title_basics <- read_tsv(
  TITLE_BASICS_PATH, 
  col_types = cols(
    tconst = col_character(),
    titleType = col_character(),
    primaryTitle = col_character(),
    originalTitle = col_character(),
    isAdult = col_integer(),  
    startYear = col_integer(),
    endYear = col_integer(),
    runtimeMinutes = col_integer(),
    genres = col_character()
  ),
  show_col_types = TRUE,
  na = "\\N"
)

# count nas across columns
title_basics %>%
  summarise_all(funs(sum(is.na(.))))


# find all duplicate tconst
# duplicated_tconst <- title_basics %>% 
#   group_by(tconst) %>% 
#   filter(n() > 1) %>% 
#   select(tconst) %>% 
#   distinct()
