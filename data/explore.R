library("tidyverse")

TITLE_BASICS_PATH <- "imdb/data/title.basics.tsv.gz"

# Read in the title basics data
df <- read_tsv(
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

problem_rows <- df[unique(problems(df)$row), ]
# View(problem_rows)


sample <- read_csv("imdb/sample.txt", col_names = c("title"))

df_tv <- df %>%
  filter(titleType == "tvSeries")

df_tv %>%
  filter(primaryTitle %in% sample$title) %>%
  # filter(is.na(startYear)) %>%
  filter(primaryTitle == "The Fall")

df_tv %>%
# group by duplicates on primaryTitle
  mutate(
    naCount = is.na(startYear) + is.na(endYear) + is.na(runtimeMinutes) + is.na(genres),
    genresLength = str_length(genres)
  ) %>%
  group_by(primaryTitle, originalTitle) %>%
  # filter out groups with only one row
  filter(n() > 1) %>%
  arrange(desc(primaryTitle)) %>%
  # filter out non-maximum naCount
  filter(naCount == max(naCount))
  


df_tv %>%
  arrange(desc(startYear)) %>%
  ggplot(aes(x = startYear)) +
  geom_histogram(binwidth = 1) +
  labs(
    title = "Number of TV Series by Year",
    x = "Year",
    y = "Number of TV Series"
  )

df_tv %>%
  filter(as.logical(isAdult)) %>%
  # remove outliers
  filter(runtimeMinutes < 60 * 5) %>%
  ggplot(aes(x = runtimeMinutes)) +
  geom_histogram(binwidth = 5) +
  labs(
    title = "Runtime of TV Series",
    x = "Runtime (minutes)",
    y = "Number of TV Series"
  ) %>%

