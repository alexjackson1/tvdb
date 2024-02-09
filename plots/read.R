library(RPostgreSQL)

dotenv::load_dot_env("../.env")

# Database connection parameters
dbname <- Sys.getenv("POSTGRES_DB")
user <- Sys.getenv("POSTGRES_USER")
password <- Sys.getenv("POSTGRES_PASSWORD")
host <- Sys.getenv("POSTGRES_HOST")
port <- 5432

# Establish a connection
con <- dbConnect(dbDriver("PostgreSQL"), dbname = dbname, user = user, password = password, host = host, port = port)

# Execute a query to retrieve data
your_query <- "SELECT tbe.* FROM title_basics_embeddings tbe JOIN title_basics tb ON tb.tconst = tbe.tconst WHERE tb.start_year >= 2000 ORDER BY RANDOM() LIMIT 5000"
data <- dbGetQuery(con, your_query)

# Close the database connection
dbDisconnect(con)


library(tidyverse)
data_v <- data %>%
  pull("summary") %>%
  str_replace_all("(\\]|\\[)", "") %>%
  str_split(",") %>%
  map( ~ as.numeric(.x))

matrix_data <- do.call(rbind, data_v)

library(tsne)
tsne_results <- tsne(matrix_data)

df <- cbind(data, tsne_results) %>%
  rename(X1 = 6, X2 = 7)

ggplot(df, aes(x = X1, y = X2)) +
  geom_point() +
  labs(title = "t-SNE Visualization of TV Show Embeddings")
