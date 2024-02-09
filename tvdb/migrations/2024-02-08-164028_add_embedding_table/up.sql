CREATE TABLE title_basics_embeddings (
    tconst TEXT PRIMARY KEY,
    summary vector(384),
    primary_title vector(384),
    original_title vector(384),
    genres vector(384),
    FOREIGN KEY (tconst) REFERENCES title_basics(tconst)
);
