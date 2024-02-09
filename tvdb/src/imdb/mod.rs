mod parse;

pub mod title_basics;

pub const IMDB_FILE_NAMES: [&str; 7] = [
    "title.basics.tsv.gz",
    "title.akas.tsv.gz",
    "title.ratings.tsv.gz",
    "title.principals.tsv.gz",
    "name.basics.tsv.gz",
    "title.crew.tsv.gz",
    "title.episode.tsv.gz",
];
