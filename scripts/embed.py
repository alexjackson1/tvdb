import enum
from typing import List, NamedTuple, Optional, TypedDict
import os

import numpy as np
from dotenv import load_dotenv

import psycopg2
from sentence_transformers import SentenceTransformer


SELECT_DATA_SQL = """
    SELECT tb.*
    FROM title_basics tb
    LEFT JOIN title_basics_embeddings tbe ON tb.tconst = tbe.tconst
    WHERE tbe.tconst IS NULL
    LIMIT %s
"""


class EmbeddingType(enum.Enum):
    SUMMARY = "summary"
    PRIMARY_TITLE = "primary_title"
    ORIGINAL_TITLE = "original_title"
    GENRES = "genres"


def connect_to_db() -> psycopg2.extensions.connection:
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
        )
        return conn
    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL database:", e)


class DataRow(NamedTuple):
    tconst: str
    title_type: str
    primary_title: str
    original_title: str
    is_adult: Optional[bool]
    start_year: Optional[int]
    end_year: Optional[int]
    runtime_minutes: Optional[int]
    genres: Optional[str]


def select_data(cursor: psycopg2.extensions.cursor, batch_size: int) -> List[DataRow]:
    cursor.execute(SELECT_DATA_SQL, (batch_size,))
    rows = cursor.fetchall()
    return [DataRow(*row) for row in rows]


class Sentence(NamedTuple):
    tconst: str
    type: EmbeddingType
    sentence: str


def to_summary_sentence(row: DataRow) -> str:
    s = ""
    s += f"{row.primary_title}"
    if row.start_year is not None:
        s += f" ({row.start_year}"
        if row.end_year is not None:
            s += f" - {row.end_year}"
        s += ")"

    if row.genres is not None:
        s += f" {row.genres.replace(',', ', ')}"
    return s


def to_sentences(item: DataRow) -> List[Sentence]:
    s = []

    s.append(Sentence(item.tconst, EmbeddingType.SUMMARY, to_summary_sentence(item)))
    s.append(Sentence(item.tconst, EmbeddingType.PRIMARY_TITLE, item.primary_title))

    if item.original_title != item.primary_title:
        s.append(
            Sentence(item.tconst, EmbeddingType.ORIGINAL_TITLE, item.original_title)
        )

    if item.genres is not None:
        s.append(Sentence(item.tconst, EmbeddingType.GENRES, item.genres))

    return s


class Embedding(NamedTuple):
    tconst: str
    type: EmbeddingType
    embedding: np.ndarray


def embed_sentences(sentences: List[Sentence]) -> List[Embedding]:
    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

    embeddings = model.encode([s.sentence for s in sentences])

    results = []
    for s, embedding in zip(sentences, embeddings):
        results.append(Embedding(s.tconst, s.type, embedding))

    return results


class EmbeddingRow(TypedDict):
    tconst: str
    summary: Optional[List[float]]
    primary_title: Optional[List[float]]
    original_title: Optional[List[float]]
    genres: Optional[List[float]]


def to_updates(embeddings: List[Embedding]) -> List[EmbeddingRow]:
    results = {}
    for e in embeddings:
        if e.tconst not in results:
            results[e.tconst] = {
                EmbeddingType.SUMMARY: None,
                EmbeddingType.PRIMARY_TITLE: None,
                EmbeddingType.ORIGINAL_TITLE: None,
                EmbeddingType.GENRES: None,
            }

        results[e.tconst][e.type] = e.embedding

    output = []
    for tconst, embeddings in results.items():
        output.append(
            {
                "tconst": tconst,
                "summary": (
                    None
                    if embeddings[EmbeddingType.SUMMARY] is None
                    else embeddings[EmbeddingType.SUMMARY].tolist()
                ),
                "primary_title": (
                    None
                    if embeddings[EmbeddingType.PRIMARY_TITLE] is None
                    else embeddings[EmbeddingType.PRIMARY_TITLE].tolist()
                ),
                "original_title": (
                    None
                    if embeddings[EmbeddingType.ORIGINAL_TITLE] is None
                    else embeddings[EmbeddingType.ORIGINAL_TITLE].tolist()
                ),
                "genres": (
                    None
                    if embeddings[EmbeddingType.GENRES] is None
                    else embeddings[EmbeddingType.GENRES].tolist()
                ),
            }
        )

    return output


# Function to store embeddings in the database
def store_embeddings(
    cursor: psycopg2.extensions.cursor, embeddings: List[EmbeddingRow]
):
    stmt = """INSERT INTO title_basics_embeddings (tconst, summary, primary_title, original_title, genres) VALUES"""
    placeholders = ()
    for i, embedding in enumerate(embeddings):
        stmt += f"(%s, %s, %s, %s, %s)"
        if i < len(embeddings) - 1:
            stmt += ","

        placeholders += (
            embedding["tconst"],
            embedding["summary"],
            embedding["primary_title"],
            embedding["original_title"],
            embedding["genres"],
        )

    cursor.execute(stmt, placeholders)


def main():
    load_dotenv()
    conn = connect_to_db()

    batch_size = 2_500

    while True:
        cursor = conn.cursor()
        print("Selecting data...")
        data = select_data(cursor, batch_size)
        if len(data) == 0:
            print("No more data")
            break

        sentences = []
        for item in data:
            sentences.extend(to_sentences(item))

        print("Embedding sentences...")
        embeddings = embed_sentences(sentences)

        print("Storing embeddings...")
        updates = to_updates(embeddings)

        store_embeddings(cursor, updates)

        print("done.\n")
        conn.commit()
        cursor.close()

    conn.close()


if __name__ == "__main__":
    main()
