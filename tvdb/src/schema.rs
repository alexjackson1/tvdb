// @generated automatically by Diesel CLI.

pub mod sql_types {
    #[derive(diesel::query_builder::QueryId, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "vector"))]
    pub struct Vector;
}

diesel::table! {
    title_basics (tconst) {
        tconst -> Text,
        title_type -> Text,
        primary_title -> Text,
        original_title -> Text,
        is_adult -> Bool,
        start_year -> Nullable<Int4>,
        end_year -> Nullable<Int4>,
        runtime_minutes -> Nullable<Float4>,
        genres -> Nullable<Text>,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::Vector;

    title_basics_embeddings (tconst) {
        tconst -> Text,
        summary -> Nullable<Vector>,
        primary_title -> Nullable<Vector>,
        original_title -> Nullable<Vector>,
        genres -> Nullable<Vector>,
    }
}

diesel::joinable!(title_basics_embeddings -> title_basics (tconst));

diesel::allow_tables_to_appear_in_same_query!(
    title_basics,
    title_basics_embeddings,
);
