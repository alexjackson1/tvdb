use diesel::{insert_into, PgConnection, RunQueryDsl};

use crate::{imdb::title_basics::TitleBasics, schema::title_basics::dsl::*};
use anyhow::Result;

pub fn insert_title_basics(values: Vec<TitleBasics>, conn: &mut PgConnection) -> Result<usize> {
    insert_into(title_basics)
        .values(&values)
        .execute(conn)
        .map_err(Into::into)
}
