use std::{
    io::BufRead,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result};
use diesel::{Insertable, Queryable, Selectable};

use crate::{
    imdb::parse,
    message::{Message, RecordKind, Sender},
    schema::title_basics,
    ReadProgress,
};

#[derive(Debug, PartialEq, Queryable, Selectable, Insertable, Clone)]
#[diesel(table_name = title_basics)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct TitleBasics {
    pub tconst: String,
    pub title_type: String,
    pub primary_title: String,
    pub original_title: String,
    pub is_adult: bool,
    pub start_year: Option<i32>,
    pub end_year: Option<i32>,
    pub runtime_minutes: Option<f32>,
    pub genres: Option<String>,
}

impl TryFrom<String> for TitleBasics {
    type Error = anyhow::Error;

    fn try_from(line: String) -> Result<Self> {
        let mut fields = line.split("\t");
        let err = || anyhow::anyhow!("Failed to parse line");
        let tconst = fields.next().ok_or_else(err)?;
        let title_type = fields.next().ok_or_else(err)?;
        let primary_title = fields.next().ok_or_else(err)?;
        let original_title = fields.next().ok_or_else(err)?;
        let is_adult = fields.next().ok_or_else(err)?;
        let start_year = fields.next().ok_or_else(err)?;
        let end_year = fields.next().ok_or_else(err)?;
        let runtime_minutes = fields.next().ok_or_else(err)?;
        let genres = fields.next().ok_or_else(err)?;

        Ok(TitleBasics {
            tconst: tconst.to_string(),
            title_type: title_type.to_string(),
            primary_title: primary_title.to_string(),
            original_title: original_title.to_string(),
            is_adult: parse::remove_na(is_adult)
                .map(|ia| ia == "1")
                .unwrap_or(false),
            start_year: parse::parse_int(start_year)?,
            end_year: parse::parse_int(end_year)?,
            runtime_minutes: parse::parse_float(runtime_minutes)?,
            genres: parse::remove_na(genres).map(|s| s.to_string()),
        })
    }
}

fn update_progress(progress: &Arc<Mutex<ReadProgress>>, selected: bool) {
    let mut progress = progress.lock().unwrap();
    progress.selected += selected as u64;
    progress.read += 1;
    drop(progress);
}

pub fn run<R>(reader: R, sender: Sender, progress: Arc<Mutex<ReadProgress>>) -> Result<()>
where
    R: BufRead,
{
    let mut sender = sender;
    // let mut count = 0;

    // Process each line in the file
    for (_, line) in reader.lines().enumerate().skip(1) {
        let result = process_item(&mut sender, line.context("Failed to read line")?)?;
        update_progress(&progress, result.is_some());
    }

    // Stop the senders
    sender
        .send(Message::done(RecordKind::TitleBasics))
        .context("Failed to send `done` message")?;

    Ok(())
}

fn process_item(sender: &mut Sender, line: String) -> Result<Option<()>> {
    let data = TitleBasics::try_from(line).context("Failed to parse line")?;

    match data.title_type.as_str() {
        "tvSeries" => Ok(Some(sender.send(Message::title_basics(data))?)),
        _ => Ok(None),
    }
}
