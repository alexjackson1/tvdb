use anyhow::{Context, Result};
use std::{
    io::{BufRead, BufReader},
    path::Path,
    sync::{Arc, Mutex},
};

use crate::message::Sender;
use crate::{imdb::title_basics::run as process_title_basics, ReadProgress};

pub fn create_decoder<P>(path: P) -> anyhow::Result<flate2::read::GzDecoder<std::fs::File>>
where
    P: AsRef<std::path::Path> + std::fmt::Debug,
{
    let file = std::fs::File::open(&path).context(format!("can't open file {:#?}", &path))?;
    let decoder = flate2::read::GzDecoder::new(file);
    Ok(decoder)
}

pub fn count_lines<P>(path: P) -> Result<u64>
where
    P: AsRef<Path> + Send + std::fmt::Debug,
{
    let decoder = create_decoder(&path)?;
    let mut lines = BufReader::new(decoder).lines();
    let count = lines.try_fold(0, |acc, line| line.map(|_| acc + 1))?;
    Ok(count)
}

pub fn read_title_basics<P>(
    path: P,
    sender: Sender,
    progress: Arc<Mutex<ReadProgress>>,
) -> Result<()>
where
    P: AsRef<Path> + std::fmt::Debug,
{
    let decoder = create_decoder(&path)?;
    let mut reader = BufReader::new(decoder);
    process_title_basics(&mut reader, sender, progress)
}
