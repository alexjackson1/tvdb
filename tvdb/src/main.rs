use std::{
    collections::HashMap,
    env,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
    time::Duration,
};

use anyhow::{Context, Result};
use diesel::{Connection, PgConnection};
use dotenvy::dotenv;
use imdb::IMDB_FILE_NAMES;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use message::{RecordKind, Sender};

pub mod imdb;
pub mod message;
pub mod read;
pub mod schema;
pub mod write;

pub fn establish_connection() -> PgConnection {
    dotenv().ok();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    PgConnection::establish(&database_url)
        .unwrap_or_else(|_| panic!("Error connecting to {}", database_url))
}

fn handle_error(e: &anyhow::Error) -> ! {
    eprintln!("An error occurred!\n\n{}\n", e);
    for (i, cause) in e.chain().skip(1).enumerate() {
        if i == 0 {
            eprintln!("Causes:");
        }
        eprintln!("{}. {}", i + 1, cause);
    }
    std::process::exit(1);
}

fn spawn_count_thread(path: PathBuf) -> JoinHandle<Result<u64>> {
    thread::spawn(move || {
        let count = read::count_lines(path)?;
        Ok::<u64, anyhow::Error>(count)
    })
}

#[derive(Debug, Clone)]
pub struct ReadProgress {
    pub selected: u64,
    pub read: u64,
}

impl Default for ReadProgress {
    fn default() -> Self {
        Self {
            selected: 0,
            read: 0,
        }
    }
}

fn spawn_read_thread<P>(
    path: P,
    kind: RecordKind,
    sender: Sender,
    progress: Arc<Mutex<ReadProgress>>,
) -> thread::JoinHandle<Result<()>>
where
    P: AsRef<Path> + Send + Sync,
{
    let file_path = path.as_ref().to_owned();
    let handle = match kind {
        RecordKind::TitleBasics => thread::spawn(move || {
            read::read_title_basics(&file_path, sender, progress)?;
            Ok::<(), anyhow::Error>(())
        }),
        _ => todo!(),
    };

    handle
}

fn write_data(
    v: &mut Vec<imdb::title_basics::TitleBasics>,
    count: &Arc<Mutex<u64>>,
) -> Result<usize> {
    let mut conn = establish_connection();
    let records = write::insert_title_basics(v.drain(..).collect(), &mut conn)?;

    let mut count_lck = count.lock().unwrap();
    *count_lck += records as u64;
    drop(count_lck);

    Ok(records)
}

fn spawn_db_thread(
    sender: Sender,
    receiver: crossbeam::channel::Receiver<message::Message>,
    count: Arc<Mutex<u64>>,
    batch_size: usize,
) -> thread::JoinHandle<Result<()>> {
    let mut queues = HashMap::new();
    thread::spawn(move || {
        loop {
            match receiver.recv().unwrap() {
                message::Message::TitleBasics(data) => {
                    let v = queues
                        .entry(message::RecordKind::TitleBasics)
                        .or_insert_with(Vec::new);

                    v.push(data);

                    if v.len() >= batch_size {
                        write_data(v, &count)?;
                    }
                }
                message::Message::Done(_) => {
                    let v = queues
                        .entry(message::RecordKind::TitleBasics)
                        .or_insert_with(Vec::new);

                    write_data(v, &count)?;
                    sender.send(message::Message::Done(message::RecordKind::TitleBasics))?;
                    break;
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    })
}

fn bar_style() -> ProgressStyle {
    ProgressStyle::with_template(
        "[{prefix:.bold.dim}] {spinner:.green} {msg} [{elapsed_precise}] [{bar:40.cyan/blue}] {human_pos}/{human_len} ({eta})",
    )
    .unwrap()
    .progress_chars("#>-")
    .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏✔")
}

fn progress_bar(
    mp: &MultiProgress,
    index: usize,
    prefix: String,
    msg: String,
    len: u64,
) -> ProgressBar {
    let pb = mp.insert(index, ProgressBar::new(len));
    pb.set_style(bar_style());
    pb.set_prefix(prefix);
    pb.set_message(msg);
    pb
}

fn spinner_style() -> ProgressStyle {
    ProgressStyle::default_spinner()
        .template("[{prefix:.bold.dim}] {spinner:.green} {msg} [{elapsed_precise}]")
        .unwrap()
        .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏✔")
        .progress_chars("#>-")
}

fn progress_spinner(mp: &MultiProgress, index: usize, prefix: String, msg: String) -> ProgressBar {
    let pb = mp.insert(index, ProgressBar::new_spinner());
    pb.set_style(spinner_style());
    pb.set_prefix(prefix);
    pb.set_message(msg);
    pb.enable_steady_tick(Duration::from_millis(20));
    pb
}

fn process<P>(
    path: P,
    data_workers: usize,
    batch_size: usize,
    mp: MultiProgress,
) -> JoinHandle<Result<()>>
where
    P: AsRef<Path> + Send + Sync,
{
    let path = path.as_ref().to_owned();
    thread::spawn(move || {
        let (tx, rx) = crossbeam::channel::unbounded();

        // Count the lines in the file
        let read_pb = progress_spinner(
            &mp,
            0,
            IMDB_FILE_NAMES[0].to_string(),
            "Counting lines...".to_string(),
        );
        let count_handle = spawn_count_thread(path.clone());

        // Read the records from the file
        let rc_read_progress = Arc::new(Mutex::new(ReadProgress::default()));
        let read_handle = spawn_read_thread(
            path.clone(),
            RecordKind::TitleBasics,
            tx.clone(),
            rc_read_progress.clone(),
        );

        // Write the records to the database
        let write_pb = progress_spinner(
            &mp,
            1,
            IMDB_FILE_NAMES[0].to_string(),
            "Writing records...".to_string(),
        );
        let write_count: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));
        let mut write_handles = Vec::new();
        for _ in 0..data_workers {
            let sender = tx.clone();
            let receiver = rx.clone();
            write_handles.push(spawn_db_thread(
                sender,
                receiver,
                write_count.clone(),
                batch_size,
            ));
        }

        // Wait for the count thread to finish
        let line_count = count_handle
            .join()
            .map_err(|_| anyhow::anyhow!("Error joining threads"))
            .context("Failed to count lines")?
            .context("Failed to count lines")?;

        read_pb.finish_and_clear();

        // Update read spinner to progress bar
        let read_pb = progress_bar(
            &mp,
            0,
            IMDB_FILE_NAMES[0].to_string(),
            "Reading records...".to_string(),
            line_count,
        );

        let mut first = true;
        while !read_handle.is_finished() {
            let progress = rc_read_progress.lock().unwrap();
            read_pb.set_position(progress.read);
            drop(progress);

            if first {
                read_pb.reset_eta();
                first = false;
            }

            thread::sleep(std::time::Duration::from_millis(15));
        }

        // Wait for the read thread to finish
        read_handle.join().unwrap()?;

        let progress = rc_read_progress.lock().unwrap();
        let read_count = progress.read.clone();
        let selected_count = progress.selected.clone();
        drop(progress);

        read_pb.finish_with_message(format!(
            "Read {} records, selected {}.",
            read_count, selected_count
        ));
        write_pb.finish_and_clear();

        // Update write spinner to progress bar
        let write_pb = progress_bar(
            &mp,
            1,
            IMDB_FILE_NAMES[0].to_string(),
            "Writing records...".to_string(),
            selected_count,
        );
        loop {
            let count = write_count.lock().unwrap();
            let progress = rc_read_progress.lock().unwrap();
            if *count == progress.selected {
                break;
            }
            write_pb.set_position(*count);
            drop(count);
            thread::sleep(std::time::Duration::from_millis(15));
        }

        // Wait for the write threads to finish
        for handle in write_handles {
            handle.join().unwrap()?;
        }

        write_pb.finish_with_message(format!("Wrote {} records.", selected_count));

        Ok(())
    })
}

fn main() {
    let mp = MultiProgress::new();
    process("imdb/data/title.basics.tsv.gz", 8, 1_000, mp)
        .join()
        .unwrap()
        .unwrap();
}
