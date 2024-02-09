use crate::imdb::title_basics::TitleBasics;

#[derive(Debug, PartialEq, Eq, Copy, Clone, Hash)]
pub enum RecordKind {
    TitleBasics,
    TitleGenres,
    TitleAkas,
    TitleCrew,
    TitleEpisodes,
    TitlePrincipals,
    NameBasics,
    TitleRatings,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Message {
    TitleBasics(TitleBasics),
    Done(RecordKind),
}

impl Message {
    pub fn title_basics(data: TitleBasics) -> Self {
        Message::TitleBasics(data)
    }

    pub fn done(kind: RecordKind) -> Self {
        Message::Done(kind)
    }
}

pub type Sender = crossbeam::channel::Sender<Message>;
pub type Receiver = crossbeam::channel::Receiver<Message>;
