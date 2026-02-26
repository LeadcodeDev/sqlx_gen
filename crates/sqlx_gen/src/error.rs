use std::io;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("{0}")]
    Config(String),
}

pub type Result<T> = std::result::Result<T, Error>;
