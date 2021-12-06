use core::result;
use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("io error")]
    IO(#[from] io::Error),
    #[error("{0}")]
    TooLarge(String),
}

pub type Result<T> = result::Result<T, Error>;
