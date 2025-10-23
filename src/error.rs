use winnow::error::{ContextError, ParseError};

#[derive(thiserror::Error, Debug)]
pub enum SQLiteError {
    #[error(transparent)]
    IOError(#[from] std::io::Error),

    #[error("Parsing error: {0}")]
    ParsingError(String),

    #[error("unknown text encoding `{0}`")]
    UnknownTextEncodingError(u32),

    #[error("Query error {0}")]
    SqlQueryErr(#[from] turso_parser::error::Error),

    #[error("Table {0}, not found")]
    TableNotFound(String),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, SQLiteError>;

impl<I> From<ParseError<I, ContextError>> for SQLiteError
where
    I: std::fmt::Debug,
{
    fn from(err: ParseError<I, ContextError>) -> Self {
        SQLiteError::ParsingError(format!("{:?}", err))
    }
}

impl From<ContextError> for SQLiteError {
    fn from(err: ContextError) -> Self {
        SQLiteError::ParsingError(format!("{:?}", err))
    }
}
