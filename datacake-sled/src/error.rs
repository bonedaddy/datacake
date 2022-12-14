use thiserror::Error;
#[derive(Debug, Error)]
pub enum SledStorageError {
    #[error("{0}")]
    /// An error has occurred while serializing data
    SerializaztionFailure(String),
}
