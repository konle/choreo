use std::fmt;

#[derive(Debug)]
pub enum RepositoryError {
    DatabaseError(String),
    NotFound(String),
}

impl fmt::Display for RepositoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DatabaseError(msg) => write!(f, "database error: {}", msg),
            Self::NotFound(msg) => write!(f, "not found: {}", msg),
        }
    }
}

impl std::error::Error for RepositoryError {}

impl From<String> for RepositoryError {
    fn from(s: String) -> Self {
        Self::DatabaseError(s)
    }
}

impl From<mongodb::error::Error> for RepositoryError {
    fn from(e: mongodb::error::Error) -> Self {
        Self::DatabaseError(e.to_string())
    }
}
