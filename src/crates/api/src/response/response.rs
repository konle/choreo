use serde::Serialize;

#[derive(Serialize)]
pub struct Response<T> {
    pub code: i32,
    pub message: String,
    pub data: Option<T>,
}

impl<T> Response<T> {
    pub fn new(code: i32, message: String, data: Option<T>) -> Self {
        Self {
            code,
            message,
            data,
        }
    }

    pub fn success(data: T) -> Self {
        // User rule: code == 0 means success
        Self::new(0, "success".to_string(), Some(data))
    }

    pub fn error(code: i32, message: String) -> Self {
        Self::new(code, message, None)
    }
}
