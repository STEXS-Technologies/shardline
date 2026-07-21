use serde::Serialize;

#[derive(Debug, Serialize)]
pub(crate) struct ErrorBody {
    pub(crate) error: String,
}
