use crate::error::XetAdapterError;

use super::XorbVisitError;

pub fn map_xorb_visit_error(error: XorbVisitError<XetAdapterError>) -> XetAdapterError {
    match error {
        XorbVisitError::Parse(error) => XetAdapterError::from(error),
        XorbVisitError::Visitor(error) => error,
    }
}
