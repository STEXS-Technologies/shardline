use shardline_protocol::XorbVisitError;

use crate::ServerError;

pub(crate) fn map_xorb_visit_error(error: XorbVisitError<ServerError>) -> ServerError {
    match error {
        XorbVisitError::Parse(error) => ServerError::from(error),
        XorbVisitError::Visitor(error) => error,
    }
}
