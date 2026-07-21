pub(crate) use shardline_xet_adapter::XorbVisitError;

use crate::FsckError;

pub(crate) fn map_xorb_visit_error_fsck(error: XorbVisitError<FsckError>) -> FsckError {
    match error {
        XorbVisitError::Parse(error) => FsckError::from(error),
        XorbVisitError::Visitor(error) => error,
    }
}
