use crate::error::XetAdapterError;
use crate::xorb::XorbParseError;

use super::XorbVisitError;

#[must_use]
pub fn map_xorb_visit_error(error: XorbVisitError<XetAdapterError>) -> XetAdapterError {
    match error {
        XorbVisitError::Parse(error) => XetAdapterError::from(error),
        XorbVisitError::Visitor(error) => error,
    }
}

#[cfg(test)]
mod tests {
    use crate::{XetAdapterError, xorb::XorbParseError};
    use super::{XorbVisitError, map_xorb_visit_error};

    #[test]
    fn parse_variant_maps_hash_mismatch() {
        let err = XorbVisitError::<XetAdapterError>::Parse(XorbParseError::HashMismatch);
        let result = map_xorb_visit_error(err);
        assert!(
            matches!(result, XetAdapterError::XorbHashMismatch),
            "expected XorbHashMismatch, got {result:?}"
        );
    }

    #[test]
    fn visitor_variant_returns_inner_unchanged() {
        let inner = XetAdapterError::NotFound;
        let err = XorbVisitError::<XetAdapterError>::Visitor(inner);
        let result = map_xorb_visit_error(err);
        assert!(
            matches!(result, XetAdapterError::NotFound),
            "expected NotFound (inner unchanged), got {result:?}"
        );
    }
}
