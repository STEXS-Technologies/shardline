use crate::ServerError;

pub(crate) const fn checked_add(left: u64, right: u64) -> Result<u64, ServerError> {
    match left.checked_add(right) {
        Some(value) => Ok(value),
        None => Err(ServerError::Overflow),
    }
}

pub(crate) const fn checked_increment(value: u64) -> Result<u64, ServerError> {
    checked_add(value, 1)
}
