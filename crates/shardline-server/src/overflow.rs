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

#[cfg(test)]
mod tests {
    use crate::ServerError;

    #[test]
    fn checked_add_zero_zero() {
        assert!(matches!(super::checked_add(0, 0), Ok(0)));
    }

    #[test]
    fn checked_add_basic() {
        assert!(matches!(super::checked_add(5, 3), Ok(8)));
    }

    #[test]
    fn checked_add_overflow() {
        let result = super::checked_add(u64::MAX, 1);
        assert!(matches!(result, Err(ServerError::Overflow)));
    }

    #[test]
    fn checked_increment_basic() {
        assert!(matches!(super::checked_increment(0), Ok(1)));
    }

    #[test]
    fn checked_increment_overflow() {
        let result = super::checked_increment(u64::MAX);
        assert!(matches!(result, Err(ServerError::Overflow)));
    }
}
