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
    fn checked_add_preserves_values_and_rejects_overflow() {
        for (left, right, expected) in [
            (0, 0, Some(0)),
            (5, 3, Some(8)),
            (u64::MAX, 0, Some(u64::MAX)),
            (u64::MAX - 1, 1, Some(u64::MAX)),
            (u64::MAX, 1, None),
        ] {
            let actual = super::checked_add(left, right);
            match expected {
                Some(expected) => assert!(matches!(actual, Ok(value) if value == expected)),
                None => assert!(matches!(actual, Err(ServerError::Overflow))),
            }
        }
    }

    #[test]
    fn checked_increment_preserves_values_and_rejects_overflow() {
        assert!(matches!(super::checked_increment(0), Ok(1)));
        assert!(matches!(
            super::checked_increment(u64::MAX - 1),
            Ok(u64::MAX)
        ));
        assert!(matches!(
            super::checked_increment(u64::MAX),
            Err(ServerError::Overflow)
        ));
    }
}
