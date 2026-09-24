use std::env::{self, VarError};

#[cfg(test)]
use std::{cell::RefCell, collections::HashMap};

#[cfg(test)]
thread_local! {
    static TEST_ENVIRONMENT: RefCell<HashMap<String, Option<String>>> =
        RefCell::new(HashMap::new());
}

pub(super) fn var(key: &str) -> Result<String, VarError> {
    #[cfg(test)]
    if let Some(value) = TEST_ENVIRONMENT.with(|environment| environment.borrow().get(key).cloned())
    {
        return value.ok_or(VarError::NotPresent);
    }

    env::var(key)
}

#[cfg(test)]
pub(super) fn set_test_var(key: &str, value: &str) {
    TEST_ENVIRONMENT.with(|environment| {
        environment
            .borrow_mut()
            .insert(key.to_owned(), Some(value.to_owned()));
    });
}

#[cfg(test)]
pub(super) fn remove_test_var(key: &str) {
    TEST_ENVIRONMENT.with(|environment| {
        environment.borrow_mut().insert(key.to_owned(), None);
    });
}
