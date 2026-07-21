use shardline_protocol::RepositoryProvider;

pub(crate) fn parse_repository_provider<E>(
    value: &str,
    invalid_provider_error: impl FnOnce(String) -> E,
) -> Result<RepositoryProvider, E> {
    value
        .parse::<RepositoryProvider>()
        .map_err(|e| invalid_provider_error(e.to_string()))
}

#[cfg(test)]
mod tests {
    use shardline_protocol::RepositoryProvider;

    use super::parse_repository_provider;

    #[test]
    fn parse_github() {
        let result: Result<RepositoryProvider, &str> =
            parse_repository_provider("github", |_| "unused");
        assert_eq!(result, Ok(RepositoryProvider::GitHub));
    }

    #[test]
    fn parse_gitea() {
        let result: Result<RepositoryProvider, &str> =
            parse_repository_provider("gitea", |_| "unused");
        assert_eq!(result, Ok(RepositoryProvider::Gitea));
    }

    #[test]
    fn parse_gitlab() {
        let result: Result<RepositoryProvider, &str> =
            parse_repository_provider("gitlab", |_| "unused");
        assert_eq!(result, Ok(RepositoryProvider::GitLab));
    }

    #[test]
    fn parse_codeberg() {
        let result: Result<RepositoryProvider, &str> =
            parse_repository_provider("codeberg", |_| "unused");
        assert_eq!(result, Ok(RepositoryProvider::Codeberg));
    }

    #[test]
    fn parse_generic() {
        let result: Result<RepositoryProvider, &str> =
            parse_repository_provider("generic", |_| "unused");
        assert_eq!(result, Ok(RepositoryProvider::Generic));
    }

    #[test]
    fn parse_invalid_error() {
        let result: Result<RepositoryProvider, &str> =
            parse_repository_provider("invalid", |_| "custom error");
        assert_eq!(result, Err("custom error"));
    }

    #[test]
    fn parse_empty_error() {
        let result: Result<RepositoryProvider, &str> =
            parse_repository_provider("", |_| "custom error");
        assert_eq!(result, Err("custom error"));
    }
}
