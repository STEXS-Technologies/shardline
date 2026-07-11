use shardline_protocol::RepositoryProvider;

pub(crate) fn parse_repository_provider<E>(
    value: &str,
    invalid_provider_error: impl FnOnce() -> E,
) -> Result<RepositoryProvider, E> {
    value.parse().map_err(|_error| invalid_provider_error())
}

#[cfg(test)]
mod tests {
    use shardline_protocol::RepositoryProvider;

    use super::parse_repository_provider;

    #[test]
    fn parse_github() {
        let result: Result<RepositoryProvider, &str> =
            parse_repository_provider("github", || "unused");
        assert_eq!(result, Ok(RepositoryProvider::GitHub));
    }

    #[test]
    fn parse_gitea() {
        let result: Result<RepositoryProvider, &str> =
            parse_repository_provider("gitea", || "unused");
        assert_eq!(result, Ok(RepositoryProvider::Gitea));
    }

    #[test]
    fn parse_gitlab() {
        let result: Result<RepositoryProvider, &str> =
            parse_repository_provider("gitlab", || "unused");
        assert_eq!(result, Ok(RepositoryProvider::GitLab));
    }

    #[test]
    fn parse_codeberg() {
        let result: Result<RepositoryProvider, &str> =
            parse_repository_provider("codeberg", || "unused");
        assert_eq!(result, Ok(RepositoryProvider::Codeberg));
    }

    #[test]
    fn parse_generic() {
        let result: Result<RepositoryProvider, &str> =
            parse_repository_provider("generic", || "unused");
        assert_eq!(result, Ok(RepositoryProvider::Generic));
    }

    #[test]
    fn parse_invalid_error() {
        let result: Result<RepositoryProvider, &str> =
            parse_repository_provider("invalid", || "custom error");
        assert_eq!(result, Err("custom error"));
    }

    #[test]
    fn parse_empty_error() {
        let result: Result<RepositoryProvider, &str> =
            parse_repository_provider("", || "custom error");
        assert_eq!(result, Err("custom error"));
    }
}
