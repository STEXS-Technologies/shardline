use shardline_protocol::RepositoryProvider;

pub(crate) fn parse_repository_provider<E>(
    value: &str,
    invalid_provider_error: impl FnOnce() -> E,
) -> Result<RepositoryProvider, E> {
    value.parse().map_err(|_error| invalid_provider_error())
}
