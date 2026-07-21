# shardline-vcs

Version control system provider adapters for GitHub, GitLab, Gitea, Codeberg,
and generic provider integration. Each adapter implements the `ProviderAdapter`
trait for repository access, reference resolution, webhook verification, and
token issuance. The generic adapter supports custom providers via configuration.
The `BuiltInProviderCatalog` wires adapters from config. Used by the server's
provider token and webhook subsystems.

See the [main Shardline README](../../README.md) for the project overview.
