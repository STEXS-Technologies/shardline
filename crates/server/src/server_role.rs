use thiserror::Error;

/// Runtime server role selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerRole {
    /// Serve both control-plane and transfer routes from one process.
    All,
    /// Serve control-plane and metadata routes only.
    Api,
    /// Serve large upload and download routes only.
    Transfer,
}

impl ServerRole {
    /// Parses a role token.
    ///
    /// # Errors
    ///
    /// Returns [`ServerRoleParseError`] when the token is not a supported role.
    pub fn parse(value: &str) -> Result<Self, ServerRoleParseError> {
        match value {
            "all" => Ok(Self::All),
            "api" => Ok(Self::Api),
            "transfer" => Ok(Self::Transfer),
            _ => Err(ServerRoleParseError),
        }
    }

    /// Returns the canonical role token.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Api => "api",
            Self::Transfer => "transfer",
        }
    }

    /// Returns whether this role serves metadata and control-plane routes.
    #[must_use]
    pub const fn serves_api(self) -> bool {
        matches!(self, Self::All | Self::Api)
    }

    /// Returns whether this role serves large upload and download routes.
    #[must_use]
    pub const fn serves_transfer(self) -> bool {
        matches!(self, Self::All | Self::Transfer)
    }

    /// Returns whether this role needs the reconstruction cache.
    #[must_use]
    pub const fn uses_reconstruction_cache(self) -> bool {
        self.serves_api()
    }
}

/// Invalid server role token.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
#[error("invalid server role")]
pub struct ServerRoleParseError;

#[cfg(test)]
mod tests {
    use super::ServerRole;

    #[test]
    fn parses_supported_roles() {
        assert_eq!(ServerRole::parse("all"), Ok(ServerRole::All));
        assert_eq!(ServerRole::parse("api"), Ok(ServerRole::Api));
        assert_eq!(ServerRole::parse("transfer"), Ok(ServerRole::Transfer));
    }

    #[test]
    fn api_role_only_serves_control_plane_routes() {
        assert!(ServerRole::Api.serves_api());
        assert!(!ServerRole::Api.serves_transfer());
        assert!(ServerRole::Api.uses_reconstruction_cache());
    }

    #[test]
    fn transfer_role_only_serves_transfer_routes() {
        assert!(!ServerRole::Transfer.serves_api());
        assert!(ServerRole::Transfer.serves_transfer());
        assert!(!ServerRole::Transfer.uses_reconstruction_cache());
    }
}
