//! Typed JWT `alg` header values.
//!
//! JWT algorithm identifiers are **case-sensitive** per RFC 7518. This module
//! centralizes parsing and classification so callers can match on a small enum
//! instead of hand-comparing raw strings — preventing a future typo from
//! silently disabling the algorithm-confusion guard.

use std::str::FromStr;

/// A JWT `alg` header value as a typed enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JwtAlgorithm {
    /// RSASSA-PKCS1-v1_5 using SHA-256.
    Rs256,
    /// RSASSA-PKCS1-v1_5 using SHA-384.
    Rs384,
    /// RSASSA-PKCS1-v1_5 using SHA-512.
    Rs512,
    /// ECDSA using P-256 and SHA-256.
    Es256,
    /// ECDSA using P-384 and SHA-384.
    Es384,
    /// ECDSA using P-521 and SHA-512.
    Es512,
    /// HMAC using SHA-256.
    Hs256,
    /// HMAC using SHA-384.
    Hs384,
    /// HMAC using SHA-512.
    Hs512,
    /// EdDSA signature scheme.
    EdDsa,
    /// Unauthenticated token (``alg` = "none"`).
    None,
}

impl JwtAlgorithm {
    /// Returns the canonical JWT algorithm string.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rs256 => "RS256",
            Self::Rs384 => "RS384",
            Self::Rs512 => "RS512",
            Self::Es256 => "ES256",
            Self::Es384 => "ES384",
            Self::Es512 => "ES512",
            Self::Hs256 => "HS256",
            Self::Hs384 => "HS384",
            Self::Hs512 => "HS512",
            Self::EdDsa => "EdDSA",
            Self::None => "none",
        }
    }

    /// Returns `true` for asymmetric public-key algorithms (RS*/ES*/EdDSA).
    #[must_use]
    pub const fn is_asymmetric(self) -> bool {
        matches!(
            self,
            Self::Rs256
                | Self::Rs384
                | Self::Rs512
                | Self::Es256
                | Self::Es384
                | Self::Es512
                | Self::EdDsa
        )
    }

    /// Returns `true` for symmetric shared-secret algorithms (HS*).
    #[must_use]
    pub const fn is_symmetric(self) -> bool {
        matches!(self, Self::Hs256 | Self::Hs384 | Self::Hs512)
    }

    /// Returns `true` for the unauthenticated `none` algorithm.
    #[must_use]
    pub const fn is_none(self) -> bool {
        matches!(self, Self::None)
    }
}

impl FromStr for JwtAlgorithm {
    type Err = ();

    /// Parses a JWT `alg` string.
    ///
    /// JWT algorithm identifiers are case-sensitive per RFC 7518, so `"rs256"`
    /// is rejected. Unknown identifiers are also rejected.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "RS256" => Ok(Self::Rs256),
            "RS384" => Ok(Self::Rs384),
            "RS512" => Ok(Self::Rs512),
            "ES256" => Ok(Self::Es256),
            "ES384" => Ok(Self::Es384),
            "ES512" => Ok(Self::Es512),
            "HS256" => Ok(Self::Hs256),
            "HS384" => Ok(Self::Hs384),
            "HS512" => Ok(Self::Hs512),
            "EdDSA" => Ok(Self::EdDsa),
            "none" => Ok(Self::None),
            _ => Err(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::JwtAlgorithm;

    #[test]
    fn parses_all_canonical_algorithms() {
        assert_eq!("RS256".parse(), Ok(JwtAlgorithm::Rs256));
        assert_eq!("RS384".parse(), Ok(JwtAlgorithm::Rs384));
        assert_eq!("RS512".parse(), Ok(JwtAlgorithm::Rs512));
        assert_eq!("ES256".parse(), Ok(JwtAlgorithm::Es256));
        assert_eq!("ES384".parse(), Ok(JwtAlgorithm::Es384));
        assert_eq!("ES512".parse(), Ok(JwtAlgorithm::Es512));
        assert_eq!("HS256".parse(), Ok(JwtAlgorithm::Hs256));
        assert_eq!("HS384".parse(), Ok(JwtAlgorithm::Hs384));
        assert_eq!("HS512".parse(), Ok(JwtAlgorithm::Hs512));
        assert_eq!("EdDSA".parse(), Ok(JwtAlgorithm::EdDsa));
        assert_eq!("none".parse(), Ok(JwtAlgorithm::None));
    }

    #[test]
    fn as_str_round_trips_canonical_strings() {
        for value in [
            "RS256",
            "RS384",
            "RS512",
            "ES256",
            "ES384",
            "ES512",
            "HS256",
            "HS384",
            "HS512",
            "EdDSA",
            "none",
        ] {
            let parsed: JwtAlgorithm = value.parse().expect("valid algorithm");
            assert_eq!(parsed.as_str(), value);
        }
    }

    #[test]
    fn parsing_is_case_sensitive() {
        for value in ["rs256", "rs512", "es256", "hs256", "eddsa", "NONE", "None", "EdDsa"] {
            assert!(
                value.parse::<JwtAlgorithm>().is_err(),
                "expected {value:?} to be rejected as case-sensitive"
            );
        }
    }

    #[test]
    fn rejects_unknown_algorithms() {
        for value in ["MACSHA256", "PS256", "", "RS256 ", "RS256/"] {
            assert!(
                value.parse::<JwtAlgorithm>().is_err(),
                "expected {value:?} to be rejected"
            );
        }
    }

    #[test]
    fn classifies_asymmetric_algorithms() {
        for value in ["RS256", "RS384", "RS512", "ES256", "ES384", "ES512", "EdDSA"] {
            let parsed: JwtAlgorithm = value.parse().expect("valid algorithm");
            assert!(parsed.is_asymmetric(), "{value:?} should be asymmetric");
            assert!(!parsed.is_symmetric(), "{value:?} should not be symmetric");
            assert!(!parsed.is_none(), "{value:?} should not be none");
        }
    }

    #[test]
    fn classifies_symmetric_algorithms() {
        for value in ["HS256", "HS384", "HS512"] {
            let parsed: JwtAlgorithm = value.parse().expect("valid algorithm");
            assert!(parsed.is_symmetric(), "{value:?} should be symmetric");
            assert!(!parsed.is_asymmetric(), "{value:?} should not be asymmetric");
            assert!(!parsed.is_none(), "{value:?} should not be none");
        }
    }

    #[test]
    fn classifies_none_algorithm() {
        let parsed: JwtAlgorithm = "none".parse().expect("valid algorithm");
        assert!(parsed.is_none(), "none should be none");
        assert!(!parsed.is_asymmetric(), "none should not be asymmetric");
        assert!(!parsed.is_symmetric(), "none should not be symmetric");
    }
}
