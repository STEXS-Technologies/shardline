//! Signed external identities must retain their assigned resource and permissions.
use super::*;
use crate::jwt_keys::test_support::{TEST_RSA_PEM, test_rsa_jwk};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde_json::{Value, json};
use shardline_protocol::TokenScope;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::path};

fn signed(payload: &Value) -> String {
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some("resource-key".into());
    encode(
        &header,
        payload,
        &EncodingKey::from_rsa_pem(TEST_RSA_PEM.as_bytes()).unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn external_jwt_resource_mapping_preserves_scope_through_internal_token_issuance() {
    let server = MockServer::start().await;
    let issuer = server.uri();
    let key = test_rsa_jwk("resource-key");
    Mock::given(path("/jwks"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"keys":[{"kid":key.kid,"kty":"RSA","n":key.n,"e":key.e}]})),
        )
        .mount(&server)
        .await;
    Mock::given(path("/.well-known/openid-configuration"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"issuer":issuer,"jwks_uri":format!("{issuer}/jwks")})),
        )
        .mount(&server)
        .await;
    let root = TempDir::new().unwrap();
    let base = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://localhost".into(),
        root.path().to_owned(),
        NonZeroUsize::new(65_536).unwrap(),
    )
    .with_token_signing_key(vec![0x41; 32])
    .unwrap()
    .with_auth_oidc_issuer(issuer.clone())
    .with_auth_oidc_audience("storage".into())
    .with_auth_jwks_url(format!("{issuer}/jwks"))
    .with_auth_jwks_issuer(issuer.clone())
    .with_auth_jwks_audience("storage".into())
    .with_auth_jwt_repository_claim("resource".into());
    let payload = json!({"iss":issuer,"aud":"storage","exp":9999999999_u64,"sub":"identity|opaque-subject",
        "scope":"write","permissions":"read","resource":{"provider":"generic","owner":"tenant-a","name":"models","revision":"main"}});
    for provider in [AuthProviderKind::Oidc, AuthProviderKind::Jwks] {
        let config = base
            .clone()
            .with_auth_provider(provider)
            .with_auth_jwt_scope_claim("permissions".into());
        let auth = crate::app::build_auth_provider(&config)
            .await
            .unwrap()
            .unwrap();
        let verified = auth.provider().verify_token(&signed(&payload)).unwrap();
        assert_eq!(verified.subject(), "identity|opaque-subject");
        assert_eq!(verified.repository().owner(), "tenant-a");
        assert_eq!(verified.repository().name(), "models");
        assert_eq!(verified.repository().revision(), Some("main"));
        assert_eq!(verified.scope(), TokenScope::Read);
        let internal = auth.provider().mint_token(&verified).unwrap();
        assert_eq!(auth.provider().verify_token(&internal).unwrap(), verified);
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            format!("Bearer {internal}").parse().unwrap(),
        );
        assert!(auth.authorize(&headers, TokenScope::Read).is_ok());
        assert!(auth.authorize(&headers, TokenScope::Write).is_err());
        let mut writable = payload.clone();
        writable["permissions"] = json!("write");
        writable["resource"]["owner"] = json!("tenant-b");
        let other = auth.provider().verify_token(&signed(&writable)).unwrap();
        assert_eq!(other.scope(), TokenScope::Write);
        assert_eq!(other.repository().owner(), "tenant-b");
        assert_ne!(other.repository(), verified.repository());
        writable["permissions"] = json!("admin");
        writable["aud"] = json!(["another-service", "storage"]);
        assert_eq!(
            auth.provider()
                .verify_token(&signed(&writable))
                .unwrap()
                .scope(),
            TokenScope::Write
        );
        for (claim, replacement) in [
            ("aud", None),
            ("aud", Some(json!("another-service"))),
            ("iss", Some(json!("another-issuer"))),
            ("resource", None),
            ("resource", Some(json!("tenant-a/models"))),
            (
                "resource",
                Some(json!({"provider":"generic","owner":" ","name":"models"})),
            ),
            ("permissions", None),
            ("permissions", Some(json!("unknown"))),
            ("permissions", Some(json!(["write"]))),
        ] {
            let mut invalid = payload.clone();
            if let Some(value) = replacement {
                invalid[claim] = value;
            } else {
                invalid.as_object_mut().unwrap().remove(claim);
            }
            assert!(
                auth.provider().verify_token(&signed(&invalid)).is_err(),
                "{provider:?} accepted invalid {claim}: {invalid}"
            );
        }
        // Unset permission mapping uses the standard scope claim, while exact
        // namespaced repository keys remain independent of the identity subject.
        let repository_claim = "https://claims.example/repository";
        let standard = base
            .clone()
            .with_auth_provider(provider)
            .with_auth_jwt_repository_claim(repository_claim.into());
        let standard = crate::app::build_auth_provider(&standard)
            .await
            .unwrap()
            .unwrap();
        let mut standard_payload = payload.clone();
        standard_payload[repository_claim] = standard_payload
            .as_object_mut()
            .unwrap()
            .remove("resource")
            .unwrap();
        let standard_claims = standard
            .provider()
            .verify_token(&signed(&standard_payload))
            .unwrap();
        assert_eq!(standard_claims.scope(), TokenScope::Write);
        assert_eq!(standard_claims.repository(), verified.repository());
    }
}

#[tokio::test]
async fn external_jwt_startup_rejects_missing_audience_and_blank_claim_names_before_network() {
    let root = TempDir::new().unwrap();
    let base = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://localhost".into(),
        root.path().to_owned(),
        NonZeroUsize::new(65_536).unwrap(),
    )
    .with_auth_oidc_issuer("http://127.0.0.1:1".into())
    .with_auth_jwks_url("http://127.0.0.1:1/jwks".into());
    for provider in [AuthProviderKind::Oidc, AuthProviderKind::Jwks] {
        let config = base.clone().with_auth_provider(provider);
        for candidate in [
            config.clone(),
            config
                .clone()
                .with_auth_oidc_audience(" ".into())
                .with_auth_jwks_audience(" ".into()),
        ] {
            let error = crate::app::build_auth_provider(&candidate)
                .await
                .err()
                .unwrap();
            assert!(
                matches!(&error, ServerError::Config(ServerConfigError::ConfigFileError(message)) if message.contains("AUDIENCE")),
                "{error:?}"
            );
        }
        let config = config
            .with_auth_oidc_audience("storage".into())
            .with_auth_jwks_audience("storage".into());
        for candidate in [
            config.clone().with_auth_jwt_repository_claim(" ".into()),
            config.with_auth_jwt_scope_claim(" ".into()),
        ] {
            let error = crate::app::build_auth_provider(&candidate)
                .await
                .err()
                .unwrap();
            assert!(
                matches!(&error, ServerError::Config(ServerConfigError::ConfigFileError(message)) if message.contains("claim names must not be empty")),
                "{error:?}"
            );
        }
    }
}
