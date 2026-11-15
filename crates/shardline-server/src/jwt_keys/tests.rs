//! Shared key-format, algorithm and bounded-document contracts.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
use super::test_support::*;
use super::*;
use serde_json::json;

#[tokio::test]
async fn auth_json_rejects_oversized_content_length_before_reading() {
    let (response, server) =
        auth_document_response(b"{}", false, Some(MAX_AUTH_JSON_BYTES + 1)).await;
    let error = read_auth_json_bounded::<serde_json::Value>(response)
        .await
        .expect_err("advertised overflow");
    server.await.expect("fixture cleanup");
    assert!(
        error.contains("document limit"),
        "must reject size before short-body error: {error}"
    );
}

#[tokio::test]
async fn auth_json_enforces_actual_chunked_bytes_and_accepts_exact_limit() {
    for extra in [0, 1] {
        let mut body = b"{}".to_vec();
        body.resize(MAX_AUTH_JSON_BYTES + extra, b' ');
        let (response, server) = auth_document_response(&body, true, None).await;
        assert!(
            response.content_length().is_none(),
            "must exercise streamed bound"
        );
        let result = read_auth_json_bounded::<serde_json::Value>(response).await;
        server.await.expect("fixture cleanup");
        if extra == 0 {
            assert_eq!(result.expect("exact limit is legal"), serde_json::json!({}));
        } else {
            assert!(
                result
                    .expect_err("streamed overflow")
                    .contains("document limit")
            );
        }
    }
}

#[test]
fn jwk_deserialize_rsa() {
    let json = json!({
        "kid": "rsa-key-1",
        "kty": "RSA",
        "alg": "RS256",
        "use": "sig",
        "n": "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4Qy5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw",
        "e": "AQAB"
    });
    let jwk: Jwk = serde_json::from_value(json).expect("should deserialize RSA JWK");
    assert_eq!(jwk.kid, "rsa-key-1");
    assert_eq!(jwk.key_type, "RSA");
    assert!(jwk.n.is_some());
    assert!(jwk.e.is_some());
}

#[test]
fn jwk_deserialize_ec() {
    let json = json!({
        "kid": "ec-key-1",
        "kty": "EC",
        "alg": "ES256",
        "use": "sig",
        "crv": "P-256",
        "x": "MKBCTNIcKUSDii11ySs3526iDZ8AiTo7Tu6KPAqv7D4",
        "y": "4Etl6SRW2YiLUrN5vfvVHuhp7x8PxltmWWlbbM4IFyM"
    });
    let jwk: Jwk = serde_json::from_value(json).expect("should deserialize EC JWK");
    assert_eq!(jwk.kid, "ec-key-1");
    assert_eq!(jwk.key_type, "EC");
    assert_eq!(
        jwk.x_coord.as_deref(),
        Some("MKBCTNIcKUSDii11ySs3526iDZ8AiTo7Tu6KPAqv7D4")
    );
    assert_eq!(
        jwk.y_coord.as_deref(),
        Some("4Etl6SRW2YiLUrN5vfvVHuhp7x8PxltmWWlbbM4IFyM")
    );
}

#[test]
fn base64_decode_url_valid() {
    // "test" in base64url (no padding)
    let result = base64_decode_url("dGVzdA").unwrap();
    assert_eq!(result, b"test");
}

#[test]
fn base64_decode_url_invalid_chars() {
    assert!(base64_decode_url("!!!not-valid!!!").is_err());
}

#[test]
fn base64_decode_url_empty_string() {
    let result = base64_decode_url("").unwrap();
    assert!(result.is_empty());
}

#[test]
fn build_decoding_key_reports_missing_parameters() {
    let mut missing_n = sample_rsa_jwk();
    missing_n.n = None;
    let mut missing_e = sample_rsa_jwk();
    missing_e.e = None;
    let mut missing_x = sample_ec_jwk();
    missing_x.x_coord = None;
    let mut missing_y = sample_ec_jwk();
    missing_y.y_coord = None;

    for (jwk, algorithm, expected) in [
        (missing_n, Algorithm::RS256, "missing n"),
        (missing_e, Algorithm::RS256, "missing e"),
        (missing_x, Algorithm::ES256, "missing x"),
        (missing_y, Algorithm::ES256, "missing y"),
    ] {
        let error = build_decoding_key(&jwk, algorithm)
            .err()
            .expect("missing key parameter must be rejected");
        assert!(error.contains(expected), "expected {expected}, got {error}");
    }
}

#[test]
fn jwk_deserialize_missing_optional_fields() {
    // Only kid and kty are required; n, e, x, y are optional
    let json = json!({
        "kid": "minimal",
        "kty": "RSA"
    });
    let jwk: Jwk = serde_json::from_value(json).expect("should deserialize minimal JWK");
    assert_eq!(jwk.kid, "minimal");
    assert_eq!(jwk.key_type, "RSA");
    assert!(jwk.n.is_none());
    assert!(jwk.e.is_none());
}

#[test]
fn jwk_deserialize_empty_kid() {
    let json = json!({
        "kid": "",
        "kty": "RSA",
        "n": "n",
        "e": "e"
    });
    let jwk: Jwk = serde_json::from_value(json).expect("should deserialize JWK with empty kid");
    assert!(jwk.kid.is_empty());
}

#[test]
fn jwk_deserialize_additional_fields_ignored() {
    let json = json!({
        "kid": "key1",
        "kty": "EC",
        "alg": "ES256",
        "use": "sig",
        "crv": "P-256",
        "x": "xval",
        "y": "yval",
        "ext": true,
        "key_ops": ["verify"]
    });
    let jwk: Jwk = serde_json::from_value(json).expect("should ignore extra fields");
    assert_eq!(jwk.kid, "key1");
    assert_eq!(jwk.key_type, "EC");
}

#[test]
fn jwk_deserialize_invalid_type_missing_kid() {
    let json = json!({
        "kty": "RSA",
        "n": "n",
        "e": "e"
        // no kid
    });
    // kid is not optional in our struct (no Option wrapper)
    let result: Result<Jwk, _> = serde_json::from_value(json);
    assert!(result.is_err(), "missing required field kid should fail");
}

#[test]
fn jwks_response_preserves_key_order_and_ignores_extra_fields() {
    for (document, expected_ids) in [
        (json!({ "keys": [] }), vec![]),
        (json!({ "keys": [], "extra": "field" }), vec![]),
        (
            json!({ "keys": [
                { "kid": "k1", "kty": "RSA", "n": "n1", "e": "e1" },
                { "kid": "k2", "kty": "EC", "crv": "P-256", "x": "x2", "y": "y2" },
                { "kid": "k3", "kty": "RSA", "n": "n3", "e": "e3" }
            ] }),
            vec!["k1", "k2", "k3"],
        ),
        (
            json!({ "keys": [
                { "kid": "k1", "kty": "RSA", "alg": "RS256", "n": "m", "e": "e" },
                { "kid": "k2", "kty": "EC", "alg": "ES256", "crv": "P-256", "x": "x", "y": "y" }
            ] }),
            vec!["k1", "k2"],
        ),
    ] {
        let response: JwksResponse = serde_json::from_value(document).expect("valid JWKS document");
        let ids: Vec<_> = response.keys.iter().map(|key| key.kid.as_str()).collect();
        assert_eq!(ids, expected_ids);
    }
}

#[test]
fn jwks_response_deserialize_missing_keys_field() {
    let result = serde_json::from_value::<JwksResponse>(json!({}));
    assert!(result.is_err(), "missing 'keys' field should fail");
}

#[test]
fn build_decoding_key_rejects_missing_parameter_pairs() {
    for (jwk, algorithm) in [
        (
            Jwk {
                kid: "missing-params".to_owned(),
                n: None,
                e: None,
                ..sample_rsa_jwk()
            },
            Algorithm::RS256,
        ),
        (
            Jwk {
                kid: "missing-ec-params".to_owned(),
                x_coord: None,
                y_coord: None,
                ..sample_ec_jwk()
            },
            Algorithm::ES256,
        ),
    ] {
        assert!(
            build_decoding_key(&jwk, algorithm).is_err(),
            "{algorithm:?}"
        );
    }
}

#[test]
fn build_decoding_key_rejects_ec_algorithm_with_rsa_parameters() {
    let jwk = Jwk {
        kid: "unsupported".to_owned(),
        ..sample_rsa_jwk()
    };
    assert!(build_decoding_key(&jwk, Algorithm::ES256).is_err());
}

#[test]
fn algorithm_key_type_matrix() {
    for (key_type, algorithm, expected) in [
        ("RSA", Algorithm::RS256, true),
        ("RSA", Algorithm::RS384, true),
        ("RSA", Algorithm::RS512, true),
        ("RSA", Algorithm::PS256, true),
        ("RSA", Algorithm::PS384, true),
        ("RSA", Algorithm::PS512, true),
        ("RSA", Algorithm::ES256, false),
        ("EC", Algorithm::ES256, true),
        ("EC", Algorithm::ES384, true),
        ("EC", Algorithm::RS256, false),
        ("RSA", Algorithm::HS256, false),
        ("RSA", Algorithm::EdDSA, false),
        ("OCT", Algorithm::HS256, false),
        ("oct", Algorithm::RS256, false),
    ] {
        assert_eq!(
            is_algorithm_compatible(key_type, algorithm),
            expected,
            "{key_type} / {algorithm:?}"
        );
    }
}

#[test]
fn decoding_keys_accept_supported_rsa_and_ec_algorithms() {
    for (key, algorithms) in [
        (
            sample_rsa_jwk(),
            &[
                Algorithm::RS256,
                Algorithm::RS384,
                Algorithm::RS512,
                Algorithm::PS256,
                Algorithm::PS384,
                Algorithm::PS512,
            ][..],
        ),
        (sample_ec_jwk(), &[Algorithm::ES256, Algorithm::ES384][..]),
    ] {
        for &algorithm in algorithms {
            let result = build_decoding_key(&key, algorithm);
            assert!(result.is_ok(), "{algorithm:?}: {:?}", result.err());
        }
    }
}

#[test]
fn decoding_keys_reject_unsupported_algorithms() {
    for (key, algorithm) in [
        (sample_rsa_jwk(), Algorithm::HS256),
        (sample_rsa_jwk(), Algorithm::HS384),
        (sample_rsa_jwk(), Algorithm::HS512),
        (sample_ec_jwk(), Algorithm::EdDSA),
    ] {
        let error = build_decoding_key(&key, algorithm)
            .err()
            .expect("unsupported algorithm");
        assert!(
            error.contains("unsupported algorithm"),
            "{algorithm:?}: {error}"
        );
    }
}

#[test]
fn secure_url_policy() {
    for (url, allowed) in [
        ("", false),
        ("ftp://example.com/jwks", false),
        ("ftp://keys.example.com/jwks", false),
        ("http://127.0.0.1:8080/jwks", true),
        ("http://[::1]:8080/jwks", true),
        ("http://accounts.example.com", false),
        ("http://attacker.example.com/jwks", false),
        ("http://keys.example.com/jwks", false),
        ("http://localhost:8080/jwks", true),
        ("https://accounts.example.com", true),
        ("https://accounts.example.com/.well-known/jwks", true),
        ("https://keys.example.com/jwks", true),
        ("https://keys.example.com:8443/jwks", true),
        ("not-a-url", false),
    ] {
        assert_eq!(is_secure_url(url), allowed, "URL {url:?}");
    }
}

#[test]
fn jwt_payload_rejects_unsigned_and_symmetric_algorithms_before_key_lookup() {
    use base64::Engine as _;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;

    for algorithm in ["none", "HS256", "HS384", "HS512"] {
        let header = URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&json!({"alg": algorithm, "kid": "test"})).unwrap());
        let result = verify_jwt_payload(&header, "payload", "signature", &[], "issuer", None);
        if algorithm == "none" {
            assert!(matches!(result, Err(AuthError::InvalidToken)));
        } else {
            assert!(matches!(
                result,
                Err(AuthError::ProviderError(message))
                    if message == format!("unsupported or insecure algorithm: {algorithm}")
            ));
        }
    }
}

#[test]
fn jwt_partitioning_requires_exactly_three_parts_without_decoding_them() {
    for (token, expected) in [
        (
            "header.payload.signature",
            ("header", "payload", "signature"),
        ),
        ("..", ("", "", "")),
        (".payload.signature", ("", "payload", "signature")),
        ("header..signature", ("header", "", "signature")),
        ("header.payload.", ("header", "payload", "")),
    ] {
        assert_eq!(super::split_jwt(token).unwrap(), expected);
    }
    for token in [
        "",
        "header",
        "header.payload",
        "header.payload.signature.extra",
        "...",
    ] {
        assert!(matches!(
            super::split_jwt(token),
            Err(shardline_server_core::AuthError::InvalidToken)
        ));
    }
}
