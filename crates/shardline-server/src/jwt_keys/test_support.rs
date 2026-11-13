//! Test keys and bounded HTTP responses used by both external auth providers.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::{Jwk, MAX_AUTH_JSON_BYTES};
use jsonwebtoken::Algorithm;
use reqwest::Client;
use std::time::Duration;

pub(crate) const TEST_RSA_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCfG3ZyPlQA0mey\n\
JmKe6fjSQyfZFRtg48r9kE5w5jK9jEtseNESnv1T5UbHpTWU5fLtBMad1gHBdoU+\n\
85ZbpukcvevIU3uqXyAoqb/kL1C5E1KwQYSt22qQrP9M4hHAAZvCkYcNxbivdDHB\n\
jYrd5M1SPY48nsQFzDiX7Epqw70RlbLKdJvoUy2xioiRM/au2CwyK5xmrCOdb1jX\n\
A2Kw/JA73DHkugZ1v+LouNGdhhx0hQIzO4OTKUBtXsEvGkfzvR4/Z/kQ+VMG+GSP\n\
Glgx3u/T6SPI48p2En6hT8vWxw7YrMaeCyPlHSZE7nRtulKgWA8ZbFw65JsRdNlI\n\
bCxUl+nvAgMBAAECggEAAMWfQx0mqX75YkloG+jQf8GWlH8Hl54p4o7bruFRGPAh\n\
9hAhIUz/t3N9M7u/zegJqLKIpRahxzCYxD1ZCPlea5zlGyw0HD73tAccj0KIVJQd\n\
FsWutbTTXcSxUIPmIPf5pQFVjC8FOV/8qqKJti1wMbD0qeTwiZAz0KfcZu41edYG\n\
X+rQsdcPdSYtL9YXBD+f/Ygjd4yEpjbVLe6ULr4sWzr6JayU4eHoNE56vf343jor\n\
xaUaOw3bifdkQzqztN+Xf2HDQesQrm0Y03dmCHMwYm56+sPjfxyDFtk1ohFuI3i/\n\
0HRHuHA5SOPSK3+VrCs7ENAN+Na0w/1f55ttX2DoyQKBgQDMFC+dLEIbqSAGZ4Di\n\
LYAL1JiqYL6kZIbZl9yWUAjNoMJpSBQ5W/NlUwOepsZH5rEDZTBEryTh3guCY25N\n\
H2XBOau34ifMhrtW8+qHhm+eZsms5z6E5NAaXmb3ThrQJgOPy/qHtoi7ADWG5rCa\n\
QcBc8//vTzHyJ1tNSvBHrnvKjQKBgQDHlj46DXRqPS+2sLZWGy3c1ps0KzjKFayF\n\
PKmbRvbr210g4A+Fy8/jiURCtPDY6hf+3th1p9pQ3tZ/gisNBycG+xnP8RpeTgFq\n\
38T5pSVHfhjwumJxySeuvekfrgcjEsOiekuXqRo/JPMS3LhkR9Fuxaotpte0BI4a\n\
N8hFENuFawKBgQC8LQjSdpLmioY7IYlYBPiC8B9tSxO+5erqDPubpmTXpppdFdeA\n\
JGdEUM2PptxCRFeId++QBaeOlX4rVp/IgWEEULckMWbdUoa/4N2q5a1adBEWW4vs\n\
Ykf5aH6tHtnegI7cMwvpw8hEFidFIsZJFsPXci3WbkHxtZScqrLwhdUjqQKBgH6b\n\
Uwv2XwPJnovQW0oR4az2Qev9AwBGcXLvgVOr15TUSaZCG/auzEg1WiTKrQGcte4K\n\
pNs1yCqGwSCPjQmtoNcv0Db1Zdmut/14x3XpidVpKx8BzNMLXG3fsJNVDNf13j4i\n\
P/OL5Mdrg/pSI3IRkMwo/YQKE0jxnscI3bTaNbbTAoGBAKMgiVyYl+wfFp3QV1zv\n\
AyLKOERs8eToNOVrylNpcw/dRahPBUPuHZ/rHzIbscVeuU14wYIq3Eje5qZU0NW6\n\
+uEiJRA0Evs1Q/93dyNO45iDCIdIhtHMA/LqnlDniz0aqPOBrFgx+4PDcfUZXvgL\n\
4FKti8JsZfXzaqRjz8KALNNV\n\
-----END PRIVATE KEY-----";

pub(crate) const TEST_RSA_N: &str = "nxt2cj5UANJnsiZinun40kMn2RUbYOPK_ZBOcOYyvYxLbHjREp79U-VGx6U1lOXy7QTGndYBwXaFPvOWW6bpHL3ryFN7ql8gKKm_5C9QuRNSsEGErdtqkKz_TOIRwAGbwpGHDcW4r3QxwY2K3eTNUj2OPJ7EBcw4l-xKasO9EZWyynSb6FMtsYqIkTP2rtgsMiucZqwjnW9Y1wNisPyQO9wx5LoGdb_i6LjRnYYcdIUCMzuDkylAbV7BLxpH870eP2f5EPlTBvhkjxpYMd7v0-kjyOPKdhJ-oU_L1scO2KzGngsj5R0mRO50bbpSoFgPGWxcOuSbEXTZSGwsVJfp7w";

pub(crate) const TEST_RSA_E: &str = "AQAB";

pub(crate) fn sample_rsa_jwk() -> Jwk {
    Jwk {
        key_use: None,
        key_operations: None,
        algorithm: None,
        curve: None,
        kid: "test".to_owned(),
        key_type: "RSA".to_owned(),
        n: Some(TEST_RSA_N.to_owned()),
        e: Some(TEST_RSA_E.to_owned()),
        x_coord: None,
        y_coord: None,
    }
}

pub(crate) fn sample_ec_jwk() -> Jwk {
    Jwk {
        key_use: None,
        key_operations: None,
        algorithm: None,
        curve: Some("P-256".to_owned()),
        kid: "test".to_owned(),
        key_type: "EC".to_owned(),
        n: None,
        e: None,
        x_coord: Some("MKBCTNIcKUSDii11ySs3526iDZ8AiTo7Tu6KPAqv7D4".to_owned()),
        y_coord: Some("4Etl6SRW2YiLUrN5vfvVHuhp7x8PxltmWWlbbM4IFyM".to_owned()),
    }
}

pub(crate) fn test_rsa_jwk(kid: &str) -> Jwk {
    Jwk {
        key_use: None,
        key_operations: None,
        algorithm: None,
        curve: None,
        kid: kid.to_owned(),
        key_type: "RSA".to_owned(),
        n: Some(TEST_RSA_N.to_owned()),
        e: Some(TEST_RSA_E.to_owned()),
        x_coord: None,
        y_coord: None,
    }
}

pub(crate) async fn auth_document_response(
    body: &[u8],
    chunked: bool,
    advertised: Option<usize>,
) -> (reqwest::Response, tokio::task::JoinHandle<()>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("owned fixture");
    let address = listener.local_addr().expect("fixture address");
    let body = body.to_vec();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("fixture request");
        let mut request = [0_u8; 8192];
        let mut filled = 0_usize;
        loop {
            let available = request.get_mut(filled..).expect("bounded request buffer");
            assert!(
                !available.is_empty(),
                "fixture request headers exceed buffer"
            );
            let read = socket.read(available).await.expect("request headers");
            assert!(read > 0, "fixture request ended before complete headers");
            filled = filled.checked_add(read).expect("bounded request count");
            if request
                .get(..filled)
                .expect("received header bytes")
                .windows(4)
                .any(|window| window == b"\r\n\r\n")
            {
                break;
            }
        }
        let framing = if chunked {
            "Transfer-Encoding: chunked\r\n".to_owned()
        } else {
            format!("Content-Length: {}\r\n", advertised.unwrap_or(body.len()))
        };
        let headers = format!("HTTP/1.1 200 OK\r\n{framing}Connection: close\r\n\r\n");
        if socket.write_all(headers.as_bytes()).await.is_err() {
            return;
        }
        if chunked {
            // Split the body so the limit must account for cumulative reads.
            for chunk in body.chunks(MAX_AUTH_JSON_BYTES / 2) {
                if socket
                    .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
                    .await
                    .is_err()
                    || socket.write_all(chunk).await.is_err()
                    || socket.write_all(b"\r\n").await.is_err()
                {
                    return;
                }
            }
            let _ = socket.write_all(b"0\r\n\r\n").await;
        } else {
            let _ = socket.write_all(&body).await;
        }
    });
    let response = Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("fixture client")
        .get(format!("http://{address}"))
        .send()
        .await
        .expect("fixture response");
    (response, server)
}

pub(crate) fn assert_rsa_jwk_metadata_policy<Verify: Fn(&str) -> bool>(
    make: impl Fn(Vec<Jwk>) -> Verify,
) {
    use jsonwebtoken::{EncodingKey, Header, encode};
    let claims = serde_json::json!({"iss":"https://example.com", "sub":"metadata-proof", "aud":"test-audience", "scope":"read", "exp":9999999999_u64});
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some("metadata".to_owned());
    let token = encode(
        &header,
        &claims,
        &EncodingKey::from_rsa_pem(TEST_RSA_PEM.as_bytes()).expect("local RSA fixture"),
    )
    .expect("signed JWT");
    let key = test_rsa_jwk("metadata");
    let base = serde_json::json!({"kid":"metadata", "kty":"RSA", "n":key.n, "e":key.e});

    for (metadata, allowed) in [
        (serde_json::json!({}), true),
        (
            serde_json::json!({"use":"sig", "key_ops":["verify"], "alg":"RS256"}),
            true,
        ),
        (serde_json::json!({"key_ops":["sign", "verify"]}), true),
        (serde_json::json!({"use":"enc"}), false),
        (serde_json::json!({"key_ops":["encrypt"]}), false),
        (serde_json::json!({"key_ops":["sign"]}), false),
        (serde_json::json!({"key_ops":[]}), false),
        (serde_json::json!({"key_ops":["verify", "verify"]}), false),
        (serde_json::json!({"key_ops":["verify", "encrypt"]}), false),
        (
            serde_json::json!({"use":"sig", "key_ops":["encrypt"]}),
            false,
        ),
        (serde_json::json!({"alg":"RS512"}), false),
        (serde_json::json!({"alg":"RSA-OAEP"}), false),
    ] {
        let mut value = base.clone();
        for (field, value_part) in metadata.as_object().expect("metadata object") {
            value[field] = value_part.clone();
        }
        let jwk: Jwk = serde_json::from_value(value).expect("JWK JSON");
        let provider = make(vec![jwk]);
        assert_eq!(provider(&token), allowed, "JWK metadata {metadata}");
        // A valid declared purpose must not weaken cryptographic verification.
        let mut corrupted = token.as_bytes().to_vec();
        let offset = corrupted.len().checked_sub(4).expect("signature byte");
        let byte = corrupted.get_mut(offset).expect("signature byte");
        *byte = if *byte == b'A' { b'B' } else { b'A' };
        assert!(!provider(
            std::str::from_utf8(&corrupted).expect("JWT ASCII")
        ));
    }
    let mut encryption = base.clone();
    encryption["use"] = serde_json::json!("enc");
    let mut wrong_algorithm = base.clone();
    wrong_algorithm["alg"] = serde_json::json!("RS512");
    let mut signing = base;
    signing["use"] = serde_json::json!("sig");
    signing["alg"] = serde_json::json!("RS256");
    let keys = [encryption, wrong_algorithm, signing]
        .into_iter()
        .map(|value| serde_json::from_value(value).expect("JWK JSON"))
        .collect();
    assert!(
        make(keys)(&token),
        "ineligible same-kid keys must not shadow signing key"
    );
}

pub(crate) fn assert_ec_jwk_curve_policy<Verify: Fn(&str) -> bool>(
    make: impl Fn(Vec<Jwk>) -> Verify,
) {
    use jsonwebtoken::{EncodingKey, Header, encode};
    let claims = serde_json::json!({"iss":"https://example.com", "sub":"metadata-proof", "aud":"test-audience", "scope":"read", "exp":9999999999_u64});

    // Synthetic signing fixtures for both supported EC algorithms.
    for (algorithm, expected_curve, pem, x, y) in [
        (
            Algorithm::ES256,
            "P-256",
            "-----BEGIN PRIVATE KEY-----\nMIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgH1qSATO7+He02xGt\n7sd8J1ZU9vtd8z/QR0zrX5P2jdmhRANCAARBR606IUs63njTAEXDffW3BQZ0rG+N\nPo7ztoRDu5dA6RSblr8zAxMdBoTg+Ly8ZDuDRgGMKawWURMNUjLl2nke\n-----END PRIVATE KEY-----\n",
            "QUetOiFLOt540wBFw331twUGdKxvjT6O87aEQ7uXQOk",
            "FJuWvzMDEx0GhOD4vLxkO4NGAYwprBZREw1SMuXaeR4",
        ),
        (
            Algorithm::ES384,
            "P-384",
            "-----BEGIN PRIVATE KEY-----\nMIG2AgEAMBAGByqGSM49AgEGBSuBBAAiBIGeMIGbAgEBBDANM2Y5m/XXz77CWk/E\nYfKJgizMx4kzDus+kCstElM0GqmTplqihO/HY6pLoigZPCChZANiAATp4XHxidU6\noyNqpqdwq1yuhFeU8DyZ8m9iyrq6T7rEPd3UvdNXQANAjQz4rg74c5BuOMl6wKd4\nYLB9EG+4gd668oYQXY/EEvtiQ4aimM9H5GZJ7FLC2+dPQeyZr/1wpe4=\n-----END PRIVATE KEY-----\n",
            "6eFx8YnVOqMjaqancKtcroRXlPA8mfJvYsq6uk-6xD3d1L3TV0ADQI0M-K4O-HOQ",
            "bjjJesCneGCwfRBvuIHeuvKGEF2PxBL7YkOGopjPR-RmSexSwtvnT0Hsma_9cKXu",
        ),
    ] {
        let mut header = Header::new(algorithm);
        header.kid = Some("curve".to_owned());
        let token = encode(
            &header,
            &claims,
            &EncodingKey::from_ec_pem(pem.as_bytes()).expect("local EC fixture"),
        )
        .expect("signed EC JWT");
        for curve in [
            None,
            Some("P-256"),
            Some("P-384"),
            Some("P-521"),
            Some("invalid"),
        ] {
            let mut value = serde_json::json!({"kid":"curve", "kty":"EC", "alg":algorithm, "use":"sig", "key_ops":["verify"], "x":x, "y":y});
            if let Some(curve) = curve {
                value["crv"] = serde_json::json!(curve);
            }
            let jwk: Jwk = serde_json::from_value(value).expect("EC JWK JSON");
            let provider = make(vec![jwk]);
            assert_eq!(
                provider(&token),
                curve == Some(expected_curve),
                "{algorithm:?} declared curve {curve:?}"
            );
            let mut corrupted = token.as_bytes().to_vec();
            let offset = corrupted.len().checked_sub(4).expect("signature byte");
            let byte = corrupted.get_mut(offset).expect("signature byte");
            *byte = if *byte == b'A' { b'B' } else { b'A' };
            assert!(!provider(
                std::str::from_utf8(&corrupted).expect("JWT ASCII")
            ));
        }
    }
}
