//! Test keys and bounded HTTP responses used by both external auth providers.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::{Jwk, MAX_AUTH_JSON_BYTES};
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
