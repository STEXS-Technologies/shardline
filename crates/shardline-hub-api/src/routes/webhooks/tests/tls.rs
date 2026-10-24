//! Actual delivery transport, with a pinned loopback-only fixture connector.
//! Public-address preflight still runs. Connector SSRF enforcement is qualified
//! separately; this custom test connector deliberately maps example.com locally.
use super::super::*;

#[tokio::test]
async fn webhook_https_validates_ca_hostname_and_preserves_signed_payload() {
    let receiver = shardline_test_support::HttpsWebhookFixture::start().unwrap();
    let unrelated = shardline_test_support::HttpsWebhookFixture::start().unwrap();
    let body = br#"{"event":"repo.content","repository":"alice/model"}"#;
    for (trust, host, valid) in [
        (&unrelated, "example.com", false),
        (&receiver, "example.net", false),
        (&receiver, "example.com", true),
    ] {
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(10))
            .resolve(host, receiver.address())
            .tls_built_in_root_certs(false)
            .add_root_certificate(
                reqwest::Certificate::from_pem(&trust.ca_certificate().unwrap()).unwrap(),
            )
            .build()
            .unwrap();
        let url = format!("https://{host}:{}/webhook", receiver.address().port());
        let result = deliver_one_webhook(&client, &url, body, Some("fixture-secret")).await;
        if !valid {
            let error =
                result.expect_err("invalid CA or hostname must fail before sending payload");
            let mut certificate_error = false;
            let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(error.as_ref());
            while let Some(current) = cause {
                let diagnostic = current.to_string().to_lowercase();
                certificate_error |=
                    diagnostic.contains("certificate") || diagnostic.contains("unknownissuer");
                cause = current.source();
            }
            assert!(certificate_error, "failure must be certificate validation");
            assert!(
                receiver.received().is_err(),
                "untrusted receiver gets no body"
            );
        } else {
            result.unwrap();
            let received: serde_json::Value =
                serde_json::from_str(&receiver.received().unwrap()).unwrap();
            assert_eq!(received["body"], std::str::from_utf8(body).unwrap());
            assert_eq!(received["content_type"], "application/json");
            use hmac::{Hmac, Mac};
            let mut mac = Hmac::<sha2::Sha256>::new_from_slice(b"fixture-secret").unwrap();
            mac.update(body);
            assert_eq!(
                received["signature"],
                format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
            );
        }
    }
}
