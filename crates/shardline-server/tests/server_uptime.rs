#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::num::NonZeroUsize;

use shardline_server::{DeploymentMode, ServerConfig, ServerFrontend};

const ADMIN_TOKEN: &str = "uptime-test-admin-secret";

async fn admin_uptime(client: &reqwest::Client, url: &str) -> i64 {
    let response = client
        .get(url)
        .bearer_auth(ADMIN_TOKEN)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    response
        .json::<serde_json::Value>()
        .await
        .unwrap()
        .get("server_uptime_seconds")
        .unwrap()
        .as_i64()
        .unwrap()
}

#[tokio::test]
async fn serving_entry_points_advance_admin_uptime_before_prometheus_scrape() {
    let client = reqwest::Client::new();
    for supplied_listener in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = reserved.local_addr().unwrap();
        let config = ServerConfig::new(
            address,
            format!("http://{address}"),
            root.path().into(),
            NonZeroUsize::new(65536).unwrap(),
        )
        .with_server_frontends([ServerFrontend::Xet])
        .unwrap()
        .with_deployment_mode(DeploymentMode::Insecure)
        .with_admin_read_token(ADMIN_TOKEN.as_bytes().to_vec())
        .unwrap();
        let server = if supplied_listener {
            reserved.set_nonblocking(true).unwrap();
            let listener = tokio::net::TcpListener::from_std(reserved).unwrap();
            tokio::spawn(shardline_server::serve_with_listener(config, listener))
        } else {
            drop(reserved);
            tokio::spawn(shardline_server::serve(config))
        };
        let admin_url = format!("http://{address}/api/v1/metrics");
        let mut ready = false;
        for _ in 0..100 {
            if let Ok(response) = client.get(&admin_url).bearer_auth(ADMIN_TOKEN).send().await
                && response.status().is_success()
            {
                ready = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(ready, "local server did not become ready");
        // No Prometheus request has occurred: admin must read the clock itself.
        let before = admin_uptime(&client, &admin_url).await;
        assert!((0..60).contains(&before));
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        let after = admin_uptime(&client, &admin_url).await;
        assert!(
            after > before,
            "entry supplied_listener={supplied_listener}"
        );
        let scrape = client
            .get(format!("http://{address}/metrics"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        let scraped = scrape
            .lines()
            .find_map(|line| line.strip_prefix("shardline_server_uptime_seconds "))
            .unwrap()
            .parse::<i64>()
            .unwrap();
        assert!((after..=after + 1).contains(&scraped));
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
    }
}
