//! A live API must survive transfer-process loss without publishing partial bytes.
use super::*;

#[tokio::test]
async fn split_lfs_process_kill_preserves_api_and_retry() {
    exercise(&[]).await;
}

pub(super) async fn exercise(environment: &[(&str, String)]) {
    for (frontend, prefix) in [("lfs", "/v1/lfs/objects"), ("hub", "/lfs/objects")] {
        let api_root = tempfile::tempdir().unwrap();
        let remote_root = tempfile::tempdir().unwrap();
        let transfer_root = if environment.is_empty() {
            api_root.path()
        } else {
            remote_root.path()
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let public = format!("http://{address}");
        let mut api_environment = environment.to_vec();
        api_environment.extend([
            ("LFS_PROCESS_PUBLIC", public.clone()),
            ("LFS_PROCESS_ROLE", "api".to_owned()),
        ]);
        let (mut api, api_base) = launch(api_root.path(), frontend, &api_environment).await;
        let mut transfer_environment = api_environment.clone();
        transfer_environment.push(("LFS_PROCESS_ROLE", "transfer".to_owned()));
        let (mut transfer, transfer_base) =
            launch(transfer_root, frontend, &transfer_environment).await;
        let proxy = split_roles::start_proxy(listener, &api_base, &transfer_base);
        let credential = token("writer", "split-process", TokenScope::Write);
        let payload = bytes::Bytes::from(vec![83_u8; 2 * 1024 * 1024]);
        let oid = hex::encode(Sha256::digest(&payload));
        let path = format!("{prefix}/{oid}");
        let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
        socket.write_all(format!("PUT {path} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {credential}\r\nContent-Length: {}\r\n\r\n", payload.len()).as_bytes()).await.unwrap();
        socket
            .write_all(&payload[..payload.len() / 2])
            .await
            .unwrap();
        transfer.0.kill().unwrap();
        assert_eq!(transfer.0.wait().unwrap().signal(), Some(9));
        drop(socket);
        assert!(
            api.0.try_wait().unwrap().is_none(),
            "API died with transfer role"
        );
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap();
        let batch_path = if frontend == "hub" {
            "/split-process/locks.git/info/lfs/objects/batch"
        } else {
            "/v1/lfs/objects/batch"
        };
        // While transfer is absent, the original API still negotiates an upload.
        let batch = client.post(format!("{public}{batch_path}"))
            .bearer_auth(&credential)
            .json(&json!({"operation":"upload","transfers":["basic"],"objects":[{"oid":oid,"size":payload.len()}]}))
            .send().await.unwrap();
        assert_eq!(batch.status(), StatusCode::OK);
        let batch: Value = batch.json().await.unwrap();
        let action = batch["objects"][0]["actions"]["upload"]["href"]
            .as_str()
            .expect("upload action");
        assert!(action.starts_with(&format!("{public}{path}")));
        proxy.abort();
        let _stopped = proxy.await;
        let (mut transfer, transfer_base) =
            launch(transfer_root, frontend, &transfer_environment).await;
        let listener = tokio::net::TcpListener::bind(address).await.unwrap();
        let proxy = split_roles::start_proxy(listener, &api_base, &transfer_base);
        let url = format!("{public}{path}");
        assert_eq!(
            client
                .get(&url)
                .bearer_auth(&credential)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
        assert!(
            client
                .put(action)
                .bearer_auth(&credential)
                .body(payload.clone())
                .send()
                .await
                .unwrap()
                .status()
                .is_success()
        );
        // Kill again after acknowledgment and recover on the same durable backend.
        transfer.0.kill().unwrap();
        assert_eq!(transfer.0.wait().unwrap().signal(), Some(9));
        proxy.abort();
        let _stopped = proxy.await;
        let (_transfer, transfer_base) =
            launch(transfer_root, frontend, &transfer_environment).await;
        let listener = tokio::net::TcpListener::bind(address).await.unwrap();
        let proxy = split_roles::start_proxy(listener, &api_base, &transfer_base);
        let response = client
            .get(&url)
            .bearer_auth(&credential)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.bytes().await.unwrap().as_ref(), payload.as_ref());
        assert!(api.0.try_wait().unwrap().is_none());
        proxy.abort();
        let _stopped = proxy.await;
    }
}
