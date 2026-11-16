use super::*;
use std::io::{Read, Write};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn operations_preserve_provider_permission_failures() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let server = std::thread::spawn(move || {
        for (index, method) in [
            "HEAD", "PUT", "PUT", "GET", "POST", "GET", "POST", "HEAD", "GET",
        ]
        .into_iter()
        .enumerate()
        {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            let mut socket = loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(error)
                        if error.kind() == std::io::ErrorKind::WouldBlock
                            && std::time::Instant::now() < deadline =>
                    {
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                    Err(error) => return Err(error),
                }
            };
            socket
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            let mut headers = Vec::new();
            let mut byte = [0];
            while !headers.ends_with(b"\r\n\r\n") {
                assert!(headers.len() < 16 * 1024);
                socket.read_exact(&mut byte).unwrap();
                headers.push(byte[0]);
            }
            let headers = String::from_utf8(headers).unwrap();
            assert!(headers.starts_with(&format!("{method} ")), "{headers}");
            if index == 8 {
                assert!(
                    headers
                        .lines()
                        .next()
                        .unwrap()
                        .contains("start-after=ns%2Fkey01"),
                    "{headers}"
                );
            }
            let length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<u64>().unwrap())
                })
                .unwrap_or(0);
            std::io::copy(&mut (&mut socket).take(length), &mut std::io::sink()).unwrap();
            socket
                .write_all(
                    b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
        }
        Ok::<(), std::io::Error>(())
    });
    let store = synchronous_runtime_fixture(format!("http://{address}"));
    let key = ObjectKey::parse("test/key").unwrap();
    let destination = ObjectKey::parse("test/destination").unwrap();
    let integrity = ObjectIntegrity::new(chunk_hash(b"hello world"), 11);
    let results = [
        store.copy_object_if_absent(&key, &key).map(|_| ()),
        store
            .put_if_absent(&key, ObjectBody::from_slice(b"hello world"), &integrity)
            .map(|_| ()),
        store.put_overwrite(&key, ObjectBody::from_slice(b"hello world"), &integrity),
        super::super::existing_copy_outcome(&store, &key, &destination, 10).map(|_| ()),
        store.delete_location_if_present(&ObjectStorePath::from("test/key")),
        store
            .list_prefix(&ObjectPrefix::parse("").unwrap())
            .map(|_| ()),
        store.delete_if_present(&key).map(|_| ()),
        store.begin_content_addressed_upload(&key).await.map(|_| ()),
        store
            .list_flat_namespace_page(
                &ObjectPrefix::parse("ns/").unwrap(),
                Some(&ObjectKey::parse("ns/key01").unwrap()),
                10,
            )
            .map(|_| ()),
    ];
    server.join().unwrap().unwrap();
    for (index, result) in results.into_iter().enumerate() {
        let error = result.unwrap_err();
        if matches!(index, 5 | 8) {
            // The upstream listing client retains HTTP failures in ListRequest.
            assert!(matches!(
                &error,
                S3ObjectStoreError::External(object_store::Error::Generic { .. })
            ));
            assert!(
                std::error::Error::source(&error)
                    .unwrap()
                    .to_string()
                    .contains("403"),
                "{error:?}"
            );
        } else {
            assert!(
                matches!(
                    error,
                    S3ObjectStoreError::External(object_store::Error::PermissionDenied { .. })
                ),
                "{error}"
            );
        }
    }
}
