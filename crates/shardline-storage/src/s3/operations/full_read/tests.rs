use std::{
    io::{Read, Seek, SeekFrom},
    ops::Range,
};

use bytes::Bytes;
use object_store::{GetResult, GetResultPayload, ObjectMeta};

use super::copy_payload_to_file;

fn response(size: u64, range: Range<u64>, frames: Vec<object_store::Result<Bytes>>) -> GetResult {
    GetResult {
        payload: GetResultPayload::Stream(Box::pin(futures_util::stream::iter(frames))),
        meta: ObjectMeta {
            location: object_store::path::Path::from("container"),
            last_modified: chrono::Utc::now(),
            size,
            e_tag: None,
            version: None,
        },
        range,
        attributes: Default::default(),
        extensions: Default::default(),
    }
}

#[tokio::test]
async fn copies_complete_frames_and_empty_objects() {
    for bytes in [b"abcdef".as_slice(), b"".as_slice()] {
        let mut file = tempfile::tempfile().unwrap();
        let result = response(
            bytes.len() as u64,
            0..bytes.len() as u64,
            bytes
                .chunks(2)
                .map(|frame| Ok(Bytes::copy_from_slice(frame)))
                .collect(),
        );
        assert_eq!(
            copy_payload_to_file(result, &mut file).await.unwrap(),
            bytes.len() as u64
        );
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut actual = Vec::new();
        file.read_to_end(&mut actual).unwrap();
        assert_eq!(actual, bytes);
    }
}

#[tokio::test]
async fn rejects_partial_ranges_and_incorrect_body_lengths() {
    for (size, range, body) in [
        (6, 1..6, "bcdef"),
        (6, 0..5, "abcde"),
        (6, 0..6, "abcde"),
        (6, 0..6, "abcdefg"),
    ] {
        let mut file = tempfile::tempfile().unwrap();
        let result = response(size, range, vec![Ok(Bytes::from(body))]);
        assert!(copy_payload_to_file(result, &mut file).await.is_err());
    }
}

#[tokio::test]
async fn propagates_body_failures_after_partial_output() {
    let mut file = tempfile::tempfile().unwrap();
    let result = response(
        6,
        0..6,
        vec![
            Ok(Bytes::from_static(b"abc")),
            Err(object_store::Error::Generic {
                store: "fixture",
                source: std::io::Error::other("disconnected").into(),
            }),
        ],
    );
    assert!(copy_payload_to_file(result, &mut file).await.is_err());
}
