#![allow(clippy::unwrap_used)]
use super::*;

#[test]
fn standard_check_values_for_all_supported_checksums() {
    for (algorithm, expected) in [
        (Algorithm::Crc32, "cbf43926"),
        (Algorithm::Crc32c, "e3069283"),
        (Algorithm::Crc64nvme, "ae8b14860a799888"),
        (Algorithm::Sha1, "f7c3bc1d808e04732adf679965ccc34ca7ae3441"),
        (
            Algorithm::Sha256,
            "15e2b0d3c33891ebb0f1ef609ec419420c20e320ce94c65fbc8c3312448eb225",
        ),
    ] {
        let mut checksum = Checksum::new(algorithm);
        checksum.update(b"1234");
        checksum.update(b"56789");
        assert_eq!(
            hex::encode(STANDARD.decode(checksum.encoded()).unwrap()),
            expected
        );
    }
}

#[test]
fn declarations_reject_duplicates_unknown_names_and_excess_input() {
    for invalid in [
        "",
        "x-amz-meta-other",
        "x-amz-checksum-crc32c,x-amz-checksum-crc32c",
        "x-amz-checksum-sha256,",
    ] {
        assert!(parse_names(invalid).is_err());
    }
    assert!(parse_names(&"x".repeat(257)).is_err());
    assert_eq!(
        parse_names("x-amz-checksum-sha256,x-amz-checksum-crc32c").unwrap(),
        vec![Algorithm::Crc32c, Algorithm::Sha256]
    );
}
