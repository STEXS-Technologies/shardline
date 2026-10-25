//! Emit only fixed diagnostic labels, never substrings from credential-bearing output.

pub(super) fn categories(stdout: &[u8], stderr: &[u8]) -> String {
    let markers: &[(&str, &[&[u8]])] = &[
        ("local-disk-full", &[b"No space left on device", b"ENOSPC"]),
        ("object-store-full", &[b"XMinioStorageFull", b"StorageFull"]),
        ("unauthorized", &[b"401 Unauthorized", b"401 Client Error"]),
        ("forbidden", &[b"403 Forbidden", b"403 Client Error"]),
        ("not-found", &[b"404 Not Found", b"404 Client Error"]),
        ("timeout", &[b"TimeoutError", b"ReadTimeout", b"timed out"]),
        (
            "connection-reset",
            &[b"Connection reset", b"ConnectionResetError"],
        ),
        ("signature-rejected", &[b"SignatureDoesNotMatch"]),
        ("range-rejected", &[b"416 Range", b"InvalidRange"]),
        ("expired", &[b"ExpiredToken", b"Request has expired"]),
    ];
    let found: Vec<&str> = markers
        .iter()
        .filter(|(_label, needles)| {
            needles.iter().any(|needle| {
                [stdout, stderr]
                    .iter()
                    .any(|bytes| bytes.windows(needle.len()).any(|window| window == *needle))
            })
        })
        .map(|(label, _needles)| *label)
        .collect();
    if found.is_empty() {
        "unclassified".to_owned()
    } else {
        found.join(",")
    }
}

#[test]
fn diagnostics_never_echo_credentials_or_unrecognized_errors() {
    let output = b"https://private/?token=secret bearer=secret XMinioStorageFull";
    assert_eq!(categories(b"", output), "object-store-full");
    assert_eq!(
        categories(
            b"secret-token No space left on device",
            b"401 Unauthorized secret"
        ),
        "local-disk-full,unauthorized"
    );
    assert_eq!(
        categories(b"private unknown failure", b"secret"),
        "unclassified"
    );
}
