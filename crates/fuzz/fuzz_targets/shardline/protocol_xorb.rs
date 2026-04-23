#![no_main]

use std::io::Cursor;

use libfuzzer_sys::fuzz_target;
use shardline_protocol::{ShardlineHash, decode_serialized_xorb_chunks, validate_serialized_xorb};
use shardline_server::fuzz_normalize_and_validate_xorb;

fuzz_target!(|data: (Vec<u8>, [u8; 32])| {
    let (serialized, expected_hash_bytes) = data;
    let expected_hash = ShardlineHash::from_bytes(expected_hash_bytes);

    let mut first_reader = Cursor::new(serialized.as_slice());
    let first_validation = validate_serialized_xorb(&mut first_reader, expected_hash);
    let mut second_reader = Cursor::new(serialized.as_slice());
    let second_validation = validate_serialized_xorb(&mut second_reader, expected_hash);
    assert_eq!(first_validation.is_ok(), second_validation.is_ok());
    match (&first_validation, &second_validation) {
        (Ok(left), Ok(right)) => assert_eq!(left, right),
        (Err(left), Err(right)) => assert_eq!(left.to_string(), right.to_string()),
        _ => return,
    }

    let Ok(validated) = first_validation else {
        return;
    };

    assert_eq!(validated.hash(), expected_hash);
    assert!(validated.packed_content_length() <= validated.total_length());

    let mut previous_packed_end = 0_u64;
    let mut previous_unpacked_end = 0_u64;

    for descriptor in validated.chunks() {
        assert_eq!(descriptor.packed_start(), previous_packed_end);
        assert!(descriptor.packed_end() > descriptor.packed_start());
        assert_eq!(descriptor.unpacked_start(), previous_unpacked_end);
        assert!(descriptor.unpacked_end() > descriptor.unpacked_start());
        previous_packed_end = descriptor.packed_end();
        previous_unpacked_end = descriptor.unpacked_end();
    }

    assert_eq!(validated.packed_content_length(), previous_packed_end);
    assert_eq!(validated.unpacked_length(), previous_unpacked_end);

    let mut first_decode_reader = Cursor::new(serialized.as_slice());
    let first_decode = decode_serialized_xorb_chunks(&mut first_decode_reader, &validated);
    let mut second_decode_reader = Cursor::new(serialized.as_slice());
    let second_decode = decode_serialized_xorb_chunks(&mut second_decode_reader, &validated);
    assert_eq!(first_decode.is_ok(), second_decode.is_ok());
    match (&first_decode, &second_decode) {
        (Ok(left), Ok(right)) => assert_eq!(left, right),
        (Err(left), Err(right)) => assert_eq!(left.to_string(), right.to_string()),
        _ => return,
    }

    let Ok(decoded) = first_decode else {
        return;
    };

    assert_eq!(decoded.len(), validated.chunks().len());

    let mut reconstructed_length = 0_u64;
    for decoded_chunk in decoded {
        let descriptor = decoded_chunk.descriptor();
        let chunk_length = u64::try_from(decoded_chunk.data().len());
        assert!(chunk_length.is_ok());
        let Ok(chunk_length) = chunk_length else {
            return;
        };
        let next_reconstructed_length = reconstructed_length.checked_add(chunk_length);
        assert!(next_reconstructed_length.is_some());
        let Some(next_reconstructed_length) = next_reconstructed_length else {
            return;
        };
        reconstructed_length = next_reconstructed_length;

        let chunk_hash = blake3::hash(decoded_chunk.data());
        let expected_chunk_hash = ShardlineHash::from_bytes(*chunk_hash.as_bytes());
        assert_eq!(descriptor.hash(), expected_chunk_hash);
    }

    assert_eq!(validated.unpacked_length(), reconstructed_length);

    let first_normalized = fuzz_normalize_and_validate_xorb(expected_hash, serialized.as_slice());
    let second_normalized = fuzz_normalize_and_validate_xorb(expected_hash, serialized.as_slice());
    assert_eq!(first_normalized.is_ok(), second_normalized.is_ok());
    match (&first_normalized, &second_normalized) {
        (Ok(left), Ok(right)) => assert_eq!(left, right),
        (Err(left), Err(right)) => assert_eq!(left.to_string(), right.to_string()),
        _ => return,
    }

    let Ok(normalized) = first_normalized else {
        return;
    };

    let input_len = u64::try_from(serialized.len());
    assert!(input_len.is_ok());
    let Ok(input_len) = input_len else {
        return;
    };
    assert!(normalized.normalized_len >= input_len);
    assert_eq!(normalized.total_len, normalized.normalized_len);
    assert_eq!(
        normalized.packed_content_len,
        validated.packed_content_length()
    );
    assert_eq!(normalized.unpacked_len, validated.unpacked_length());
    assert_eq!(normalized.chunk_count, validated.chunks().len());
});
