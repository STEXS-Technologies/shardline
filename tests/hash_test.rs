use std::io::Cursor;
use shardline_xet_adapter::{store_uploaded_xorb_bytes, validate_serialized_xorb};
use shardline_xet_core::{
    merklehash::{compute_data_hash, xorb_hash},
    xorb_object::{
        CompressionScheme, SerializedXorbObject,
        xorb_format_test_utils::serialized_xorb_object_from_components,
    },
};
use shardline_protocol::ShardlineHash;
use shardline_server_core::ServerObjectStore;
use tempfile::TempDir;

fn main() {
    let num_chunks = 1;
    let chunk_size = 256;
    let mut all_data = Vec::with_capacity(num_chunks * chunk_size);
    let mut boundaries = Vec::with_capacity(num_chunks);
    let mut chunk_specs = Vec::with_capacity(num_chunks);

    for i in 0..num_chunks {
        let chunk: Vec<u8> = (0..chunk_size).map(|j| ((i + j) & 0xFF) as u8).collect();
        let chunk_hash = compute_data_hash(&chunk);
        all_data.extend_from_slice(&chunk);
        boundaries.push((chunk_hash, u32::try_from((i + 1) * chunk_size).unwrap()));
        chunk_specs.push((chunk_hash, u64::try_from(chunk_size).unwrap()));
    }

    let xorb_hash = xorb_hash(&chunk_specs);
    println!("xorb_hash hex: {:?}", xorb_hash.hex());
    println!("xorb_hash as_bytes len: {}", xorb_hash.as_bytes().len());
    println!("xorb_hash as_bytes: {:02x?}", xorb_hash.as_bytes());
    
    let SerializedXorbObject { serialized_data, hash, .. } =
        serialized_xorb_object_from_components(&xorb_hash, all_data, boundaries, CompressionScheme::None)
            .expect("fixture");
    
    println!("serialized hash hex: {:?}", hash.hex());
    println!("hashes match: {}", xorb_hash == hash);
    
    let expected_hash = ShardlineHash::from_bytes(xorb_hash.as_bytes().try_into().unwrap());
    println!("expected_hash hex: {:?}", expected_hash.hex_string());
    
    // Try to validate directly
    let mut reader = Cursor::new(serialized_data.as_slice());
    match validate_serialized_xorb(&mut reader, expected_hash) {
        Ok(v) => println!("validate OK: hash={:?}, chunks={}", v.hash().hex_string(), v.chunks().len()),
        Err(e) => println!("validate FAILED: {:?}", e),
    }
    
    // Try to store
    let temp = TempDir::new().unwrap();
    let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
    let hash_hex = expected_hash.hex_string();
    println!("storing with hash hex: {:?}", hash_hex);
    match store_uploaded_xorb_bytes(&object_store, &hash_hex, &serialized_data) {
        Ok(r) => println!("store OK: was_inserted={}", r.was_inserted),
        Err(e) => println!("store FAILED: {:?}", e),
    }
}
