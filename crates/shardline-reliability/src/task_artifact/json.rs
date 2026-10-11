use crate::ReliabilityError;
use serde::Serialize;
use std::io::{self, Write};

struct BoundedJson {
    bytes: Vec<u8>,
}
impl Write for BoundedJson {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(bytes.len())
            .is_none_or(|length| length > 65536)
        {
            return Err(io::Error::other("native task proof exceeds 64 KiB"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(super) fn encode(value: &impl Serialize) -> Result<Vec<u8>, ReliabilityError> {
    let mut writer = BoundedJson { bytes: Vec::new() };
    serde_json::to_writer(&mut writer, value)?;
    Ok(writer.bytes)
}
