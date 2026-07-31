use std::io::{Read, Write};
use std::mem::size_of;

use crate::merklehash::DataHash;

#[inline]
pub fn write_hash<W: Write>(writer: &mut W, m: &DataHash) -> Result<(), std::io::Error> {
    let bytes: [u8; 32] = (*m).into();
    writer.write_all(&bytes)
}

#[inline]
pub fn write_u8<W: Write>(writer: &mut W, v: u8) -> Result<(), std::io::Error> {
    writer.write_all(&v.to_le_bytes())
}

#[inline]
pub fn write_u32<W: Write>(writer: &mut W, v: u32) -> Result<(), std::io::Error> {
    writer.write_all(&v.to_le_bytes())
}

#[inline]
pub fn write_u64<W: Write>(writer: &mut W, v: u64) -> Result<(), std::io::Error> {
    writer.write_all(&v.to_le_bytes())
}

#[inline]
pub fn write_bytes<W: Write>(writer: &mut W, vs: &[u8]) -> Result<(), std::io::Error> {
    writer.write_all(vs)
}

#[inline]
pub fn write_u32s<W: Write>(writer: &mut W, vs: &[u32]) -> Result<(), std::io::Error> {
    for e in vs {
        write_u32(writer, *e)?;
    }
    Ok(())
}

#[inline]
pub fn write_u64s<W: Write>(writer: &mut W, vs: &[u64]) -> Result<(), std::io::Error> {
    for e in vs {
        write_u64(writer, *e)?;
    }
    Ok(())
}

#[inline]
pub fn read_hash<R: Read>(reader: &mut R) -> Result<DataHash, std::io::Error> {
    let mut m = [0u8; 32];
    reader.read_exact(&mut m)?;
    Ok(DataHash::from(m))
}

#[inline]
pub fn read_u8<R: Read>(reader: &mut R) -> Result<u8, std::io::Error> {
    let mut buf = [0u8; size_of::<u8>()];
    reader.read_exact(&mut buf[..])?;
    Ok(u8::from_le_bytes(buf))
}

#[inline]
pub fn read_u32<R: Read>(reader: &mut R) -> Result<u32, std::io::Error> {
    let mut buf = [0u8; size_of::<u32>()];
    reader.read_exact(&mut buf[..])?;
    Ok(u32::from_le_bytes(buf))
}

#[inline]
pub fn read_u64<R: Read>(reader: &mut R) -> Result<u64, std::io::Error> {
    let mut buf = [0u8; size_of::<u64>()];
    reader.read_exact(&mut buf[..])?;
    Ok(u64::from_le_bytes(buf))
}

#[inline]
pub fn read_bytes<R: Read>(reader: &mut R, val: &mut [u8]) -> Result<(), std::io::Error> {
    reader.read_exact(val)
}

#[inline]
pub fn read_u32s<R: Read>(reader: &mut R, vs: &mut [u32]) -> Result<(), std::io::Error> {
    for e in vs.iter_mut() {
        *e = read_u32(reader)?;
    }
    Ok(())
}

#[inline]
pub fn read_u64s<R: Read>(reader: &mut R, vs: &mut [u64]) -> Result<(), std::io::Error> {
    for e in vs.iter_mut() {
        *e = read_u64(reader)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::merklehash::DataHash;

    use super::*;

    #[test]
    fn write_and_read_hash_roundtrip() {
        let h = DataHash::from([
            0xDEADBEEF_CAFEBABEu64,
            0x12345678_9ABCDEF0,
            0xFEDCBA09_87654321,
            0x01020304_05060708,
        ]);
        let mut buf = Vec::new();
        write_hash(&mut buf, &h).unwrap();
        let mut r = Cursor::new(&buf);
        let h2 = read_hash(&mut r).unwrap();
        assert_eq!(h, h2);
    }

    #[test]
    fn write_and_read_u8_edge_cases() {
        for v in [0u8, 1, 127, 128, 255] {
            let mut buf = Vec::new();
            write_u8(&mut buf, v).unwrap();
            let mut r = Cursor::new(&buf);
            assert_eq!(read_u8(&mut r).unwrap(), v);
        }
    }

    #[test]
    fn write_and_read_u32_edge_cases() {
        for v in [0u32, 1, 0xFFFF, 0x1_0000, u32::MAX >> 1, u32::MAX] {
            let mut buf = Vec::new();
            write_u32(&mut buf, v).unwrap();
            let mut r = Cursor::new(&buf);
            assert_eq!(read_u32(&mut r).unwrap(), v);
        }
    }

    #[test]
    fn write_and_read_u64_edge_cases() {
        for v in [0u64, 1, u64::MAX >> 1, u64::MAX] {
            let mut buf = Vec::new();
            write_u64(&mut buf, v).unwrap();
            let mut r = Cursor::new(&buf);
            assert_eq!(read_u64(&mut r).unwrap(), v);
        }
    }

    #[test]
    fn write_and_read_bytes_various_sizes() {
        for len in [0, 1, 15, 16, 31, 32, 100] {
            let data: Vec<u8> = (0..len).map(|i| (i % 256) as u8).collect();
            let mut buf = Vec::new();
            write_bytes(&mut buf, &data).unwrap();
            assert_eq!(buf.len(), data.len());
            let mut out = vec![0u8; len];
            let mut r = Cursor::new(&buf);
            read_bytes(&mut r, &mut out).unwrap();
            assert_eq!(out, data);
        }
    }

    #[test]
    fn write_and_read_u32s_various() {
        let cases: [&[u32]; 4] = [&[], &[42], &[0, 1, u32::MAX], &[100, 200, 300, 400, 500]];
        for input in &cases {
            let mut buf = Vec::new();
            write_u32s(&mut buf, input).unwrap();
            assert_eq!(buf.len(), input.len() * 4);
            let mut out = vec![0u32; input.len()];
            let mut r = Cursor::new(&buf);
            read_u32s(&mut r, &mut out).unwrap();
            assert_eq!(out.as_slice(), *input);
        }
    }

    #[test]
    fn write_and_read_u64s_various() {
        let cases: [&[u64]; 4] = [&[], &[u64::MAX], &[0, 1, u64::MAX], &[100, 200, 300]];
        for input in &cases {
            let mut buf = Vec::new();
            write_u64s(&mut buf, input).unwrap();
            assert_eq!(buf.len(), input.len() * 8);
            let mut out = vec![0u64; input.len()];
            let mut r = Cursor::new(&buf);
            read_u64s(&mut r, &mut out).unwrap();
            assert_eq!(out.as_slice(), *input);
        }
    }

    #[test]
    fn read_errors_on_truncated_input() {
        assert!(read_u8(&mut Cursor::new(vec![])).is_err());
        assert!(read_u32(&mut Cursor::new(vec![])).is_err());
        assert!(read_u64(&mut Cursor::new(vec![])).is_err());
        assert!(read_hash(&mut Cursor::new(vec![])).is_err());
        assert!(read_u32(&mut Cursor::new(vec![0u8; 3])).is_err());
        assert!(read_u64(&mut Cursor::new(vec![0u8; 7])).is_err());
        assert!(read_hash(&mut Cursor::new(vec![0u8; 31])).is_err());
    }
}
