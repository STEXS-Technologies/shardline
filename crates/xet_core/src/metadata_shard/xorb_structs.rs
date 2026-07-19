use std::io::{Cursor, Read, Write};
use std::mem::size_of;

use bytes::Bytes;

use super::hash_is_global_dedup_eligible;
use crate::merklehash::MerkleHash;
use crate::utils::serialization_utils::*;

pub const MDB_DEFAULT_XORB_FLAG: u32 = 0;
pub const MDB_CHUNK_WITH_GLOBAL_DEDUP_FLAG: u32 = 1 << 31;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct XorbChunkSequenceHeader {
    pub xorb_hash: MerkleHash,
    pub xorb_flags: u32,
    pub num_entries: u64,
    pub num_bytes_in_xorb: u64,
    pub num_bytes_on_disk: u64,
}

impl XorbChunkSequenceHeader {
    pub fn new(
        xorb_hash: MerkleHash,
        num_entries: u64,
        num_bytes_in_xorb: u64,
    ) -> Self {
        Self {
            xorb_hash,
            xorb_flags: MDB_DEFAULT_XORB_FLAG,
            num_entries,
            num_bytes_in_xorb,
            num_bytes_on_disk: 0,
        }
    }

    pub fn bookend() -> Self {
        Self {
            xorb_hash: [!0u64; 4].into(),
            ..Default::default()
        }
    }

    pub fn is_bookend(&self) -> bool {
        self.xorb_hash == [!0u64; 4].into()
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, std::io::Error> {
        let mut buf = [0u8; size_of::<Self>()];
        {
            let mut writer_cur = Cursor::new(&mut buf[..]);
            let writer = &mut writer_cur;
            write_hash(writer, &self.xorb_hash)?;
            write_u32(writer, self.xorb_flags)?;
            write_u64(writer, self.num_entries)?;
            write_u64(writer, self.num_bytes_in_xorb)?;
            write_u64(writer, self.num_bytes_on_disk)?;
        }
        writer.write_all(&buf[..])?;
        Ok(size_of::<Self>())
    }

    pub fn deserialize<R: Read>(reader: &mut R, _version: u64) -> Result<Self, std::io::Error> {
        let mut v = [0u8; size_of::<Self>()];
        reader.read_exact(&mut v[..])?;
        let mut reader_curs = Cursor::new(&v);
        let reader = &mut reader_curs;
        Ok(Self {
            xorb_hash: read_hash(reader)?,
            xorb_flags: read_u32(reader)?,
            num_entries: read_u64(reader)?,
            num_bytes_in_xorb: read_u64(reader)?,
            num_bytes_on_disk: read_u64(reader)?,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct XorbChunkSequenceEntry {
    pub chunk_hash: MerkleHash,
    pub chunk_byte_range_start: u64,
    pub unpacked_segment_bytes: u64,
    pub flags: u32,
    pub _unused: u64,
}

impl XorbChunkSequenceEntry {
    pub fn new(
        chunk_hash: MerkleHash,
        unpacked_segment_bytes: u64,
        chunk_byte_range_start: u64,
    ) -> Self {
        Self {
            chunk_hash,
            unpacked_segment_bytes,
            chunk_byte_range_start,
            flags: 0,
            _unused: 0,
        }
    }

    pub fn with_global_dedup_flag(self, is_global_dedup_chunk: bool) -> Self {
        if is_global_dedup_chunk {
            Self {
                flags: self.flags | MDB_CHUNK_WITH_GLOBAL_DEDUP_FLAG,
                ..self
            }
        } else {
            Self {
                flags: self.flags & !MDB_CHUNK_WITH_GLOBAL_DEDUP_FLAG,
                ..self
            }
        }
    }

    pub fn is_global_dedup_eligible(&self) -> bool {
        (self.flags & MDB_CHUNK_WITH_GLOBAL_DEDUP_FLAG) != 0
            || hash_is_global_dedup_eligible(&self.chunk_hash)
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, std::io::Error> {
        let mut buf = [0u8; size_of::<Self>()];
        {
            let mut writer_cur = Cursor::new(&mut buf[..]);
            let writer = &mut writer_cur;
            write_hash(writer, &self.chunk_hash)?;
            write_u64(writer, self.chunk_byte_range_start)?;
            write_u64(writer, self.unpacked_segment_bytes)?;
            write_u32(writer, self.flags)?;
            write_u64(writer, self._unused)?;
        }
        writer.write_all(&buf[..])?;
        Ok(size_of::<XorbChunkSequenceEntry>())
    }

    pub fn deserialize<R: Read>(reader: &mut R, _version: u64) -> Result<Self, std::io::Error> {
        let mut v = [0u8; size_of::<Self>()];
        reader.read_exact(&mut v[..])?;
        let mut reader_curs = Cursor::new(&v);
        let reader = &mut reader_curs;
        Ok(Self {
            chunk_hash: read_hash(reader)?,
            chunk_byte_range_start: read_u64(reader)?,
            unpacked_segment_bytes: read_u64(reader)?,
            flags: read_u32(reader)?,
            _unused: read_u64(reader)?,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MDBXorbInfo {
    pub metadata: XorbChunkSequenceHeader,
    pub chunks: Vec<XorbChunkSequenceEntry>,
}

impl MDBXorbInfo {
    pub fn num_bytes(&self) -> u64 {
        (size_of::<XorbChunkSequenceHeader>()
            + self.chunks.len() * size_of::<XorbChunkSequenceEntry>()) as u64
    }

    pub fn deserialize<R: Read>(reader: &mut R, version: u64) -> Result<Option<Self>, std::io::Error> {
        let metadata = XorbChunkSequenceHeader::deserialize(reader, version)?;
        if metadata.is_bookend() {
            return Ok(None);
        }
        let mut chunks = Vec::with_capacity(metadata.num_entries as usize);
        for _ in 0..metadata.num_entries {
            chunks.push(XorbChunkSequenceEntry::deserialize(reader, version)?);
        }
        Ok(Some(Self { metadata, chunks }))
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, std::io::Error> {
        let mut n_out_bytes = 0;
        n_out_bytes += self.metadata.serialize(writer)?;
        for chunk in self.chunks.iter() {
            n_out_bytes += chunk.serialize(writer)?;
        }
        Ok(n_out_bytes)
    }

    pub fn chunks_and_boundaries(&self) -> Vec<(MerkleHash, u64)> {
        self.chunks
            .iter()
            .map(|entry| {
                (
                    entry.chunk_hash,
                    entry.chunk_byte_range_start + entry.unpacked_segment_bytes,
                )
            })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MDBXorbInfoView {
    header: XorbChunkSequenceHeader,
    data: Bytes,
}

impl MDBXorbInfoView {
    pub fn new(data: Bytes) -> std::io::Result<Self> {
        let mut reader = Cursor::new(&data);
        let header = XorbChunkSequenceHeader::deserialize(&mut reader, 3)?;
        Self::from_data_and_header(header, data)
    }

    pub fn from_data_and_header(
        header: XorbChunkSequenceHeader,
        data: Bytes,
    ) -> std::io::Result<Self> {
        let n = header.num_entries as usize;
        let n_bytes =
            size_of::<XorbChunkSequenceHeader>() + n * size_of::<XorbChunkSequenceEntry>();
        if data.len() < n_bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Provided slice too small to read Xorb Info",
            ));
        }
        Ok(Self { header, data })
    }

    pub fn header(&self) -> &XorbChunkSequenceHeader {
        &self.header
    }

    pub fn xorb_hash(&self) -> MerkleHash {
        self.header.xorb_hash
    }

    pub fn num_entries(&self) -> usize {
        self.header.num_entries as usize
    }

    pub fn chunk(&self, idx: usize) -> XorbChunkSequenceEntry {
        XorbChunkSequenceEntry::deserialize(
            &mut Cursor::new(
                &self.data[(size_of::<XorbChunkSequenceHeader>()
                    + idx * size_of::<XorbChunkSequenceEntry>())..],
            ),
            3,
        )
        .expect("bookkeeping error on data bounds")
    }

    pub fn byte_size(&self) -> usize {
        let n = self.num_entries();
        size_of::<XorbChunkSequenceHeader>() + n * size_of::<XorbChunkSequenceEntry>()
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> std::io::Result<usize> {
        let n_bytes = self.byte_size();
        writer.write_all(&self.data[..n_bytes])?;
        Ok(n_bytes)
    }
}

impl From<&MDBXorbInfoView> for MDBXorbInfo {
    fn from(view: &MDBXorbInfoView) -> Self {
        let chunks: Vec<XorbChunkSequenceEntry> =
            (0..view.num_entries()).map(|i| view.chunk(i)).collect();
        let total_bytes: u64 = chunks
            .last()
            .map(|c| c.chunk_byte_range_start + c.unpacked_segment_bytes)
            .unwrap_or(0);
        MDBXorbInfo {
            metadata: XorbChunkSequenceHeader::new(
                view.xorb_hash(),
                chunks.len() as u64,
                total_bytes,
            ),
            chunks,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use bytes::Bytes;

    use super::*;
    use crate::merklehash::compute_data_hash;

    fn make_header(num_entries: u64, bytes: u64) -> XorbChunkSequenceHeader {
        XorbChunkSequenceHeader::new(compute_data_hash(b"xorb"), num_entries, bytes)
    }

    fn make_entry(unpacked: u64, start: u64) -> XorbChunkSequenceEntry {
        XorbChunkSequenceEntry::new(compute_data_hash(b"chunk"), unpacked, start)
    }

    fn serialize_header_and_entries(
        header: &XorbChunkSequenceHeader,
        entries: &[XorbChunkSequenceEntry],
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        header.serialize(&mut buf).unwrap();
        for e in entries {
            e.serialize(&mut buf).unwrap();
        }
        buf
    }

    // ======= XorbChunkSequenceHeader =======

    #[test]
    fn header_new() {
        let h = make_header(3, 500);
        assert_eq!(h.xorb_flags, 0);
        assert_eq!(h.num_entries, 3);
        assert_eq!(h.num_bytes_in_xorb, 500);
        assert_eq!(h.num_bytes_on_disk, 0);
    }

    #[test]
    fn header_bookend() {
        let b = XorbChunkSequenceHeader::bookend();
        assert!(b.is_bookend());
        assert!(!make_header(0, 0).is_bookend());
    }

    #[test]
    fn header_serialize_roundtrip() {
        let h = make_header(2, 300);
        let mut buf = Vec::new();
        h.serialize(&mut buf).unwrap();
        let mut r = Cursor::new(&buf);
        let h2 = XorbChunkSequenceHeader::deserialize(&mut r, 3).unwrap();
        assert_eq!(h.xorb_hash, h2.xorb_hash);
        assert_eq!(h.num_entries, h2.num_entries);
        assert_eq!(h.num_bytes_in_xorb, h2.num_bytes_in_xorb);
        assert_eq!(h.num_bytes_on_disk, h2.num_bytes_on_disk);
    }

    #[test]
    fn header_default() {
        let h = XorbChunkSequenceHeader::default();
        assert_eq!(h.num_entries, 0);
        assert_eq!(h.xorb_hash, MerkleHash::default());
    }

    // ======= XorbChunkSequenceEntry =======

    #[test]
    fn entry_new() {
        let e = make_entry(512, 100);
        assert_eq!(e.unpacked_segment_bytes, 512);
        assert_eq!(e.chunk_byte_range_start, 100);
        assert_eq!(e.flags, 0);
        assert_eq!(e._unused, 0);
    }

    #[test]
    fn entry_with_global_dedup_flag() {
        let e = make_entry(100, 0);
        let flagged = e.with_global_dedup_flag(true);
        assert_ne!(flagged.flags, 0);
        let unflagged = flagged.with_global_dedup_flag(false);
        assert_eq!(unflagged.flags, 0);
    }

    #[test]
    fn entry_is_global_dedup_eligible_by_flag() {
        let h = compute_data_hash(b"test");
        let e = XorbChunkSequenceEntry::new(h, 100, 0);
        let e = e.with_global_dedup_flag(true);
        assert!(e.is_global_dedup_eligible());
    }

    #[test]
    fn entry_is_global_dedup_eligible_by_hash() {
        // Default hash (all zeros) => 0 % 1024 == 0 => eligible
        let e = XorbChunkSequenceEntry::new(MerkleHash::default(), 100, 0);
        assert!(e.is_global_dedup_eligible());
    }

    #[test]
    fn entry_is_global_dedup_eligible_false() {
        let h = compute_data_hash(b"non_eligible");
        if !h[3].to_le().is_multiple_of(1024) {
            let e = XorbChunkSequenceEntry::new(h, 100, 0);
            assert!(!e.is_global_dedup_eligible());
        }
    }

    #[test]
    fn entry_serialize_roundtrip() {
        let e = make_entry(256, 64);
        let mut buf = Vec::new();
        e.serialize(&mut buf).unwrap();
        let mut r = Cursor::new(&buf);
        let e2 = XorbChunkSequenceEntry::deserialize(&mut r, 3).unwrap();
        assert_eq!(e.chunk_hash, e2.chunk_hash);
        assert_eq!(e.unpacked_segment_bytes, e2.unpacked_segment_bytes);
        assert_eq!(e.chunk_byte_range_start, e2.chunk_byte_range_start);
        assert_eq!(e.flags, e2.flags);
    }

    #[test]
    fn entry_default() {
        let e = XorbChunkSequenceEntry::default();
        assert_eq!(e.chunk_hash, MerkleHash::default());
        assert_eq!(e.unpacked_segment_bytes, 0);
    }

    // ======= MDBXorbInfo =======

    #[test]
    fn info_num_bytes() {
        let info = MDBXorbInfo {
            metadata: make_header(2, 200),
            chunks: vec![make_entry(100, 0), make_entry(100, 100)],
        };
        assert_eq!(info.num_bytes(), (64 + 2 * 64) as u64);
    }

    #[test]
    fn info_num_bytes_zero_chunks() {
        let info = MDBXorbInfo {
            metadata: make_header(0, 0),
            chunks: vec![],
        };
        assert_eq!(info.num_bytes(), 64);
    }

    #[test]
    fn info_serialize_roundtrip() {
        let info = MDBXorbInfo {
            metadata: make_header(1, 200),
            chunks: vec![make_entry(200, 0)],
        };
        let mut buf = Vec::new();
        info.serialize(&mut buf).unwrap();
        let mut r = Cursor::new(&buf);
        let info2 = MDBXorbInfo::deserialize(&mut r, 3).unwrap().unwrap();
        assert_eq!(info.metadata.xorb_hash, info2.metadata.xorb_hash);
        assert_eq!(info.chunks.len(), info2.chunks.len());
        assert_eq!(info.chunks[0].chunk_hash, info2.chunks[0].chunk_hash);
    }

    #[test]
    fn info_deserialize_bookend() {
        let bookend = XorbChunkSequenceHeader::bookend();
        let mut buf = Vec::new();
        bookend.serialize(&mut buf).unwrap();
        let mut r = Cursor::new(&buf);
        assert!(MDBXorbInfo::deserialize(&mut r, 3).unwrap().is_none());
    }

    #[test]
    fn info_deserialize_empty_chunks() {
        let h = make_header(0, 0);
        let mut buf = Vec::new();
        h.serialize(&mut buf).unwrap();
        let mut r = Cursor::new(&buf);
        let info = MDBXorbInfo::deserialize(&mut r, 3).unwrap().unwrap();
        assert!(info.chunks.is_empty());
    }

    #[test]
    fn info_chunks_and_boundaries() {
        let info = MDBXorbInfo {
            metadata: make_header(2, 300),
            chunks: vec![make_entry(100, 0), make_entry(200, 100)],
        };
        let bounds = info.chunks_and_boundaries();
        assert_eq!(bounds.len(), 2);
        assert_eq!(bounds[0].1, 100);
        assert_eq!(bounds[1].1, 300);
    }

    #[test]
    fn info_default() {
        let info = MDBXorbInfo::default();
        assert!(info.chunks.is_empty());
    }

    // ======= MDBXorbInfoView =======

    #[test]
    fn view_new() {
        let data = serialize_header_and_entries(&make_header(1, 100), &[make_entry(100, 0)]);
        let view = MDBXorbInfoView::new(Bytes::from(data)).unwrap();
        assert_eq!(view.num_entries(), 1);
        assert_ne!(view.xorb_hash(), MerkleHash::default());
    }

    #[test]
    fn view_header() {
        let header = XorbChunkSequenceHeader::new(compute_data_hash(b"test2"), 3, 300);
        let data = serialize_header_and_entries(
            &header,
            &[
                make_entry(100, 0),
                make_entry(100, 100),
                make_entry(100, 200),
            ],
        );
        let view = MDBXorbInfoView::new(Bytes::from(data)).unwrap();
        assert_eq!(view.header().xorb_hash, header.xorb_hash);
        assert_eq!(view.num_entries(), 3);
    }

    #[test]
    fn view_chunk_accessor() {
        let entries = vec![make_entry(100, 0), make_entry(200, 100)];
        let data = serialize_header_and_entries(&make_header(2, 300), &entries);
        let view = MDBXorbInfoView::new(Bytes::from(data)).unwrap();
        let c0 = view.chunk(0);
        assert_eq!(c0.unpacked_segment_bytes, 100);
        let c1 = view.chunk(1);
        assert_eq!(c1.unpacked_segment_bytes, 200);
    }

    #[test]
    fn view_byte_size() {
        let data = serialize_header_and_entries(
            &make_header(2, 200),
            &[make_entry(100, 0), make_entry(100, 100)],
        );
        let view = MDBXorbInfoView::new(Bytes::from(data)).unwrap();
        let expected =
            size_of::<XorbChunkSequenceHeader>() + 2 * size_of::<XorbChunkSequenceEntry>();
        assert_eq!(view.byte_size(), expected);
    }

    #[test]
    fn view_serialize_roundtrip() {
        let data = serialize_header_and_entries(&make_header(1, 50), &[make_entry(50, 0)]);
        let view = MDBXorbInfoView::new(Bytes::from(data.clone())).unwrap();
        let mut out = Vec::new();
        view.serialize(&mut out).unwrap();
        assert_eq!(data, out);
    }

    #[test]
    fn view_from_data_too_small() {
        let header = make_header(2, 100);
        let result = MDBXorbInfoView::from_data_and_header(header, Bytes::from(vec![0u8; 10]));
        assert!(result.is_err());
    }

    #[test]
    fn view_into_mdb_xorb_info() {
        let entries = vec![make_entry(50, 0), make_entry(100, 50)];
        let data = serialize_header_and_entries(&make_header(2, 150), &entries);
        let view = MDBXorbInfoView::new(Bytes::from(data)).unwrap();
        let info: MDBXorbInfo = (&view).into();
        assert_eq!(info.chunks.len(), 2);
        assert_eq!(info.chunks[0].unpacked_segment_bytes, 50);
        assert_eq!(info.chunks[1].unpacked_segment_bytes, 100);
        assert_eq!(info.metadata.num_bytes_in_xorb, 150);
    }

    #[test]
    fn view_into_mdb_xorb_info_empty() {
        let header = make_header(0, 0);
        let data = serialize_header_and_entries(&header, &[]);
        let view = MDBXorbInfoView::new(Bytes::from(data)).unwrap();
        let info: MDBXorbInfo = (&view).into();
        assert!(info.chunks.is_empty());
        assert_eq!(info.metadata.num_bytes_in_xorb, 0);
    }

    #[test]
    fn mdb_xorb_info_default_flag_values() {
        assert_eq!(MDB_DEFAULT_XORB_FLAG, 0);
        assert_eq!(MDB_CHUNK_WITH_GLOBAL_DEDUP_FLAG, 1 << 31);
    }
}
