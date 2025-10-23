use crate::error::SQLiteError;
use simdutf8::basic::from_utf8 as simd_from_utf8;
use std::borrow::Cow;

const SIMD_CHUNK_SIZE: usize = 64;

pub struct DbHeader {
    pub page_size: PageSize,
    // pub(crate) write_version: u8,
    // pub(crate) read_version: u8,
    /// number of bytes reserved at the end of each page (usually 0)
    pub(crate) reserved_size: u8,
    // pub(crate) max_payload_fraction: u8,
    // pub(crate) min_payload_fraction: u8,
    // pub(crate) leaf_payload_fraction: u8,
    // pub(crate) file_change_counter: u32,
    // pub(crate) db_size: u32,
    // pub(crate) first_freelist_page_no: u32,
    // pub(crate) total_freelist_pages: u32,
    // pub(crate) schema_cookie: u32,
    // pub(crate) schema_format_no: u32,
    // pub(crate) default_page_cache_size: u32,
    // pub(crate) no_largest_root_b_tree: u32,
    pub db_text_encoding: TextEncoding,
    // pub(crate) user_version: u32,
    // pub(crate) incremental_vacuum_mode: u32,
    // pub(crate) application_id: u32,
    // pub(crate) version_valid_for_no: u32,
    // pub(crate) sqlite_version_number: u32,
}

impl DbHeader {
    /// calculate the usable page size
    pub(crate) fn usable_page_size(&self) -> usize {
        self.page_size.real_size() - (self.reserved_size as usize)
    }
}

pub struct PageSize(pub(crate) u16);

impl PageSize {
    #[inline(always)]
    pub fn real_size(&self) -> usize {
        match self.0 {
            1 => 0x1_00_00,
            _ => self.0.into(),
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum TextEncoding {
    Utf8,
    Utf16Le,
    Utf16Be,
}

impl TryFrom<u32> for TextEncoding {
    type Error = SQLiteError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        use TextEncoding::*;

        match value {
            1 => Ok(Utf8),
            2 => Ok(Utf16Le),
            3 => Ok(Utf16Be),
            _ => Err(SQLiteError::UnknownTextEncodingError(value)),
        }
    }
}

pub(crate) enum Page<'a> {
    InteriorIndex,
    LeafIndex,
    InteriorTable(InteriorTablePage),
    LeafTable(LeafTablePage<'a>),
    // Overflow(OverflowPage<'a>),
}

pub(crate) struct InteriorPageHeader {
    // pub(crate) first_freeblock_offset: Option<u16>,
    pub(crate) no_cells: u16,
    // pub(crate) cell_content_offset: u16,
    // pub(crate) no_fragmented_bytes: u8,
    pub(crate) rightmost_pointer: u32,
}


/// Interior table B-tree page
pub(crate) struct InteriorTablePage {
    pub(crate) header: InteriorPageHeader,
    pub(crate) cells: Vec<InteriorCell>,
}

pub(crate) struct InteriorCell {
    pub(crate) left_child_page_no: u32,
}

pub(crate) struct LeafPageHeader {
    // pub(crate) first_freeblock_offset: Option<u16>,
    pub(crate) no_cells: u16,
    // pub(crate) cell_content_offset: u16,
    // pub(crate) no_fragmented_bytes: u8,
}

impl LeafPageHeader {
    /// calculate local and overflow payload sizes for a table leaf cell
    /// returns (local_size, overflow_size) where overflow_size is None if payload fits locally
    pub(crate) fn local_and_overflow_size(
        &self,
        db_header: &DbHeader,
        payload_size: u64,
    ) -> (usize, Option<usize>) {
        let usable = db_header.usable_page_size();
        let max_local = usable - 35;

        if payload_size as usize <= max_local {
            return (payload_size as usize, None);
        }

        // payload doesn't fit locally, calculate local and overflow portions
        let min_local = ((usable - 12) * 32 / 255) - 23;
        let k = min_local + ((payload_size as usize - min_local) % (usable - 4));
        let local_size = if k <= max_local { k } else { min_local };
        let overflow_size = payload_size as usize - local_size;

        (local_size, Some(overflow_size))
    }
}


pub(crate) struct LeafTablePage<'a> {
    // pub(crate) header: LeafPageHeader,
    pub(crate) cells: Vec<LeafTableCell<'a>>,
}

impl<'a> std::ops::Deref for LeafTablePage<'a> {
    type Target = [LeafTableCell<'a>];

    fn deref(&self) -> &Self::Target {
        &self.cells
    }
}

#[derive(Default)]
pub struct TableCellPayload {
    // pub(crate) header_size: u64,
    pub(crate) column_types: std::sync::Arc<Vec<SerialType>>,
}

#[derive(Default)]
pub struct LeafTableCell<'a> {
    pub payload_size: u64,
    pub rowid: u64,
    pub payload: TableCellPayload,
    pub overflow_page_no: Option<u32>,
    pub(crate) column_values: Option<Vec<Option<Payload<'a>>>>,
}

#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub(crate) enum SerialType {
    Null,
    I8,
    I16,
    I24,
    I32,
    I48,
    I64,
    F64,
    Const0,
    Const1,
    Reserved,
    Blob(u64),
    Text(u64),
}

impl From<u64> for SerialType {
    fn from(value: u64) -> Self {
        use SerialType::*;
        match value {
            0 => Null,
            1 => I8,
            2 => I16,
            3 => I24,
            4 => I32,
            5 => I48,
            6 => I64,
            7 => F64,
            8 => Const0,
            9 => Const1,
            10 | 11 => Reserved,
            n if n >= 12 && n % 2 == 0 => Blob(n),
            n if n >= 13 && n % 2 == 1 => Text(n),
            _ => unreachable!(),
        }
    }
}

impl SerialType {
    #[inline(always)]
    pub(crate) fn size(&self) -> usize {
        match self {
            SerialType::Null => 0,
            SerialType::I8 => 1,
            SerialType::I16 => 2,
            SerialType::I24 => 3,
            SerialType::I32 => 4,
            SerialType::I48 => 6,
            SerialType::I64 => 8,
            SerialType::F64 => 8,
            SerialType::Const0 => 0,
            SerialType::Const1 => 0,
            SerialType::Reserved => unimplemented!("reserved"),
            SerialType::Blob(n) => ((n - 12) / 2).try_into().unwrap(),
            SerialType::Text(n) => ((n - 13) / 2).try_into().unwrap(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RawText<'a>(&'a [u8]);

impl<'a> RawText<'a> {
    pub(crate) fn new(v: &'a [u8]) -> Self {
        RawText(v)
    }

    #[inline(always)]
    pub fn decode(&self, text_encoding: TextEncoding) -> Cow<'a, str> {
        match text_encoding {
            TextEncoding::Utf8 => {
                let s = if self.0.len() < SIMD_CHUNK_SIZE {
                    std::str::from_utf8(self.0).expect("invalid UTF-8 in SQLite database")
                } else {
                    simd_from_utf8(self.0).expect("invalid UTF-8 in SQLite database")
                };
                Cow::Borrowed(s)
            }
            TextEncoding::Utf16Le => {
                let u16_slice: Vec<_> = self
                    .0
                    .chunks_exact(2)
                    .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
                    .collect();
                let decoded =
                    String::from_utf16(&u16_slice).expect("invalid UTF-16 LE in SQLite database");
                Cow::Owned(decoded)
            }
            TextEncoding::Utf16Be => {
                let u16_slice: Vec<_> = self
                    .0
                    .chunks_exact(2)
                    .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
                    .collect();
                let decoded =
                    String::from_utf16(&u16_slice).expect("invalid UTF-16 BE in SQLite database");
                Cow::Owned(decoded)
            }
        }
    }
}

impl<'a> From<&'a str> for RawText<'a> {
    fn from(value: &'a str) -> Self {
        RawText(value.as_bytes())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Payload<'a> {
    I64(i64),
    F64(f64),
    Blob(&'a [u8]),
    Text(RawText<'a>),
}

impl<'a> Payload<'a> {
    #[inline(always)]
    pub(crate) fn as_u32(&self) -> Option<u32> {
        match self {
            Payload::I64(n) if (*n as u64) <= u32::MAX as u64 => Some(*n as u32),
            _ => None,
        }
    }
}

impl<'a> From<&'a str> for Payload<'a> {
    fn from(value: &'a str) -> Self {
        Payload::Text(value.into())
    }
}

impl<'a> From<&'a [u8]> for Payload<'a> {
    fn from(value: &'a [u8]) -> Self {
        Payload::Blob(value)
    }
}

impl<'a> From<i64> for Payload<'a> {
    fn from(value: i64) -> Self {
        Payload::I64(value)
    }
}

impl<'a> From<f64> for Payload<'a> {
    fn from(value: f64) -> Self {
        Payload::F64(value)
    }
}

pub(crate) type OverflowPage<'a> = (Option<u32>, &'a [u8]);