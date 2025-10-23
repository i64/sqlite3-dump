use std::sync::{Arc, OnceLock};
use winnow::binary::{be_f64, be_i16, be_i24, be_i32, be_i64, be_i8, be_u16, be_u32, be_u8};
use winnow::combinator::seq;
use winnow::error::{ContextError, ParserError};
use winnow::prelude::*;
use winnow::token::{literal, take};

use crate::model::*;

use super::HashMap;

static EMPTY_COLUMN_TYPES: OnceLock<Arc<Vec<SerialType>>> = OnceLock::new();

#[inline(always)]
fn empty_column_types() -> Arc<Vec<SerialType>> {
    Arc::clone(EMPTY_COLUMN_TYPES.get_or_init(|| Arc::new(Vec::with_capacity(0))))
}

const PAGE_TYPE_INTERIOR_INDEX: u8 = 0x02;
const PAGE_TYPE_INTERIOR_TABLE: u8 = 0x05;
const PAGE_TYPE_LEAF_INDEX: u8 = 0x0a;
const PAGE_TYPE_LEAF_TABLE: u8 = 0x0d;

pub(crate) fn db_header<'a, E: ParserError<&'a [u8]>>(input: &mut &'a [u8]) -> Result<DbHeader, E> {
    literal("SQLite format 3\0").parse_next(input)?;
    let page_size = be_u16.map(PageSize).parse_next(input)?;
    let _write_version = be_u8.parse_next(input)?;
    let _read_version = be_u8.parse_next(input)?;
    let reserved_size = be_u8.parse_next(input)?;
    let _max_payload_fraction: u8 = be_u8.parse_next(input)?;
    let _min_payload_fraction = be_u8.parse_next(input)?;
    let _leaf_payload_fraction = be_u8.parse_next(input)?;
    let _file_change_counter = be_u32.parse_next(input)?;
    let _db_size = be_u32.parse_next(input)?;
    let _first_freelist_page_no = be_u32.parse_next(input)?;
    let _total_freelist_pages = be_u32.parse_next(input)?;
    let _schema_cookie = be_u32.parse_next(input)?;
    let _schema_format_no = be_u32.parse_next(input)?;
    let _default_page_cache_size = be_u32.parse_next(input)?;
    let _no_largest_root_b_tree = be_u32.parse_next(input)?;
    let db_text_encoding_raw = be_u32.parse_next(input)?;
    let db_text_encoding = db_text_encoding_raw
        .try_into()
        .map_err(|_| E::from_input(input))?;
    let _user_version = be_u32.parse_next(input)?;
    let _incremental_vacuum_mode = be_u32.parse_next(input)?;
    let _application_id = be_u32.parse_next(input)?;
    let _reserved = take(20u8).parse_next(input)?;
    let _version_valid_for_no = be_u32.parse_next(input)?;
    let _sqlite_version_number = be_u32.parse_next(input)?;

    Ok(DbHeader {
        page_size,
        // write_version,
        // read_version,
        reserved_size,
        // max_payload_fraction,
        // min_payload_fraction,
        // leaf_payload_fraction,
        // file_change_counter,
        // db_size,
        // first_freelist_page_no,
        // total_freelist_pages,
        // schema_cookie,
        // schema_format_no,
        // default_page_cache_size,
        // no_largest_root_b_tree,
        db_text_encoding,
        // user_version,
        // incremental_vacuum_mode,
        // application_id,
        // version_valid_for_no,
        // sqlite_version_number,
    })
}

fn be_i48<'a, E: ParserError<&'a [u8]>>(input: &mut &'a [u8]) -> Result<i64, E> {
    let (head, tail): (u16, u32) = (be_u16, be_u32).parse_next(input)?;
    let mut x = (head as u64) << 32 | (tail as u64);
    if x & 0x80_00_00_00_00_00 != 0 {
        x |= 0xff_ff_00_00_00_00_00_00;
    };

    Ok(x as i64)
}

fn be_u64_varint<'a, E: ParserError<&'a [u8]>>(input: &mut &'a [u8]) -> Result<u64, E> {
    let mut res = 0;
    let i = *input;

    // SQLite varints can be up to 9 bytes
    let max_slice = &i[0..(i.len().min(9))];
    for (id, &b) in max_slice.iter().enumerate() {
        let b = b as u64;

        // Special case: 9th byte uses all 8 bits (no continuation bit)
        if id == 8 {
            res = (res << 8) | b;
            *input = &i[id + 1..];
            return Ok(res);
        }

        res = (res << 7) | (b & 0b0111_1111);

        if b >> 7 == 0 {
            *input = &i[id + 1..];
            return Ok(res);
        }
    }

    Err(E::from_input(input))
}

pub(crate) fn page_with_overflow<'a, E: ParserError<&'a [u8]>>(
    input: &mut &'a [u8],
    db_header: &'a DbHeader,
    page_start_offset: usize,
) -> Result<Page<'a>, E> {
    let page_type = input.first().ok_or_else(|| E::from_input(input))?;

    match *page_type {
        PAGE_TYPE_INTERIOR_INDEX => {
            interior_index_b_tree_page(page_start_offset).parse_next(input)?;
            Ok(Page::InteriorIndex)
        }
        PAGE_TYPE_INTERIOR_TABLE => {
            let page = interior_table_b_tree_page(page_start_offset).parse_next(input)?;
            Ok(Page::InteriorTable(page))
        }
        PAGE_TYPE_LEAF_INDEX => {
            leaf_index_b_tree_page(page_start_offset).parse_next(input)?;
            Ok(Page::LeafIndex)
        }
        PAGE_TYPE_LEAF_TABLE => {
            let page = leaf_table_b_tree_page_with_overflow(db_header, page_start_offset)
                .parse_next(input)?;
            Ok(Page::LeafTable(page))
        }
        _ => Err(E::from_input(input)),
    }
}

fn interior_page_header<'a, E: ParserError<&'a [u8]>>(
    input: &mut &'a [u8],
) -> Result<InteriorPageHeader, E> {
    seq!(InteriorPageHeader {
        _: be_u16, // first_freeblock_offset (unused)
        no_cells: be_u16,
        _: be_u16, // cell_content_offset (unused)
        _: be_u8,  // no_fragmented_bytes (unused)
        rightmost_pointer: be_u32,
    })
    .parse_next(input)
}

fn leaf_page_header<'a, E: ParserError<&'a [u8]>>(
    input: &mut &'a [u8],
) -> Result<LeafPageHeader, E> {
    seq!(LeafPageHeader {
        _: be_u16, // first_freeblock_offset (unused)
        no_cells: be_u16,
        _: be_u16, // cell_content_offset (unused)
        _: be_u8, // no_fragmented_bytes
    })
    .parse_next(input)
}

#[inline(always)]
fn interior_index_b_tree_page<'a, E: ParserError<&'a [u8]>>(
    page_start_offset: usize,
) -> impl Parser<&'a [u8], (), E> {
    move |input: &mut &'a [u8]| {
        let page_start = *input;
        literal(&[PAGE_TYPE_INTERIOR_INDEX][..]).parse_next(input)?;
        let header = interior_page_header.parse_next(input)?;

        for _ in 0..header.no_cells {
            let ptr = be_u16.parse_next(input)?;
            let cell_offset = ptr as usize - page_start_offset;
            let mut cell_input = &page_start[cell_offset..];
            interior_index_cell.parse_next(&mut cell_input)?;
        }
        Ok(())
    }
}

#[inline(always)]
fn column_types<'a, E: ParserError<&'a [u8]>>(input: &mut &'a [u8]) -> Result<Vec<SerialType>, E> {
    // most tables have < 20 columns (basic very scientific statistics), pre-allocate for common case
    let mut types = Vec::with_capacity(16);

    while !input.is_empty() {
        let val = be_u64_varint(input)?;
        types.push(SerialType::from(val));
    }

    Ok(types)
}

#[inline(always)]
fn parse_single_column<'a, E: ParserError<&'a [u8]>>(
    serial_type: &SerialType,
    input: &mut &'a [u8],
) -> Result<Option<Payload<'a>>, E> {
    match serial_type {
        SerialType::Null => Ok(None),
        SerialType::I8 => Ok(Some(Payload::I64(be_i8.parse_next(input)? as i64))),
        SerialType::I16 => Ok(Some(Payload::I64(be_i16.parse_next(input)? as i64))),
        SerialType::I24 => Ok(Some(Payload::I64(be_i24.parse_next(input)? as i64))),
        SerialType::I32 => Ok(Some(Payload::I64(be_i32.parse_next(input)? as i64))),
        SerialType::I48 => Ok(Some(Payload::I64(be_i48.parse_next(input)? as i64))),
        SerialType::I64 => Ok(Some(Payload::I64(be_i64.parse_next(input)?))),
        SerialType::F64 => Ok(Some(Payload::F64(be_f64.parse_next(input)?))),
        SerialType::Const0 => Ok(Some(Payload::I64(0))),
        SerialType::Const1 => Ok(Some(Payload::I64(1))),
        SerialType::Reserved => unimplemented!("reserved"),
        SerialType::Blob(_) if serial_type.size() == 0 => Ok(None),
        SerialType::Blob(_) => {
            let size = serial_type.size();
            let data = take(size).parse_next(input)?;
            Ok(Some(Payload::Blob(data)))
        }
        SerialType::Text(_) if serial_type.size() == 0 => Ok(None),
        SerialType::Text(_) => {
            let size = serial_type.size();
            let data = take(size).parse_next(input)?;
            Ok(Some(Payload::Text(RawText::new(data))))
        }
    }
}

fn interior_index_cell<'a, E: ParserError<&'a [u8]>>(input: &mut &'a [u8]) -> Result<(), E> {
    let _left_child_page_no = be_u32.parse_next(input)?;
    let payload_size = be_u64_varint.parse_next(input)?;

    // skip the payload we are not interested with the indexes
    take(payload_size as usize).parse_next(input)?;
    Ok(())
}

fn interior_table_cell<'a, E: ParserError<&'a [u8]>>(
    input: &mut &'a [u8],
) -> Result<InteriorCell, E> {
    seq!(InteriorCell {
        left_child_page_no: be_u32,
        _: be_u64_varint, // integer_key
    })
    .parse_next(input)
}

fn interior_table_b_tree_page<'a, E: ParserError<&'a [u8]>>(
    page_start_offset: usize,
) -> impl Parser<&'a [u8], InteriorTablePage, E> {
    move |input: &mut &'a [u8]| {
        let page_start = *input;
        literal(&[PAGE_TYPE_INTERIOR_TABLE][..]).parse_next(input)?;
        let header = interior_page_header.parse_next(input)?;

        let mut cells = Vec::with_capacity(header.no_cells as usize);
        for _ in 0..header.no_cells {
            let ptr = be_u16.parse_next(input)?;
            let cell_offset = ptr as usize - page_start_offset;
            let mut cell_input = &page_start[cell_offset..];
            let cell = interior_table_cell.parse_next(&mut cell_input)?;
            cells.push(cell);
        }

        Ok(InteriorTablePage { header, cells })
    }
}

fn leaf_index_b_tree_page<'a, E: ParserError<&'a [u8]>>(
    page_start_offset: usize,
) -> impl Parser<&'a [u8], (), E> {
    move |input: &mut &'a [u8]| {
        let page_start = *input;
        literal(&[PAGE_TYPE_LEAF_INDEX][..]).parse_next(input)?;
        let header = leaf_page_header.parse_next(input)?;

        for _ in 0..header.no_cells {
            let ptr = be_u16.parse_next(input)?;
            let cell_offset = ptr as usize - page_start_offset;
            let mut cell_input = &page_start[cell_offset..];
            leaf_index_cell.parse_next(&mut cell_input)?;
        }
        Ok(())
    }
}

fn leaf_index_cell<'a, E: ParserError<&'a [u8]>>(input: &mut &'a [u8]) -> Result<(), E> {
    let payload_size = be_u64_varint.parse_next(input)?;
    take(payload_size as usize).parse_next(input)?;
    Ok(())
}

fn leaf_table_b_tree_page_with_overflow<'a, E: ParserError<&'a [u8]>>(
    db_header: &'a DbHeader,
    page_start_offset: usize,
) -> impl Parser<&'a [u8], LeafTablePage<'a>, E> {
    move |input: &mut &'a [u8]| {
        let page_start = *input;
        literal(&[0x0du8][..]).parse_next(input)?;
        let header = leaf_page_header.parse_next(input)?;
        let mut cells = Vec::with_capacity(header.no_cells as usize);

        let mut cached_types: HashMap<u64, Arc<Vec<SerialType>>> = HashMap::default();

        for _ in 0..header.no_cells {
            let ptr = be_u16.parse_next(input)?;
            let cell_offset = ptr as usize - page_start_offset;
            let mut cell_input = &page_start[cell_offset..];
            let mut column_values = Vec::new();
            let mut cell = leaf_table_cell_with_overflow_cached(
                &mut cell_input,
                db_header,
                &header,
                &mut cached_types,
                &mut column_values,
            )?;

            cell.column_values = Some(column_values.into_iter().collect());
            cells.push(cell);
        }

        Ok(LeafTablePage { cells })
    }
}

#[inline(always)]
fn table_cell_payload_cached<'a, E: ParserError<&'a [u8]>>(
    input: &mut &'a [u8],
    max_local: Option<usize>,
    payload_size: u64,
    cached_types: &mut HashMap<u64, Arc<Vec<SerialType>>>,
    column_values: &mut Vec<Option<Payload<'a>>>,
) -> Result<TableCellPayload, E> {
    let header_size = be_u64_varint.parse_next(input)?;

    if header_size == 1 {
        return Ok(TableCellPayload {
            column_types: empty_column_types(),
        });
    }

    let header_bytes = &input[0..header_size as usize - 1];

    let header_hash = ahash::RandomState::with_seeds(0, 0, 0, 0).hash_one(header_bytes);

    let types = if let Some(cached) = cached_types.get(&header_hash) {
        *input = &input[header_size as usize - 1..];
        Arc::clone(cached)
    } else {
        let mut header_input = header_bytes;
        let types = column_types.parse_next(&mut header_input)?;
        *input = &input[header_size as usize - 1..];
        let types_arc = Arc::new(types);
        cached_types.insert(header_hash, Arc::clone(&types_arc));
        types_arc
    };

    let total_payload_size = (payload_size as usize).saturating_sub(header_size as usize);

    let local_data_size = if let Some(max_local) = max_local {
        let max_payload_bytes = max_local.saturating_sub(header_size as usize);
        input.len().min(max_payload_bytes).min(total_payload_size)
    } else {
        input.len().min(total_payload_size)
    };

    let local_data = &input[..local_data_size];

    let mut bytes_read = 0;
    column_values.resize(types.len(), None);

    for (idx, serial_type) in types.iter().enumerate() {
        let col_size = serial_type.size();
        if bytes_read + col_size <= local_data_size {
            let col_data = &local_data[bytes_read..bytes_read + col_size];
            let mut col_input = col_data;
            let value = parse_single_column(serial_type, &mut col_input)?;
            column_values[idx] = value;
            bytes_read += col_size;
        } else {
            column_values[idx] = None;
        }
    }
    *input = &input[local_data_size..];

    Ok(TableCellPayload {
        // header_size,
        column_types: types,
    })
}

fn leaf_table_cell_with_overflow_cached<'a, E: ParserError<&'a [u8]>>(
    input: &mut &'a [u8],
    db_header: &DbHeader,
    page_header: &LeafPageHeader,
    cached_types: &mut HashMap<u64, Arc<Vec<SerialType>>>,
    column_values: &mut Vec<Option<Payload<'a>>>,
) -> Result<LeafTableCell<'a>, E> {
    let payload_size = be_u64_varint.parse_next(input)?;
    let rowid = be_u64_varint.parse_next(input)?;

    let (local_size, overflow_size) = page_header.local_and_overflow_size(db_header, payload_size);

    let payload = table_cell_payload_cached(
        input,
        Some(local_size),
        payload_size,
        cached_types,
        column_values,
    )?;

    let overflow_page_no = if overflow_size.is_some() {
        Some(be_u32.parse_next(input)?)
    } else {
        None
    };

    Ok(LeafTableCell {
        payload_size,
        rowid,
        payload,
        overflow_page_no,
        column_values: None,
    })
}

pub(crate) fn overflow_page<'a, E: ParserError<&'a [u8]>>(
    input: &mut &'a [u8],
) -> Result<OverflowPage<'a>, E> {
    let next_page_no_raw = be_u32.parse_next(input)?;
    let next_page_no = if next_page_no_raw == 0 {
        None
    } else {
        Some(next_page_no_raw)
    };

    let payload = *input;
    *input = &[];

    Ok((next_page_no, payload))
}

pub(crate) enum CellType<'a, 'b> {
    LeafTable(LeafTableCell<'a>, &'b Vec<Option<Payload<'a>>>), // cell + column values reference
    // LeafIndex,
    InteriorTable(u32),          // page number
    InteriorTableRightmost(u32), // rightmost pointer
}

pub(crate) fn stream_page_cells<'a, F>(
    input: &'a [u8],
    db_header: &DbHeader,
    page_start_offset: usize,
    column_values: &'a mut Vec<Option<Payload<'a>>>,
    cached_types: &mut HashMap<u64, Arc<Vec<SerialType>>>,
    mut callback: F,
) -> Result<(), crate::error::SQLiteError>
where
    F: for<'b> FnMut(
        CellType<'a, 'b>,
        &mut HashMap<u64, Arc<Vec<SerialType>>>,
    ) -> Result<(), crate::error::SQLiteError>,
{
    let mut input_mut = input;
    let page_start = input;

    let page_type = *input
        .first()
        .ok_or_else(|| crate::error::SQLiteError::Other("Empty page".into()))?;
    literal(&[page_type][..])
        .parse_next(&mut input_mut)
        .map_err(|_: ContextError| crate::error::SQLiteError::Other("Invalid page type".into()))?;

    match page_type {
        PAGE_TYPE_LEAF_TABLE => {
            let header = leaf_page_header::<ContextError>(&mut input_mut).map_err(|_| {
                crate::error::SQLiteError::Other("Failed to parse leaf header".into())
            })?;

            for _ in 0..header.no_cells {
                let ptr = be_u16
                    .parse_next(&mut input_mut)
                    .map_err(|_: ContextError| {
                        crate::error::SQLiteError::Other("Failed to parse cell pointer".into())
                    })?;
                let cell_offset = ptr as usize - page_start_offset;
                let mut cell_input = &page_start[cell_offset..];

                let cell = {
                    leaf_table_cell_with_overflow_cached(
                        &mut cell_input,
                        db_header,
                        &header,
                        cached_types,
                        column_values,
                    )
                    .map_err(|_: ContextError| {
                        crate::error::SQLiteError::Other("Failed to parse cell".into())
                    })?
                };
                callback(CellType::LeafTable(cell, &*column_values), cached_types)?;
            }
        }
        PAGE_TYPE_LEAF_INDEX => {
            let header = leaf_page_header::<ContextError>(&mut input_mut).map_err(|_| {
                crate::error::SQLiteError::Other("Failed to parse leaf index header".into())
            })?;

            for _ in 0..header.no_cells {
                let ptr = be_u16
                    .parse_next(&mut input_mut)
                    .map_err(|_: ContextError| {
                        crate::error::SQLiteError::Other("Failed to parse cell pointer".into())
                    })?;
                let cell_offset = ptr as usize - page_start_offset;
                let mut cell_input = &page_start[cell_offset..];
                leaf_index_cell::<ContextError>(&mut cell_input).map_err(|_: ContextError| {
                    crate::error::SQLiteError::Other("Failed to parse index cell".into())
                })?;
            }
        }
        PAGE_TYPE_INTERIOR_TABLE => {
            let header = interior_page_header::<ContextError>(&mut input_mut).map_err(|_| {
                crate::error::SQLiteError::Other("Failed to parse interior header".into())
            })?;

            for _ in 0..header.no_cells {
                let ptr = be_u16
                    .parse_next(&mut input_mut)
                    .map_err(|_: ContextError| {
                        crate::error::SQLiteError::Other("Failed to parse cell pointer".into())
                    })?;
                let cell_offset = ptr as usize - page_start_offset;
                let mut cell_input = &page_start[cell_offset..];
                let cell = interior_table_cell::<ContextError>(&mut cell_input)
                    .map_err(|_| crate::error::SQLiteError::Other("Failed to parse cell".into()))?;

                callback(
                    CellType::InteriorTable(cell.left_child_page_no),
                    cached_types,
                )?;
            }

            if header.rightmost_pointer > 0 {
                callback(
                    CellType::InteriorTableRightmost(header.rightmost_pointer),
                    cached_types,
                )?;
            }
        }
        _ => {
            return Err(crate::error::SQLiteError::Other(
                "Unsupported page type for streaming".into(),
            ));
        }
    }

    Ok(())
}
