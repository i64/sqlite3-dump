#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

extern crate core;

use memmap2::{Mmap, MmapOptions};
use once_cell::unsync::OnceCell;
use std::fs::File;
use std::path::Path;

use winnow::error::ContextError;

use crate::error::SQLiteError;
use crate::model::{DbHeader, Page};
use crate::parser::{db_header, overflow_page};

pub mod error;
pub mod model;
pub mod parquet_writer;
mod parser;

const HEADER_SIZE: usize = 100;

pub type HashMap<K, V> = std::collections::HashMap<K, V, ahash::RandomState>;

const SQLITE_MASTER_TABLE_SIZE: usize = 5;

#[repr(usize)]
enum SqliteMasterTable {
    Type = 0,
    Name = 1,
    // TblName = 2,
    RootPage = 3,
    Sql = 4,
}

#[inline(always)]
fn get_table_cell_values<'a>(
    cell: &'a model::LeafTableCell<'a>,
) -> &'a [Option<model::Payload<'a>>] {
    cell.column_values.as_deref().unwrap_or_default()
}

pub struct SqlSchema {
    pub columns: Vec<turso_parser::ast::ColumnDefinition>,
}

impl TryFrom<String> for SqlSchema {
    type Error = SQLiteError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        use turso_parser::ast::{Cmd, CreateTableBody, Stmt};
        use turso_parser::parser::Parser;
        let mut parser = Parser::new(value.as_bytes());

        match parser.next_cmd() {
            Ok(Some(Cmd::Stmt(Stmt::CreateTable {
                body: CreateTableBody::ColumnsAndConstraints { columns, .. },
                ..
            }))) => Ok(SqlSchema { columns }),
            Err(err) => Err(SQLiteError::SqlQueryErr(err)),
            _ => Err(SQLiteError::ParsingError(format!(
                "Unexpected SQL query: {value}"
            ))),
        }
    }
}

impl SqlSchema {
    pub fn get_column_names(&self) -> Vec<String> {
        self.columns
            .iter()
            .map(|col| col.col_name.as_str().to_owned())
            .collect()
    }
}

pub struct Reader<S: AsRef<[u8]>> {
    buf: S,
    pub header: DbHeader,
    tables: OnceCell<HashMap<String, Option<SqlSchema>>>,
}

impl Reader<Mmap> {
    pub fn open_mmap<P: AsRef<Path>>(database: P) -> error::Result<Reader<Mmap>> {
        let file_read = File::open(database)?;
        let mmap = unsafe { MmapOptions::new().map(&file_read) }?;
        Reader::from_source(mmap)
    }
}

impl<S: AsRef<[u8]> + Sync> Reader<S> {
    fn from_source(buf: S) -> error::Result<Reader<S>> {
        let mut input = buf.as_ref();
        let header = db_header::<ContextError>(&mut input)?;

        let reader = Reader {
            buf,
            header,
            tables: OnceCell::default(),
        };

        Ok(reader)
    }

    fn get_page(&self, pageno: u32) -> error::Result<Page<'_>> {
        use crate::parser::page_with_overflow;

        let page_size = self.header.page_size.real_size();

        let pageno_usize = (pageno as usize).saturating_sub(1);

        let page_bytes =
            &self.buf.as_ref()[page_size * pageno_usize..page_size * (pageno_usize + 1)];

        let page_start_offset = if pageno <= 1 { HEADER_SIZE } else { 0 };
        let input_bytes = if pageno <= 1 {
            &page_bytes[HEADER_SIZE..]
        } else {
            page_bytes
        };

        let mut input = input_bytes;
        let page = page_with_overflow::<ContextError>(&mut input, &self.header, page_start_offset)?;

        Ok(page)
    }

    fn get_overflow_page(&self, pageno: u32) -> Result<model::OverflowPage<'_>, SQLiteError> {
        let page_size = self.header.page_size.real_size();
        let usable_size = self.header.usable_page_size();

        let pageno_usize = (pageno as usize).saturating_sub(1);

        let page_start = page_size * pageno_usize;
        let page_bytes = &self.buf.as_ref()[page_start..page_start + usable_size];

        let mut input = page_bytes;
        let overflow = overflow_page::<ContextError>(&mut input)?;

        Ok(overflow)
    }

    fn read_overflow_chain(&self, first_page: u32, total_size: usize) -> error::Result<Vec<u8>> {
        let mut buffer = Vec::with_capacity(total_size);
        let mut next_page = Some(first_page);

        while buffer.len() < total_size && next_page.is_some() {
            let (next_page_no, payload) = self.get_overflow_page(next_page.unwrap())?;
            let to_read = (total_size - buffer.len()).min(payload.len());
            buffer.extend_from_slice(&payload[..to_read]);
            next_page = next_page_no;
        }

        Ok(buffer)
    }

    #[inline(always)]
    pub fn reconstruct_full_payload(
        &self,
        cell: &model::LeafTableCell<'_>,
    ) -> error::Result<Vec<u8>> {
        if cell.overflow_page_no.is_none() {
            return Err(SQLiteError::Other(
                "Cell has no overflow - use existing payload".into(),
            ));
        }

        let overflow_page_no = cell.overflow_page_no.unwrap();
        let overflow_data =
            self.read_overflow_chain(overflow_page_no, cell.payload_size as usize)?;

        Ok(overflow_data)
    }

    pub fn get_tables_map(&self) -> error::Result<&HashMap<String, Option<SqlSchema>>> {
        self.tables.get_or_try_init(|| {
            let root = self.get_page(0)?;
            let mut new_tables = HashMap::default();

            match root {
                Page::LeafTable(ref p) => {
                    self.extract_tables_from_leaf(&p.cells, &mut new_tables);
                }
                Page::InteriorTable(ref p) => {
                    // for interior page, only read the sqlite_master leaf pages
                    // never recursively traverse, those would be user table pages
                    let _ = self.traverse_interior_children(&p.header, &p.cells, |reader, page| {
                        if let Page::LeafTable(ref leaf) = page {
                            reader.extract_tables_from_leaf(&leaf.cells, &mut new_tables);
                        }
                        Ok::<Option<()>, SQLiteError>(None)
                    });
                }
                _ => {}
            }
            Ok(new_tables)
        })
    }

    #[inline(always)]
    fn extract_tables_from_column_values<'a>(
        &self,
        column_values: &[Option<model::Payload<'a>>],
        tables: &mut HashMap<String, Option<SqlSchema>>,
    ) {
        if column_values.len() == SQLITE_MASTER_TABLE_SIZE {
            if let Some(model::Payload::Text(ref type_text)) =
                column_values[SqliteMasterTable::Type as usize]
            {
                let type_str = type_text.decode(self.header.db_text_encoding);
                if type_str == "table" {
                    if let Some(model::Payload::Text(ref name_text)) =
                        column_values[SqliteMasterTable::Name as usize]
                    {
                        let table_name =
                            name_text.decode(self.header.db_text_encoding).into_owned();

                        let table_schema = match column_values[SqliteMasterTable::Sql as usize] {
                            Some(model::Payload::Text(ref sql_text)) => SqlSchema::try_from(
                                sql_text.decode(self.header.db_text_encoding).into_owned(),
                            )
                            .ok(),
                            _ => None,
                        };

                        tables.insert(table_name.clone(), table_schema);
                    }
                }
            }
        }
    }

    #[inline(always)]
    fn extract_tables_from_leaf<'a>(
        &self,
        cells: &[model::LeafTableCell<'a>],
        tables: &mut HashMap<String, Option<SqlSchema>>,
    ) {
        for cell in cells {
            let column_values = get_table_cell_values(cell);
            self.extract_tables_from_column_values(column_values, tables);
        }
    }

    pub fn stream_table_rows_sequential<F>(
        &self,
        table_name: &str,
        mut callback: F,
    ) -> error::Result<()>
    where
        F: FnMut(&model::LeafTableCell<'_>, &Vec<Option<model::Payload<'_>>>) -> error::Result<()>,
    {
        let root = self.get_page(0)?;

        let table_root_pageno = match root {
            Page::LeafTable(ref p) => self.find_table_root_in_leaf(&p.cells, table_name)?,
            Page::InteriorTable(ref p) => self.find_table_root_in_interior(p, table_name)?,
            _ => None,
        };

        let table_root_pageno = table_root_pageno
            .ok_or_else(|| SQLiteError::Other(format!("Table '{}' not found", table_name)))?;

        let mut cached_types = HashMap::default();

        self.stream_table_rows_from_page(table_root_pageno, &mut callback, &mut cached_types)
    }

    #[inline(always)]
    fn find_table_root_in_column_values<'a>(
        &self,
        column_values: &[Option<model::Payload<'a>>],
        table_name: &str,
    ) -> error::Result<Option<u32>> {
        if column_values.len() == SQLITE_MASTER_TABLE_SIZE {
            if let Some(model::Payload::Text(ref type_text)) =
                column_values[SqliteMasterTable::Type as usize]
            {
                let type_str = type_text.decode(self.header.db_text_encoding);
                if type_str == "table" {
                    if let Some(model::Payload::Text(ref name_text)) =
                        column_values[SqliteMasterTable::Name as usize]
                    {
                        let name = name_text.decode(self.header.db_text_encoding);
                        if name == table_name {
                            if let Some(ref pageno_payload) =
                                column_values[SqliteMasterTable::RootPage as usize]
                            {
                                if let Some(pageno) = pageno_payload.as_u32() {
                                    return Ok(Some(pageno));
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(None)
    }

    #[inline(always)]
    fn find_table_root_in_leaf<'a>(
        &self,
        cells: &[model::LeafTableCell<'a>],
        table_name: &str,
    ) -> error::Result<Option<u32>> {
        for cell in cells {
            let column_values = get_table_cell_values(cell);
            if let Some(pageno) =
                self.find_table_root_in_column_values(column_values, table_name)?
            {
                return Ok(Some(pageno));
            }
        }
        Ok(None)
    }

    fn traverse_interior_children<F, R>(
        &self,
        header: &model::InteriorPageHeader,
        cells: &[model::InteriorCell],
        mut visitor: F,
    ) -> error::Result<Option<R>>
    where
        F: FnMut(&Self, &Page) -> error::Result<Option<R>>,
    {
        for cell in cells {
            let page = self.get_page(cell.left_child_page_no)?;
            if let Some(result) = visitor(self, &page)? {
                return Ok(Some(result));
            }
        }

        if header.rightmost_pointer > 0 {
            let page = self.get_page(header.rightmost_pointer)?;
            if let Some(result) = visitor(self, &page)? {
                return Ok(Some(result));
            }
        }

        Ok(None)
    }

    fn find_table_root_in_interior(
        &self,
        interior: &model::InteriorTablePage,
        table_name: &str,
    ) -> error::Result<Option<u32>> {
        self.traverse_interior_children(
            &interior.header,
            &interior.cells,
            |reader, page| match page {
                Page::LeafTable(ref p) => reader.find_table_root_in_leaf(&p.cells, table_name),
                Page::InteriorTable(ref p) => reader.find_table_root_in_interior(p, table_name),
                _ => Ok(None),
            },
        )
    }

    fn stream_table_rows_from_page<F>(
        &self,
        pageno: u32,
        callback: &mut F,
        cached_types: &mut HashMap<u64, std::sync::Arc<Vec<model::SerialType>>>,
    ) -> error::Result<()>
    where
        F: FnMut(&model::LeafTableCell<'_>, &Vec<Option<model::Payload<'_>>>) -> error::Result<()>,
    {
        use crate::parser::stream_page_cells;

        let page_size = self.header.page_size.real_size();
        let pageno_usize = (pageno as usize).saturating_sub(1);

        let page_bytes =
            &self.buf.as_ref()[page_size * pageno_usize..page_size * (pageno_usize + 1)];

        let page_start_offset = if pageno <= 1 { HEADER_SIZE } else { 0 };
        let input_bytes = if pageno <= 1 {
            &page_bytes[HEADER_SIZE..]
        } else {
            page_bytes
        };
        let mut column_values = Vec::new();

        stream_page_cells(
            input_bytes,
            &self.header,
            page_start_offset,
            &mut column_values,
            &mut *cached_types,
            |cell_type, cache| {
                match cell_type {
                    parser::CellType::LeafTable(cell, column_values) => {
                        callback(&cell, column_values)
                    }
                    // parser::CellType::LeafIndex => {
                    //     Ok(())
                    // }
                    parser::CellType::InteriorTable(pageno) => {
                        self.stream_table_rows_from_page(pageno, callback, cache)
                    }
                    parser::CellType::InteriorTableRightmost(pageno) => {
                        self.stream_table_rows_from_page(pageno, callback, cache)
                    }
                }
            },
        )
    }
}
