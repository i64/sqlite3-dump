use crate::error::SQLiteError;
use crate::model::{LeafTableCell, Payload, SerialType, TextEncoding};
use crate::Reader;
use arrow::array::{ArrayRef, BinaryBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use arrow_schema::Field;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::fs::File;

use std::path::Path;
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::Arc;

enum ColumnBuilder {
    Int64(Int64Builder),
    Float64(Float64Builder),
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
}

pub struct ParquetContext {
    schema: Arc<Schema>,
    sender: SyncSender<RecordBatch>,
    writer_handle: std::thread::JoinHandle<Result<(), SQLiteError>>,
    rowid_builder: Int64Builder,
    column_builders: Vec<ColumnBuilder>,
    columns: Vec<ArrayRef>,
    batch_size: usize,
}

impl ColumnBuilder {
    fn new(data_type: &DataType, capacity: usize) -> Self {
        match data_type {
            DataType::Int64 => ColumnBuilder::Int64(Int64Builder::with_capacity(capacity)),
            DataType::Float64 => ColumnBuilder::Float64(Float64Builder::with_capacity(capacity)),
            DataType::Utf8 => {
                ColumnBuilder::Utf8(StringBuilder::with_capacity(capacity, capacity * 32))
            }
            DataType::Binary => {
                ColumnBuilder::Binary(BinaryBuilder::with_capacity(capacity, capacity * 64))
            }
            _ => ColumnBuilder::Binary(BinaryBuilder::with_capacity(capacity, capacity * 64)),
        }
    }

    fn append_null(&mut self) {
        match self {
            ColumnBuilder::Int64(b) => b.append_null(),
            ColumnBuilder::Float64(b) => b.append_null(),
            ColumnBuilder::Utf8(b) => b.append_null(),
            ColumnBuilder::Binary(b) => b.append_null(),
        }
    }

    fn finish_reset(&mut self, capacity: usize) -> ArrayRef {
        match self {
            ColumnBuilder::Int64(b) => {
                let array = Arc::new(b.finish());
                *b = Int64Builder::with_capacity(capacity);
                array
            }
            ColumnBuilder::Float64(b) => {
                let array = Arc::new(b.finish());
                *b = Float64Builder::with_capacity(capacity);
                array
            }
            ColumnBuilder::Utf8(b) => {
                let array = Arc::new(b.finish());
                *b = StringBuilder::with_capacity(capacity, capacity * 32);
                array
            }
            ColumnBuilder::Binary(b) => {
                let array = Arc::new(b.finish());
                *b = BinaryBuilder::with_capacity(capacity, capacity * 64);
                array
            }
        }
    }
}

pub(crate) fn build_arrow_schema_from_row(
    column_types: &[SerialType],
    column_names: Option<&[String]>,
) -> Arc<Schema> {
    let mut fields = Vec::new();

    fields.push(Field::new("rowid", DataType::Int64, false));

    let skip_first = column_types
        .first()
        .is_some_and(|t| matches!(t, SerialType::Null));
    let columns_to_process = if skip_first {
        &column_types[1..]
    } else {
        column_types
    };

    for (idx, serial_type) in columns_to_process.iter().enumerate() {
        let (data_type, nullable) = serial_type_to_arrow(serial_type);
        let column_name = if let Some(names) = column_names {
            names
                .get(idx)
                .cloned()
                .unwrap_or_else(|| format!("col_{}", idx))
        } else {
            format!("col_{}", idx)
        };

        fields.push(Field::new(column_name, data_type, nullable));
    }

    Arc::new(Schema::new(fields))
}

fn serial_type_to_arrow(serial_type: &SerialType) -> (DataType, bool) {
    match serial_type {
        SerialType::Null => (DataType::Binary, true),
        SerialType::I8
        | SerialType::I16
        | SerialType::I24
        | SerialType::I32
        | SerialType::I48
        | SerialType::I64
        | SerialType::Const0
        | SerialType::Const1 => (DataType::Int64, true),
        SerialType::F64 => (DataType::Float64, true),
        SerialType::Text(_) => (DataType::Utf8, true),
        SerialType::Blob(_) => (DataType::Binary, true),
        SerialType::Reserved => (DataType::Binary, true),
    }
}

pub fn initialize_context<P: AsRef<Path>>(
    cell: &LeafTableCell,
    output_path: P,
    batch_size: usize,
    column_names: Option<&[String]>,
) -> Result<ParquetContext, SQLiteError> {
    let column_types = cell.payload.column_types.clone();
    let arrow_schema = build_arrow_schema_from_row(&column_types, column_names);

    let (tx, rx) = std::sync::mpsc::sync_channel::<RecordBatch>(2);

    let output_path = output_path.as_ref().to_path_buf();
    let schema_clone = arrow_schema.clone();
    let writer_handle = std::thread::spawn(move || -> Result<(), SQLiteError> {
        write_batches_to_parquet(rx, &output_path, schema_clone)
    });

    let rowid_builder = Int64Builder::with_capacity(batch_size);
    let column_builders = arrow_schema
        .fields()
        .iter()
        .skip(1)
        .map(|f| ColumnBuilder::new(f.data_type(), batch_size))
        .collect();
    let columns = Vec::with_capacity(arrow_schema.fields().len());

    Ok(ParquetContext {
        schema: arrow_schema,
        sender: tx,
        writer_handle,
        rowid_builder,
        column_builders,
        columns,
        batch_size,
    })
}

fn process_row_values(
    column_values: &[Option<Payload>],
    column_builders: &mut [ColumnBuilder],
    text_encoding: TextEncoding,
    full_payload: Option<&Vec<u8>>,
) {
    let values_to_write = if column_values.first().is_some_and(|v| v.is_none()) {
        &column_values[1..]
    } else {
        column_values
    };

    for (value, column_builder) in values_to_write.iter().zip(column_builders.iter_mut()) {
        let Some(payload) = value else {
            if let ColumnBuilder::Binary(builder) = column_builder {
                if let Some(data) = full_payload {
                    builder.append_value(data);
                    continue;
                }
            }
            column_builder.append_null();
            continue;
        };

        match column_builder {
            ColumnBuilder::Int64(builder) => match payload {
                Payload::I64(v) => builder.append_value(*v),
                Payload::F64(v) => builder.append_value(*v as i64),
                _ => builder.append_null(),
            },
            ColumnBuilder::Float64(builder) => match payload {
                Payload::F64(v) => builder.append_value(*v),
                Payload::I64(v) => builder.append_value(*v as f64),
                _ => builder.append_null(),
            },
            ColumnBuilder::Utf8(builder) => match payload {
                Payload::Text(t) => {
                    let text = t.decode(text_encoding);
                    builder.append_value(text);
                }
                _ => builder.append_null(),
            },
            ColumnBuilder::Binary(builder) => match payload {
                Payload::Blob(b) => builder.append_value(b),
                _ => builder.append_null(),
            },
        }
    }

    for column_builder in column_builders.iter_mut().skip(values_to_write.len()) {
        column_builder.append_null();
    }
}

fn flush_rows(context: &mut ParquetContext, last: bool) -> Result<(), SQLiteError> {
    context.columns.clear();

    let rowid_array = Arc::new(context.rowid_builder.finish());
    if !last {
        context.rowid_builder = Int64Builder::with_capacity(context.batch_size);
    }
    context.columns.push(rowid_array as ArrayRef);

    for builder in context.column_builders.iter_mut() {
        context
            .columns
            .push(builder.finish_reset(context.batch_size));
    }

    let record_batch = RecordBatch::try_new(context.schema.clone(), context.columns.clone())
        .map_err(|e| SQLiteError::Other(format!("Failed to create record batch: {}", e)))?;

    context
        .sender
        .send(record_batch)
        .map_err(|_| SQLiteError::Other("Writer thread died".to_string()))?;

    Ok(())
}

pub fn export_table_to_parquet<P: AsRef<Path>>(
    reader: &Reader<impl AsRef<[u8]> + Sync>,
    table_name: &str,
    output_path: P,
    batch_size: usize,
) -> Result<usize, SQLiteError> {
    let text_encoding = reader.header.db_text_encoding;
    let mut total_rows = 0;

    let column_names = reader
        .get_tables_map()?
        .get(table_name)
        .ok_or_else(|| SQLiteError::TableNotFound(table_name.to_owned()))?
        .as_ref()
        .map(|schema| schema.get_column_names());

    let mut context: Option<ParquetContext> = None;
    let mut rows_buffered = 0;

    reader.stream_table_rows_sequential(table_name, |cell, column_values| {
        if context.is_none() {
            context = Some(initialize_context(
                cell,
                &output_path,
                batch_size,
                column_names.as_deref(),
            )?);
        }
        let context = context.as_mut().unwrap();
        context.rowid_builder.append_value(cell.rowid as i64);

        let full_payload = if cell.overflow_page_no.is_some() {
            reader.reconstruct_full_payload(cell).ok()
        } else {
            None
        };

        process_row_values(
            column_values,
            context.column_builders.as_mut_slice(),
            text_encoding,
            full_payload.as_ref(),
        );

        rows_buffered += 1;
        total_rows += 1;

        if rows_buffered >= batch_size {
            flush_rows(context, false)?;
            rows_buffered = 0;
        }

        Ok(())
    })?;

    let mut context = context.unwrap();

    if rows_buffered > 0 {
        flush_rows(&mut context, true)?;
    }

    drop(context.sender);

    context
        .writer_handle
        .join()
        .map_err(|_| SQLiteError::Other("Writer thread panicked".to_string()))??;

    Ok(total_rows)
}

fn write_batches_to_parquet<P: AsRef<Path>>(
    receiver: Receiver<RecordBatch>,
    output_path: P,
    schema: Arc<Schema>,
) -> Result<(), SQLiteError> {
    let file = File::create(output_path)
        .map_err(|e| SQLiteError::Other(format!("Failed to create file: {}", e)))?;

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();

    let mut writer = ArrowWriter::try_new(file, schema, Some(props))
        .map_err(|e| SQLiteError::Other(format!("Failed to create ArrowWriter: {}", e)))?;

    for batch in receiver {
        writer
            .write(&batch)
            .map_err(|e| SQLiteError::Other(format!("Failed to write batch: {}", e)))?;
    }

    writer
        .close()
        .map_err(|e| SQLiteError::Other(format!("Failed to close writer: {}", e)))?;

    Ok(())
}
