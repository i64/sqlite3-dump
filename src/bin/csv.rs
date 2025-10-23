use memmap2::Mmap;
use sqlite3_dump::error::SQLiteError;
use sqlite3_dump::model::LeafTableCell;
use sqlite3_dump::{model, HashMap, Reader, SqlSchema};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

// this example is an ai slop

/// SQLite to CSV exporter
#[derive(argh::FromArgs)]
/// Export SQLite tables to CSV or list available tables
struct Args {
    /// path to SQLite database file
    #[argh(positional)]
    database: PathBuf,

    /// table name to export (omit to list all tables)
    #[argh(option, short = 't')]
    table: Option<String>,

    /// output CSV file path (optional, defaults to stdout)
    #[argh(option, short = 'o')]
    output: Option<PathBuf>,
}

fn main() {
    let args: Args = argh::from_env();
    let reader = open_database(&args.database);
    let tables = reader.get_tables_map().expect("Failed to get tables");

    match args.table {
        None => list_tables(tables),
        Some(ref table_name) => dump_table(&reader, tables, table_name, args.output.as_ref()),
    }
}

fn open_database(path: &PathBuf) -> Reader<memmap2::Mmap> {
    Reader::open_mmap(path).unwrap_or_else(|_| {
        eprintln!("Error: Failed to open database '{}'", path.display());
        std::process::exit(1);
    })
}

fn list_tables(tables: &HashMap<String, Option<SqlSchema>>) {
    println!("Tables in database:");
    for table_name in tables.keys() {
        println!("  - {}", table_name);
    }
}

fn dump_table(
    reader: &Reader<Mmap>,
    tables: &HashMap<String, Option<SqlSchema>>,
    table_name: &str,
    output_path: Option<&PathBuf>,
) {
    if !tables.contains_key(table_name) {
        eprintln!("Error: table '{}' not found", table_name);
        std::process::exit(1);
    }

    let mut output = create_output_writer(output_path);
    stream_table_to_csv(reader, table_name, &mut output);
    output.flush().expect("Failed to flush output");
}

fn create_output_writer(output_path: Option<&PathBuf>) -> BufWriter<Box<dyn Write>> {
    if let Some(path) = output_path {
        let file = File::create(path).unwrap_or_else(|_| {
            eprintln!("Error: Failed to create output file '{}'", path.display());
            std::process::exit(1);
        });
        BufWriter::with_capacity(256 * 1024, Box::new(file))
    } else {
        BufWriter::with_capacity(256 * 1024, Box::new(std::io::stdout()))
    }
}

fn stream_table_to_csv(reader: &Reader<Mmap>, table_name: &str, output: &mut BufWriter<Box<dyn Write>>) {
    reader
        .stream_table_rows_sequential(table_name, |row, column_values| {
            write_row_to_csv(reader, row, column_values, output).map_err(SQLiteError::IOError)
        })
        .expect("Failed to stream table");
}

fn write_row_to_csv(
    reader: &Reader<Mmap>,
    row: &LeafTableCell<'_>,
    column_values: &[Option<model::Payload>],
    output: &mut BufWriter<Box<dyn Write>>,
) -> Result<(), std::io::Error> {
    let mut itoa_buf = itoa::Buffer::new();
    output.write_all(itoa_buf.format(row.rowid).as_bytes())?;

    let skip_first = column_values.first().is_some_and(|v| v.is_none());
    let values_to_output = if skip_first {
        &column_values[1..]
    } else {
        column_values
    };

    let overflow_data = if row.overflow_page_no.is_some()
        && values_to_output.iter().any(|v| v.is_none())
    {
        reader.reconstruct_full_payload(row).ok()
    } else {
        None
    };

    for value in values_to_output.iter() {
        output.write_all(b",")?;
        write_value_to_csv(reader, value, &overflow_data, output)?;
    }

    output.write_all(b"\n")?;
    Ok(())
}

fn write_value_to_csv(
    reader: &Reader<Mmap>,
    value: &Option<model::Payload>,
    overflow_data: &Option<Vec<u8>>,
    output: &mut BufWriter<Box<dyn Write>>,
) -> Result<(), std::io::Error> {
    const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

    match value {
        None if overflow_data.is_some() => {
            for &byte in overflow_data.as_ref().unwrap() {
                output.write_all(&[HEX_CHARS[(byte >> 4) as usize]])?;
                output.write_all(&[HEX_CHARS[(byte & 0x0f) as usize]])?;
            }
        }
        None => {}
        Some(model::Payload::I64(v)) => {
            let mut itoa_buf = itoa::Buffer::new();
            output.write_all(itoa_buf.format(*v).as_bytes())?;
        }
        Some(model::Payload::F64(v)) => {
            let mut ryu_buf = ryu::Buffer::new();
            output.write_all(ryu_buf.format(*v).as_bytes())?;
        }
        Some(model::Payload::Text(t)) => {
            let text = t.decode(reader.header.db_text_encoding);
            write_csv_text(&text, output)?;
        }
        Some(model::Payload::Blob(b)) => {
            for byte in b.iter() {
                output.write_all(&[HEX_CHARS[(byte >> 4) as usize]])?;
                output.write_all(&[HEX_CHARS[(byte & 0x0f) as usize]])?;
            }
        }
    }

    Ok(())
}

fn write_csv_text(text: &str, output: &mut BufWriter<Box<dyn Write>>) -> Result<(), std::io::Error> {
    let bytes = text.as_bytes();
    let needs_quoting = bytes.iter().any(|&b| matches!(b, b',' | b'"' | b'\n' | b'\r'));

    if needs_quoting {
        output.write_all(b"\"")?;
        for &b in bytes {
            match b {
                b'"' => output.write_all(b"\"\"")?,
                b'\n' => output.write_all(b"\\n")?,
                b'\r' => output.write_all(b"\\r")?,
                b'\t' => output.write_all(b"\\t")?,
                b'\\' => output.write_all(b"\\\\")?,
                _ => output.write_all(&[b])?,
            }
        }
        output.write_all(b"\"")?;
    } else {
        output.write_all(bytes)?;
    }

    Ok(())
}
