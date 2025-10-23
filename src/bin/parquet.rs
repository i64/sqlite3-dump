use sqlite3_dump::parquet_writer::export_table_to_parquet;
use sqlite3_dump::{HashMap, Reader, SqlSchema};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

// this example is an ai slop

#[derive(argh::FromArgs)]
/// SQLite to Parquet exporter
struct Args {
    /// path to SQLite database file
    #[argh(positional)]
    database: PathBuf,

    /// name of table to export (optional, if omitted exports all tables)
    #[argh(positional)]
    table: Option<String>,

    /// output (for db dir, for table file) for Parquet files
    #[argh(option, short = 'o')]
    output: Option<String>,

    /// number of rows per batch (default: 10000)
    #[argh(option, short = 'b', default = "10000")]
    batch_size: usize,
}

fn main() {
    let args: Args = argh::from_env();
    
    let reader = open_database(&args.database);
    
    
    let db_name = get_db_name(&args.database);
    
    if let Some(table_name) = &args.table {
        let output_path = args.output.clone().unwrap_or(format!("{table_name}.parquet"));
        print_header(&args,&output_path , &reader);
        export_single_table(&reader, table_name, &output_path, args.batch_size);
    } else {
        let output_dir = prepare_output_dir(&args.output);
        export_all_tables(&reader, &output_dir, args.batch_size, db_name);
    }
}


fn prepare_output_dir(output_dir_opt: &Option<String>) -> String {
    let output_dir = output_dir_opt.clone().unwrap_or_else(|| ".".to_string());

    if !output_dir.is_empty() && output_dir != "." {
        if let Err(e) = fs::create_dir_all(&output_dir) {
            eprintln!(
                "Error: Failed to create output directory '{}': {:?}",
                output_dir, e
            );
            std::process::exit(1);
        }
    }
    output_dir
}

fn open_database(database: &PathBuf) -> Reader<memmap2::Mmap> {
    let start = Instant::now();
    let reader = match Reader::open_mmap(database) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error: Failed to open database: {:?}", e);
            std::process::exit(1);
        }
    };

    println!("Database opened in {:?}", start.elapsed());
    println!();
    reader
}

fn print_header(args: &Args, output_dir: &str, reader: &Reader<impl AsRef<[u8]> + Sync>) {
    println!("SQLite to Parquet Exporter");
    println!("==========================");
    println!("Database: {}", args.database.display());
    println!("Page size: {} bytes", reader.header.page_size.real_size());
    println!("Text encoding: {:?}", reader.header.db_text_encoding);
    println!("Output: {}", output_dir);
    println!("Batch size: {}", args.batch_size);
    println!();
}

fn get_db_name(database: &Path) -> &str {
    database
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("database")
}

fn create_db_dir(output_dir: &str, db_name: &str) -> String {
    let db_dir = if output_dir == "." {
        db_name.to_string()
    } else {
        format!("{}/{}", output_dir.trim_end_matches('/'), db_name)
    };

    if let Err(e) = fs::create_dir_all(&db_dir) {
        eprintln!(
            "Error: Failed to create database directory '{}': {:?}",
            db_dir, e
        );
        std::process::exit(1);
    }
    db_dir
}

fn export_single_table(
    reader: &Reader<impl AsRef<[u8]> + Sync>,
    table_name: &str,
    output_file: &str,
    batch_size: usize,
) {
    println!("Exporting table: {}", table_name);
    println!("Output file: {}", output_file);
    println!();

    let export_start = Instant::now();

    match export_table_to_parquet(reader, table_name, output_file, batch_size) {
        Ok(row_count) => print_single_table_summary(
            table_name,
            row_count,
            &export_start,
            output_file,
        ),
        Err(e) => {
            eprintln!();
            eprintln!("Error: Export failed for table '{}': {:?}", table_name, e);
            std::process::exit(1);
        }
    }
}

fn print_single_table_summary(
    table_name: &str,
    row_count: usize,
    export_start: &Instant,
    output_file: &str,
) {
    let duration = export_start.elapsed();
    println!();
    println!("Export completed successfully!");
    println!("==========================");
    println!("Table: {}", table_name);
    println!("Rows exported: {}", row_count);
    println!("Time taken: {:.2?}", duration);
    println!("Output file: {}", &output_file);

    if duration.as_secs() > 0 {
        let rows_per_sec = row_count as f64 / duration.as_secs_f64();
        println!("Throughput: {:.0} rows/sec", rows_per_sec);
    }

    if let Ok(metadata) = std::fs::metadata(output_file) {
        let size_mb = metadata.len() as f64 / (1024.0 * 1024.0);
        println!("File size: {:.2} MB", size_mb);
    }
}

fn export_all_tables(
    reader: &Reader<impl AsRef<[u8]> + Sync>,
    output_dir: &str,
    batch_size: usize,
    db_name: &str,
) {
    let tables = match reader.get_tables_map() {
        Ok(t) => t,
        Err(_) => {
            eprintln!("Error: Failed to get tables from database.");
            std::process::exit(1);
        }
    };

    if tables.is_empty() {
        println!("No tables found in database.");
        return;
    }

    println!("Found {} tables:", tables.len());
    for table_name in tables.keys() {
        println!("  - {}", table_name);
    }
    println!();

    let db_dir = create_db_dir(output_dir, db_name);
    process_all_tables(reader, tables, &db_dir, batch_size);
}

fn process_all_tables(
    reader: &Reader<impl AsRef<[u8]> + Sync>,
    tables: &HashMap<String, Option<SqlSchema>>,
    db_dir: &str,
    batch_size: usize,
) {
    let total_start = Instant::now();
    let mut total_rows = 0;
    let mut successful_exports = 0;

    for table_name in tables.keys() {
        println!("Exporting table: {}", table_name);
        let output_file = format!("{}/{}.parquet", db_dir, table_name);

        match export_table(reader, table_name, &output_file, batch_size) {
            Ok(row_count) => {
                total_rows += row_count;
                successful_exports += 1;
            }
            Err(e) => eprintln!("  ✗ Failed to export '{}': {:?}", table_name, e),
        }
    }

    print_export_summary(successful_exports, total_rows, total_start.elapsed());
}

fn export_table(
    reader: &Reader<impl AsRef<[u8]> + Sync>,
    table_name: &str,
    output_file: &str,
    batch_size: usize,
) -> sqlite3_dump::error::Result<usize> {
    let export_start = Instant::now();
    let result = export_table_to_parquet(reader, table_name, output_file, batch_size);
    if let Ok(row_count) = &result {
        let duration = export_start.elapsed();

        let rows_per_sec = if duration.as_secs() > 0 {
            *row_count as f64 / duration.as_secs_f64()
        } else {
            0.0
        };

        let size_mb = std::fs::metadata(output_file)
            .map(|m| m.len() as f64 / (1024.0 * 1024.0))
            .unwrap_or(0.0);

        println!(
            "  ✓ {}: {} rows ({:.2} MB) - {:.2?} ({:.0} rows/sec)",
            table_name, row_count, size_mb, duration, rows_per_sec
        );
    }
    result
}

fn print_export_summary(successful_exports: usize, total_rows: usize, total_duration: std::time::Duration) {
    println!();
    println!("Export Summary");
    println!("==========================");
    println!("Tables processed: {}", successful_exports);
    println!("Total rows exported: {}", total_rows);
    println!("Total time taken: {:.2?}", total_duration);

    if total_duration.as_secs() > 0 {
        let overall_throughput = total_rows as f64 / total_duration.as_secs_f64();
        println!("Overall throughput: {:.0} rows/sec", overall_throughput);
    }

    if successful_exports == 0 {
        eprintln!("Error: No tables were exported successfully.");
        std::process::exit(1);
    }
}
