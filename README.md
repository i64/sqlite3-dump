# sqlite3-dump

Fast SQLite3 database file parser and dumper written in Rust.

## Features
- export tables to CSV and Parquet formats
- parses SQLite3 database files directly without SQLite library
- zero-copy
- no unsafe

## warning
it's not fuzzed yet so use it with your own risk on production and on untrusted enviroment

## Usage
### CSV Export
```bash
RUSTFLAGS="-C target-cpu=native" cargo run --bin csv --release -- database.db -t table_name output.csv
RUSTFLAGS="-C target-cpu=native" cargo run --bin csv --release -- database.db -t table_name > output.csv
```

### Parquet Export
```bash
RUSTFLAGS="-C target-cpu=native" cargo run --bin parquet --release -- database.db table_name -o output.parquet
```

## quick comprasion
generated database of [100m rows](https://github.com/avinassh/fast-sqlite3-inserts).

```
$ du -sh basic_batched.db
1.5G	basic_batched.db
```

`sqlite3` -- 23 seconds:
```
time sqlite3 -header -csv ./basic_batched.db "SELECT * FROM user;" > test.csv
real	0m23.757s
user	0m22.033s
sys	0m1.691s
```

`sqlite3-dump csv` -- 6.4 seconds
```
$ time target/release/csv basic_batched.db -t user > test2.csv
real	0m6.403s
user	0m5.480s
sys	0m0.920s
```

`sqlite3-dump parquet` -- 6.4 seconds
```
$ time target/release/parquet basic_batched.db user  -o user.parquet
Database opened in 58.439µs

SQLite to Parquet Exporter
==========================
Database: basic_batched.db
Page size: 4096 bytes
Text encoding: Utf8
Output: user.parquet
Batch size: 10000

Exporting table: user
Output file: user.parquet


Export completed successfully!
==========================
Table: user
Rows exported: 100000000
Time taken: 6.53s
Output file: user.parquet
Throughput: 15319197 rows/sec
File size: 310.01 MB

real	0m6.577s
user	0m11.006s
sys	0m0.757s
```
## References
- [https://github.com/sqlite/sqlite/](https://github.com/sqlite/sqlite/tree/master)
- [https://github.com/andrusha/sqlite-parser-nom/](https://github.com/andrusha/sqlite-parser-nom/)
## License

Apache-2.0