#[macro_use]
extern crate log;

use clap::Parser;
use humantime::format_duration;
use itertools::Itertools;
use polars::prelude::{col, lit, when, CsvWriter, LazyCsvReader, LazyFileListReader, SerWriter};
use rayon::prelude::*;
use std::error::Error;
use std::fs;
use std::io::{BufRead, Write};
use std::path;
use std::time::Instant;

#[derive(Parser, PartialEq, Debug)]
#[clap(author, version, about, long_about = None)]
struct Options {
    #[clap(short = 'i', long, required = true)]
    input: path::PathBuf,

    #[clap(short = 'c', long, required = true)]
    column: String,

    #[clap(short = 'o', long, required = true)]
    output: path::PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let start = Instant::now();
    env_logger::init();

    let options = Options::parse();
    debug!("{:?}", options);

    let unit_separator: String = format!("{}", char::from_u32(0x0000001F).unwrap()).to_string();
    let comma: String = format!("{}", char::from_u32(0x0000002C).unwrap()).to_string();
    let pipe: String = format!("{}", char::from_u32(0x0000007C).unwrap()).to_string();

    let mut df = LazyCsvReader::new(options.input.clone())
        .with_separator(b'\t')
        .with_infer_schema_length(Some(0))
        .with_ignore_errors(true)
        .with_truncate_ragged_lines(true)
        .with_has_header(true)
        .finish()
        .unwrap()
        .with_columns([when(
            col(options.column.clone()).str().contains_literal(lit(unit_separator.clone())).or(col(options.column.clone()).str().contains_literal(lit(comma.clone()))),
        )
        .then(col(options.column.clone()).str().replace_all(lit(unit_separator.clone()), lit(pipe.clone()), true))
        .otherwise(col(options.column.clone()))
        .alias(options.column.clone())])
        .collect()
        .unwrap();

    let parent_dir = options.output.parent().unwrap();
    debug!("writing output to: {:?}", parent_dir);
    fs::create_dir_all(parent_dir).expect("Could not create parent directory");

    let mut output_file = fs::File::create(options.output.as_path()).unwrap();
    CsvWriter::new(&mut output_file).with_separator(b'\t').finish(&mut df).unwrap();

    info!("Duration: {}", format_duration(start.elapsed()).to_string());
    Ok(())
}
