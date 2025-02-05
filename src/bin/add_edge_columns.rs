#[macro_use]
extern crate log;

use clap::Parser;
use humantime::format_duration;
use itertools::Itertools;
use polars::prelude::*;
use std::error;
use std::fs;
use std::io;
use std::io::prelude::*;
use std::path;
use std::time::Instant;

#[derive(Parser, PartialEq, Debug)]
#[clap(author, version, about, long_about = None)]
struct Options {
    #[clap(short = 'i', long, required = true)]
    input: path::PathBuf,

    #[clap(short = 'o', long, required = true)]
    output: path::PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn error::Error>> {
    let start = Instant::now();
    env_logger::init();

    let options = Options::parse();
    debug!("{:?}", options);

    let input = options.input;
    let output = options.output;

    let parse_options = CsvParseOptions::default().with_separator(b'\t');

    let mut df = CsvReadOptions::default()
        .with_parse_options(parse_options.clone())
        .with_has_header(true)
        .with_ignore_errors(true)
        .try_into_reader_with_file_path(Some(input.clone()))
        .unwrap()
        .finish()
        .unwrap();

    let capacity = df.height();

    let expected_columns = vec![
        "knowledge_level",
        "primary_knowledge_source",
        "aggregator_knowledge_source",
        "publications",
        "subject_aspect_qualifier",
        "subject_direction_qualifier",
        "object_aspect_qualifier",
        "object_direction_qualifier",
    ]
    .iter()
    .map(|a| a.to_string())
    .collect_vec();

    let column_names = df.get_column_names_str();

    let new_columns: Vec<_> = expected_columns
        .into_iter()
        .filter_map(|c| match column_names.contains(&c.as_str()) {
            true => None,
            false => Some(Series::full_null(PlSmallStr::from(c), capacity, &DataType::String).lit()),
        })
        .collect();
    df = df.clone().lazy().with_columns(new_columns).collect().unwrap();

    let mut file = fs::File::create(output.as_path()).unwrap();
    CsvWriter::new(&mut file).with_separator(b'\t').finish(&mut df).unwrap();

    info!("Duration: {}", format_duration(start.elapsed()).to_string());
    Ok(())
}
