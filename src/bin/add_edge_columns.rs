#[macro_use]
extern crate log;

use clap::Parser;
use humantime::format_duration;
use itertools::Itertools;
use polars::prelude::*;
use std::error;
use std::fs;
use std::path;
use std::time::Instant;

#[derive(Parser, PartialEq, Debug)]
#[clap(author, version, about, long_about = None)]
struct Options {
    #[clap(short = 'i', long, required = true)]
    input: path::PathBuf,

    #[clap(short = 'o', long, required = true)]
    output: path::PathBuf,

    #[clap(short = 'p', long, required = true)]
    primary_knowledge_source: String,

    #[clap(short = 'k', long, default_value = "knowledge_assertion")]
    knowledge_level: String,

    #[clap(short = 'a', long, default_value = "data_analysis_pipeline")]
    agent_type: String,
}

fn main() -> Result<(), Box<dyn error::Error>> {
    let start = Instant::now();
    env_logger::init();

    let options = Options::parse();
    debug!("{:?}", options);

    let input = options.input;

    let mut df = create_missing_columns(input).expect("Could not fill missing columns");

    df = df
        .clone()
        .lazy()
        .with_columns([
            col("knowledge_level").fill_null(lit(options.knowledge_level)),
            col("agent_type").fill_null(lit(options.agent_type)),
            col("primary_knowledge_source").fill_null(lit(options.primary_knowledge_source)),
        ])
        .collect()
        .unwrap();

    let output = options.output;
    let mut file = fs::File::create(output.as_path()).unwrap();
    CsvWriter::new(&mut file).with_separator(b'\t').finish(&mut df).unwrap();

    info!("Duration: {}", format_duration(start.elapsed()).to_string());
    Ok(())
}

fn create_missing_columns(input: path::PathBuf) -> Result<DataFrame, Box<dyn error::Error>> {
    let parse_options = CsvParseOptions::default().with_separator(b'\t');

    let df = CsvReadOptions::default()
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
        "agent_type",
        "primary_knowledge_source",
        "aggregator_knowledge_source",
        "publications",
        "subject_aspect_qualifier",
        "subject_direction_qualifier",
        "object_aspect_qualifier",
        "object_direction_qualifier",
    ]
    .into_iter()
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

    Ok(df.clone().lazy().with_columns(new_columns).collect().unwrap())
}
