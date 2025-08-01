use clap::Parser;
use humantime::format_duration;
use log::{debug, info, warn};
use polars::prelude::*;
use std::time::Instant;
use std::{error, fs, path};

#[derive(Parser, PartialEq, Debug)]
#[clap(author, version, about, long_about = None)]
struct Options {
    #[clap(short = 'i', long, required = true)]
    input: path::PathBuf,

    #[clap(short = 'o', long, required = true)]
    output: path::PathBuf,
}

fn main() -> Result<(), Box<dyn error::Error>> {
    let start = Instant::now();
    env_logger::init();

    let options = Options::parse();
    debug!("{:?}", options);

    let primary_columns = vec![
        "id",
        "category",
        "original_id",
        "all_categories",
        "name",
        "description",
        "equivalent_identifiers",
        "publications",
        "labels",
        "international_resource_identifier",
    ];

    let tmp_df = LazyCsvReader::new(options.input.clone())
        .with_separator(b'\t')
        .with_infer_schema_length(Some(0))
        .with_ignore_errors(true)
        .with_truncate_ragged_lines(true)
        .with_has_header(true)
        .finish()
        .unwrap()
        .limit(10)
        .collect()
        .unwrap();

    debug!("Shape of {} is {:?}", options.input.to_string_lossy(), tmp_df.shape());

    let mut header: Vec<String> = tmp_df.get_column_names_str().iter().map(|a| a.to_string()).collect();
    debug!("header: {:?}", header);
    for column_name in header.iter_mut() {
        if column_name.as_str().eq(":LABEL") {
            *column_name = "_label".to_string();
        }
        if column_name.contains(":") {
            let split: Vec<&str> = column_name.split(":").collect();
            *column_name = split.get(0).unwrap().to_string();
        }
        if !primary_columns.contains(&&**column_name) {
            column_name.insert_str(0, "_");
        }
    }
    debug!("transformed header: {:?}", header);

    let mut df = LazyCsvReader::new(options.input.clone())
        .with_separator(b'\t')
        .with_infer_schema_length(Some(0))
        .with_ignore_errors(true)
        .with_truncate_ragged_lines(true)
        .with_has_header(true)
        .finish()
        .unwrap()
        .collect()
        .unwrap();
    df.set_column_names(header).expect("Count not set column names");

    let parent_dir = options.output.parent().unwrap();
    debug!("writing output to: {:?}", parent_dir);

    fs::create_dir_all(parent_dir).expect("Could not create parent directory");
    let mut output_edges_file = fs::File::create(options.output.as_path()).unwrap();
    CsvWriter::new(&mut output_edges_file).with_separator(b'\t').finish(&mut df).unwrap();

    info!("Duration: {}", format_duration(start.elapsed()).to_string());
    Ok(())
}
