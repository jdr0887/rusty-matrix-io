use clap::{Parser, Subcommand};
use humantime::format_duration;
use log::{debug, info, warn};
use polars::prelude::*;
use std::path::PathBuf;
use std::time::Instant;
use std::{error, fs, path};

#[derive(Parser, PartialEq, Debug)]
#[command(author, version, about, long_about = None)]
struct Options {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, PartialEq, Debug)]
enum Commands {
    NodesHeader {
        #[arg(short, long, required = true)]
        input: path::PathBuf,

        #[arg(short, long, required = true)]
        output: path::PathBuf,
    },
    EdgesHeader {
        #[arg(short, long, required = true)]
        input: path::PathBuf,

        #[arg(short, long, required = true)]
        output: path::PathBuf,
    },
}
fn main() -> Result<(), Box<dyn error::Error>> {
    let start = Instant::now();
    env_logger::init();

    let options = Options::parse();
    debug!("{:?}", options);

    match &options.command {
        Some(Commands::NodesHeader { input, output }) => {
            clean_nodes_header(input, output).expect("Could not clean nodes header");
        }
        Some(Commands::EdgesHeader { input, output }) => {
            clean_edges_header(input, output).expect("Could not clean edges header");
        }
        None => {}
    }

    info!("Duration: {}", format_duration(start.elapsed()).to_string());
    Ok(())
}

fn clean_edges_header(input: &PathBuf, output: &PathBuf) -> Result<(), Box<dyn error::Error>> {
    let primary_columns = vec![
        "subject",
        "predicate",
        "object",
        "publications",
        "knowledge_level",
        "agent_type",
        "primary_knowledge_source",
        "subject_aspect_qualifier",
        "subject_direction_qualifier",
        "object_aspect_qualifier",
        "object_direction_qualifier",
        "aggregator_knowledge_source",
        "original_subject",
        "original_object",
    ];

    let tmp_df = LazyCsvReader::new(input.clone())
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

    debug!("Shape of {} is {:?}", input.to_string_lossy(), tmp_df.shape());

    let mut header: Vec<String> = tmp_df.get_column_names_str().iter().map(|a| a.to_string()).collect();
    debug!("header: {:?}", header);
    for column_name in header.iter_mut() {
        if column_name.contains(":") {
            let split: Vec<&str> = column_name.split(":").collect();
            *column_name = split.get(0).unwrap().to_string();
        }
        if !primary_columns.contains(&&**column_name) {
            column_name.insert_str(0, "_");
        }
    }
    debug!("transformed header: {:?}", header);

    let mut df = LazyCsvReader::new(input.clone())
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

    let parent_dir = output.parent().unwrap();
    debug!("writing output to: {:?}", parent_dir);

    fs::create_dir_all(parent_dir).expect("Could not create parent directory");
    let mut output_edges_file = fs::File::create(output.as_path()).unwrap();
    CsvWriter::new(&mut output_edges_file).with_separator(b'\t').finish(&mut df).unwrap();

    Ok(())
}

fn clean_nodes_header(input: &PathBuf, output: &PathBuf) -> Result<(), Box<dyn error::Error>> {
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

    let tmp_df = LazyCsvReader::new(input)
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

    debug!("Shape of {} is {:?}", input.to_string_lossy(), tmp_df.shape());

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

    let mut df = LazyCsvReader::new(input.clone())
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

    let parent_dir = output.parent().unwrap();
    debug!("writing output to: {:?}", parent_dir);

    fs::create_dir_all(parent_dir).expect("Could not create parent directory");
    let mut output_edges_file = fs::File::create(output.as_path()).unwrap();
    CsvWriter::new(&mut output_edges_file).with_separator(b'\t').finish(&mut df).unwrap();
    Ok(())
}
