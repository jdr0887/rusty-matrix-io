use clap::Parser;
use humantime::format_duration;
use itertools::Itertools;
use log::{debug, info, warn};
use polars::prelude::*;
use std::fs;
use std::time::Instant;
use std::{error, path};

#[derive(Parser, PartialEq, Debug)]
#[clap(author, version, about, long_about = None)]
struct Options {
    #[clap(short = 'n', long, required = true)]
    nodes: path::PathBuf,

    #[clap(short = 'e', long, required = true)]
    edges: path::PathBuf,

    #[clap(short = 'o', long, required = true)]
    output_dir: path::PathBuf,

    #[clap(short = 's', long, default_value_t = 100)]
    size: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn error::Error>> {
    let start = Instant::now();
    env_logger::init();

    let options = Options::parse();
    debug!("{:?}", options);

    let mut edge_id_columns_df = LazyCsvReader::new(options.edges.clone())
        .with_separator(b'\t')
        .with_truncate_ragged_lines(true)
        .with_has_header(true)
        .with_ignore_errors(true)
        .finish()
        .unwrap()
        .select([col("subject"), col("object")])
        .collect()
        .unwrap();

    // CsvWriter::new(&mut fs::File::create("/tmp/edge_id_columns.tsv").unwrap()).with_separator(9u8).finish(&mut edge_id_columns_df).unwrap();

    let mut edge_ids_df = concat(
        [edge_id_columns_df.clone().lazy().select([col("subject").alias("id")]), edge_id_columns_df.clone().lazy().select([col("object").alias("id")])],
        UnionArgs::default(),
    )
    .unwrap()
    .unique(None, UniqueKeepStrategy::First)
    .limit(IdxSize::from(options.size / 2))
    .collect()
    .unwrap();

    // CsvWriter::new(&mut fs::File::create("/tmp/edge_ids.tsv").unwrap()).with_separator(9u8).finish(&mut edge_ids_df).unwrap();

    let ids_sorted_deduped = edge_ids_df.column("id").unwrap().as_series().unwrap().clone();

    let mut edges_df = LazyCsvReader::new(options.edges.clone())
        .with_separator(b'\t')
        .with_truncate_ragged_lines(true)
        .with_has_header(true)
        .with_ignore_errors(true)
        .finish()
        .unwrap()
        .filter(col("subject").str().contains_any(lit(ids_sorted_deduped.clone()), false).or(col("object").str().contains_any(lit(ids_sorted_deduped.clone()), false)))
        .limit(IdxSize::from(options.size))
        .collect()
        .unwrap();

    // CsvWriter::new(&mut fs::File::create("/tmp/filtered_subjects_edges.tsv").unwrap()).with_separator(9u8).finish(&mut filtered_subject_edges_df.collect().unwrap()).unwrap();

    let mut output_edges_file = fs::File::create(format!("{}/edges_{}.tsv", options.output_dir.to_string_lossy(), options.size.clone())).unwrap();
    CsvWriter::new(&mut output_edges_file).with_separator(9u8).finish(&mut edges_df).unwrap();

    let selected_edge_ids_df =
        concat([edges_df.clone().lazy().select([col("subject").alias("id")]), edges_df.clone().lazy().select([col("object").alias("id")])], UnionArgs::default())
            .unwrap()
            .collect()
            .unwrap();
    let selected_edge_ids = selected_edge_ids_df.column("id").unwrap().as_series().unwrap().clone();

    let mut nodes_df = LazyCsvReader::new(options.nodes)
        .with_separator(b'\t')
        .with_truncate_ragged_lines(true)
        .with_has_header(true)
        .with_ignore_errors(true)
        .finish()
        .unwrap()
        .filter(col("id").str().contains_any(lit(selected_edge_ids.clone()), false))
        .collect()
        .unwrap();

    let mut output_nodes_file = fs::File::create(format!("{}/nodes_{}.tsv", options.output_dir.to_string_lossy(), options.size.clone())).unwrap();
    CsvWriter::new(&mut output_nodes_file).with_separator(9u8).finish(&mut nodes_df).unwrap();

    info!("Duration: {}", format_duration(start.elapsed()).to_string());
    Ok(())
}
