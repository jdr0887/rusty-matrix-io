use clap::Parser;
use humantime::format_duration;
use itertools::Itertools;
use log::{debug, info};
use polars::prelude::*;
use rand::distr::Uniform;
use rand::Rng;
use std::collections::{BTreeSet, HashSet};
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
}

fn main() -> Result<(), Box<dyn error::Error>> {
    let start = Instant::now();
    env_logger::init();

    let options = Options::parse();
    debug!("{:?}", options);

    let edges_id_columns_df = LazyCsvReader::new(options.edges.clone())
        .with_separator(b'\t')
        .with_truncate_ragged_lines(true)
        .with_has_header(true)
        .with_ignore_errors(true)
        .finish()
        .unwrap()
        .select([col("subject"), col("object")])
        .collect()
        .unwrap();

    let edge_ids_df = concat(
        [
            edges_id_columns_df.clone().lazy().select([col("subject").alias("id")]),
            edges_id_columns_df.clone().lazy().select([col("object").alias("id")]),
        ],
        UnionArgs::default(),
    )
    .unwrap()
    .unique(None, UniqueKeepStrategy::First)
    .collect()
    .unwrap();

    let edges_id_column = edge_ids_df.column("id").unwrap();
    let edges_id_series = edges_id_column.as_series().unwrap();

    let edges_ids: BTreeSet<String> = edges_id_series.str().unwrap().into_iter().filter_map(|a| a.map(String::from)).collect();

    let mut nodes_df = LazyCsvReader::new(options.nodes.clone())
        .with_separator(b'\t')
        .with_truncate_ragged_lines(true)
        .with_has_header(true)
        .with_ignore_errors(true)
        .finish()
        .unwrap()
        .select([col("id")])
        .collect()
        .unwrap();

    let nodes_id_column = nodes_df.column("id").unwrap();
    let nodes_id_series = nodes_id_column.as_series().unwrap();

    let nodes_ids: BTreeSet<String> = nodes_id_series.str().unwrap().into_iter().filter_map(|a| a.map(String::from)).collect();

    println!(
        "nodes_ids.difference(edges_ids): {:?}",
        nodes_ids.difference(&edges_ids).cloned().collect::<BTreeSet<_>>()
    );
    println!(
        "edges_ids.difference(nodes_ids): {:?}",
        edges_ids.difference(&nodes_ids).cloned().collect::<BTreeSet<_>>()
    );

    info!("Duration: {}", format_duration(start.elapsed()).to_string());
    Ok(())
}
