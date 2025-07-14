#[macro_use]
extern crate log;

use humantime::format_duration;
use itertools::Itertools;
use log::info;
use polars::prelude::*;
use std::error;
use std::fs;
use std::path;
use std::path::PathBuf;
use std::time::Instant;

fn main() -> Result<(), Box<dyn error::Error>> {
    let start = Instant::now();
    env_logger::init();

    let base_path = path::PathBuf::from("/home/jdr0887/data/matrix/KGs/spoke/V5");
    let edges_path = base_path.join("edges");

    let edge_files_to_use = vec![
        "edge_4.tsv",
        "edge_6.tsv",
        "edge_7.tsv",
        "edge_8.tsv",
        "edge_21.tsv",
        "edge_24.tsv",
        "edge_27.tsv",
        "edge_33.tsv",
        "edge_36.tsv",
        "edge_39.tsv",
        "edge_47.tsv",
        "edge_49.tsv",
        "edge_50.tsv",
        "edge_53.tsv",
        "edge_56.tsv",
        "edge_59.tsv",
        "edge_61.tsv",
        "edge_62.tsv",
        "edge_63.tsv",
        "edge_69.tsv",
        "edge_72.tsv",
        "edge_73.tsv",
        "edge_75.tsv",
        "edge_84.tsv",
        "edge_85.tsv",
        "edge_88.tsv",
    ];

    let usable_edge_paths = edge_files_to_use.into_iter().map(|a| edges_path.join(a)).collect_vec();

    let output = base_path.join("merged_edges.tsv");
    merge_files(&output, usable_edge_paths);

    info!("Duration: {}", format_duration(start.elapsed()).to_string());
    Ok(())
}

fn merge_files(output_path: &PathBuf, edge_file_names: Vec<PathBuf>) {
    let join_args = JoinArgs::new(JoinType::Full).with_coalesce(JoinCoalesce::CoalesceColumns);
    let mut main_df = df!(
        "subject" => &Vec::<String>::new(),
                   "predicate" => &Vec::<String>::new(),
                   "object" => &Vec::<String>::new(),
    )
    .unwrap();

    for edge_file_path in edge_file_names.into_iter() {
        info!("edge_file_path: {:?}", edge_file_path);

        let df = LazyCsvReader::new(edge_file_path.clone()).with_separator(b'\t').with_truncate_ragged_lines(true).with_has_header(true).with_ignore_errors(true).finish().unwrap();

        main_df = main_df
            .clone()
            .lazy()
            .join(df.clone(), [col("subject"), col("predicate"), col("object")], [col("subject"), col("predicate"), col("object")], join_args.clone())
            .collect()
            .expect("Could not join");

        let columns = vec!["source", "sources", "unbiased", "evidence", "vestige", "version", "p_value", "direction", "alternative_allele", "reference_allele"];
        main_df = rusty_matrix_io::coalesce_columns(main_df, columns);

        println!("column names: {:?}", main_df.get_column_names());
    }

    let mut file = fs::File::create(output_path.as_path()).unwrap();
    CsvWriter::new(&mut file).with_separator(b'\t').finish(&mut main_df).unwrap();
}

#[cfg(test)]
mod test {
    use in_place::InPlace;
    use itertools::Itertools;
    use polars::prelude::*;
    use rusty_matrix_io::Node;
    use serde_json::json;
    use std::collections::HashMap;
    use std::io::BufWriter;
    use std::io::{BufRead, BufReader, Write};
    use std::{fs, io, path};

    #[test]
    fn scratch() {
        assert_eq!(true, true);
    }
}
