#[macro_use]
extern crate log;

use async_once::AsyncOnce;
use humantime::format_duration;
use in_place::InPlace;
use lazy_static::lazy_static;
use log::{info, warn};
use polars::prelude::*;
use reqwest::redirect::Policy;
use reqwest::{Client, header};
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::ser::CharEscape::AsciiControl;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::error;
use std::fs;
use std::fs::{File, read_dir};
use std::io;
use std::io::BufWriter;
use std::io::prelude::*;
use std::path;
use std::path::PathBuf;
use std::time::Instant;

fn main() -> Result<(), Box<dyn error::Error>> {
    let start = Instant::now();
    env_logger::init();

    let base_path = path::PathBuf::from("/home/jdr0887/data/matrix/KGs/spoke/V5");
    // let nodes_path = base_path.join("nodes");

    // let first_output_path = base_path.join("first_merged_nodes.tsv");
    // let second_output_path = base_path.join("second_merged_nodes.tsv");
    // let third_output_path = base_path.join("third_merged_nodes.tsv");
    // let fourth_output_path = base_path.join("fourth_merged_nodes.tsv");
    // let fifth_output_path = base_path.join("fifth_merged_nodes.tsv");

    // let first_node_file_names = vec![
    //     "node_0_new.tsv",
    //     "node_1_new.tsv",
    //     "node_3_new.tsv",
    //     "node_4_new.tsv",
    //     "node_6_new.tsv",
    //     "node_9_new.tsv",
    //     "node_10_new.tsv",
    //     "node_11_new.tsv",
    //     "node_12_new.tsv", // doesn't have name...maybe use reference instead???
    //     "node_13_new.tsv", // doesn't have name...maybe use accession instead???
    //     "node_14_new.tsv",
    //     "node_15_new.tsv",
    //     "node_19_new.tsv",
    //     "node_20_new.tsv",
    //     "node_21_new.tsv",
    //     "node_25_new.tsv", // doesn't have name...maybe use Allele_ID instead???
    // ]
    // .into_iter()
    // .map(|a| base_path.join(a))
    // .collect_vec();
    //
    // merge_files(&first_output_path, first_node_file_names);
    //
    // let second_node_file_names = vec![
    //     "node_18_new_00.tsv",
    //     "node_18_new_01.tsv",
    //     "node_18_new_02.tsv",
    //     "node_18_new_03.tsv",
    //     "node_18_new_04.tsv",
    //     "node_18_new_05.tsv",
    //     "node_18_new_06.tsv",
    //     "node_18_new_07.tsv",
    //     "node_18_new_08.tsv",
    //     "node_18_new_09.tsv",
    // ]
    // .into_iter()
    // .map(|a| base_path.join(a))
    // .collect_vec();
    //
    // merge_files(&second_output_path, second_node_file_names);
    //
    // let third_node_file_names = vec![
    //     "node_18_new_10.tsv",
    //     "node_18_new_11.tsv",
    //     "node_18_new_12.tsv",
    //     "node_18_new_13.tsv",
    //     "node_18_new_14.tsv",
    //     "node_18_new_15.tsv",
    //     "node_18_new_16.tsv",
    //     "node_18_new_17.tsv",
    //     "node_18_new_18.tsv",
    //     "node_18_new_19.tsv",
    // ]
    // .into_iter()
    // .map(|a| base_path.join(a))
    // .collect_vec();
    //
    // merge_files(&third_output_path, third_node_file_names);
    //
    // let fourth_node_file_names = vec![
    //     "node_18_new_20.tsv",
    //     "node_18_new_21.tsv",
    //     "node_18_new_22.tsv",
    //     "node_18_new_23.tsv",
    //     "node_18_new_24.tsv",
    //     "node_18_new_25.tsv",
    //     "node_18_new_26.tsv",
    //     "node_18_new_27.tsv",
    //     "node_18_new_28.tsv",
    //     "node_18_new_29.tsv",
    // ]
    // .into_iter()
    // .map(|a| base_path.join(a))
    // .collect_vec();
    //
    // merge_files(&fourth_output_path, fourth_node_file_names);
    //
    // let fifth_node_file_names = vec![
    //     "node_18_new_30.tsv",
    //     "node_18_new_31.tsv",
    //     "node_18_new_32.tsv",
    //     "node_18_new_33.tsv",
    //     "node_18_new_34.tsv",
    //     "node_18_new_35.tsv",
    //     "node_18_new_36.tsv",
    //     "node_18_new_37.tsv",
    //     "node_18_new_38.tsv",
    // ]
    // .into_iter()
    // .map(|a| base_path.join(a))
    // .collect_vec();
    //
    // merge_files(JoinArgs::new(JoinType::Full).with_coalesce(JoinCoalesce::CoalesceColumns), &fifth_output_path, fifth_node_file_names);

    // let part_1 = base_path.join("merged_nodes_1_2.tsv");
    // merge_files(&part_1, vec![first_output_path.clone(), second_output_path]);
    //
    // let part_2 = base_path.join("merged_nodes_1_3.tsv");
    // merge_files(&part_2, vec![first_output_path.clone(), third_output_path]);
    //
    // let part_3 = base_path.join("merged_nodes_1_4.tsv");
    // merge_files(&part_3, vec![first_output_path.clone(), fourth_output_path]);
    //
    // let part_4 = base_path.join("merged_nodes_1_5.tsv");
    // merge_files(&part_4, vec![first_output_path.clone(), fifth_output_path]);

    // let part_one_df = LazyCsvReader::new(part_1.clone())
    //     .with_separator(b'\t')
    //     .with_truncate_ragged_lines(true)
    //     .with_has_header(true)
    //     .with_ignore_errors(true)
    //     .finish()
    //     .unwrap()
    //     .with_columns([
    //         col("vestige").strict_cast(DataType::String),
    //         col("org_ncbi_id").strict_cast(DataType::String),
    //         col("start").strict_cast(DataType::String),
    //         col("end").strict_cast(DataType::String),
    //     ])
    //     .collect()
    //     .unwrap();
    //
    // println!("column names: {:?}", part_one_df.get_column_names());
    // println!("column types: {:?}", part_one_df.dtypes());
    //
    // let part_two_df = LazyCsvReader::new(part_2.clone())
    //     .with_separator(b'\t')
    //     .with_truncate_ragged_lines(true)
    //     .with_has_header(true)
    //     .with_ignore_errors(true)
    //     .finish()
    //     .unwrap()
    //     .with_columns([
    //         col("vestige").strict_cast(DataType::String),
    //         col("org_ncbi_id").strict_cast(DataType::String),
    //         col("start").strict_cast(DataType::String),
    //         col("end").strict_cast(DataType::String),
    //     ])
    //     .collect()
    //     .unwrap();
    //
    // println!("column names: {:?}", part_two_df.get_column_names());
    // println!("column types: {:?}", part_two_df.dtypes());
    //
    // let part_three_df = LazyCsvReader::new(part_3.clone())
    //     .with_separator(b'\t')
    //     .with_truncate_ragged_lines(true)
    //     .with_has_header(true)
    //     .with_ignore_errors(true)
    //     .finish()
    //     .unwrap()
    //     .with_columns([
    //         col("vestige").strict_cast(DataType::String),
    //         col("org_ncbi_id").strict_cast(DataType::String),
    //         col("start").strict_cast(DataType::String),
    //         col("end").strict_cast(DataType::String),
    //     ])
    //     .collect()
    //     .unwrap();
    //
    // println!("column names: {:?}", part_three_df.get_column_names());
    // println!("column types: {:?}", part_three_df.dtypes());
    //
    // let mut df_vertical_concat = concat([part_one_df.clone().lazy(), part_two_df.clone().lazy(), part_three_df.clone().lazy()], UnionArgs::default())
    //     .unwrap()
    //     .unique(Some(vec!["id".parse().unwrap(), "category".parse().unwrap()]), UniqueKeepStrategy::First)
    //     .collect()
    //     .unwrap();
    //

    let final_output = base_path.join("merged_nodes.tsv");
    let final_df = LazyCsvReader::new(final_output.clone())
        .with_separator(b'\t')
        .with_truncate_ragged_lines(true)
        .with_has_header(true)
        .with_ignore_errors(true)
        .finish()
        .unwrap()
        // .unique(Some(vec!["id".parse().unwrap(), "category".parse().unwrap()]), UniqueKeepStrategy::First)
        .collect()
        .unwrap();

    println!("final_df.shape(): {:?}", final_df.shape());

    // let final_output_distinct = base_path.join("merged_nodes_distinct.tsv");
    // let mut file = fs::File::create(final_output_distinct.as_path()).unwrap();
    // CsvWriter::new(&mut file).with_separator(b'\t').finish(&mut final_df).unwrap();

    info!("Duration: {}", format_duration(start.elapsed()).to_string());
    Ok(())
}

fn merge_files(output_path: &PathBuf, node_file_names: Vec<PathBuf>) {
    let join_args = JoinArgs::new(JoinType::Full).with_coalesce(JoinCoalesce::CoalesceColumns);
    let mut main_df = df!(
        "id" => &Vec::<String>::new(),
                   "name" => &Vec::<String>::new(),
                   "category" => &Vec::<String>::new(),
    )
    .unwrap();

    for node_file_path in node_file_names.into_iter() {
        info!("node_file_path: {:?}", node_file_path);

        let df = LazyCsvReader::new(node_file_path.clone())
            .with_separator(b'\t')
            .with_truncate_ragged_lines(true)
            .with_has_header(true)
            .with_ignore_errors(true)
            .finish()
            .unwrap();

        main_df = main_df
            .clone()
            .lazy()
            .join(df.clone(), [col("id"), col("category")], [col("id"), col("category")], join_args.clone())
            .collect()
            .expect("Could not join");

        let columns = vec![
            "name",
            "source",
            "description",
            "license",
            "url",
            "synonyms",
            "vestige",
            "sources",
            "accession",
            "chembl_id",
            "org_name",
            "reviewed",
            "EC",
            "org_ncbi_id",
            "gene",
            "start",
            "end",
            "polyprotein",
            "isoform",
        ];
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
