#[macro_use]
extern crate log;

use async_once::AsyncOnce;
use humantime::format_duration;
use in_place::InPlace;
use itertools::Itertools;
use lazy_static::lazy_static;
use log::{info, warn};
use polars::prelude::*;
use rayon::prelude::*;
use reqwest::redirect::Policy;
use reqwest::{header, Client};
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::ser::CharEscape::AsciiControl;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::error;
use std::ffi::OsStr;
use std::fmt::{Display, Formatter};
use std::fs;
use std::fs::{read_dir, File};
use std::io;
use std::io::prelude::*;
use std::io::BufWriter;
use std::path;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn error::Error>> {
    let start = Instant::now();
    env_logger::init();

    let parse_options = CsvParseOptions::default().with_separator(b'\t');

    let base_path = path::PathBuf::from("/home/jdr0887/data/matrix/KGs/spoke/V5");
    let edges_path = base_path.join("edges.tsv");

    let mut df = CsvReadOptions::default()
        .with_parse_options(parse_options.clone())
        .with_has_header(true)
        .with_ignore_errors(true)
        .try_into_reader_with_file_path(Some(edges_path.clone()))
        .unwrap()
        .finish()
        .unwrap();

    let capacity = df.height();
    let knowledge_level = Series::full_null(PlSmallStr::from("knowledge_level"), capacity, &DataType::String).lit();
    let primary_knowledge_source = Series::full_null(PlSmallStr::from("primary_knowledge_source"), capacity, &DataType::String).lit();
    let aggregator_knowledge_source = Series::full_null(PlSmallStr::from("aggregator_knowledge_source"), capacity, &DataType::String).lit();
    let publications = Series::full_null(PlSmallStr::from("publications"), capacity, &DataType::String).lit();
    let subject_aspect_qualifier = Series::full_null(PlSmallStr::from("subject_aspect_qualifier"), capacity, &DataType::String).lit();
    let subject_direction_qualifier = Series::full_null(PlSmallStr::from("subject_direction_qualifier"), capacity, &DataType::String).lit();
    let object_aspect_qualifier = Series::full_null(PlSmallStr::from("object_aspect_qualifier"), capacity, &DataType::String).lit();
    let object_direction_qualifier = Series::full_null(PlSmallStr::from("object_direction_qualifier"), capacity, &DataType::String).lit();

    df = df
        .clone()
        .lazy()
        .with_columns([
            knowledge_level,
            primary_knowledge_source,
            aggregator_knowledge_source,
            publications,
            subject_aspect_qualifier,
            subject_direction_qualifier,
            object_aspect_qualifier,
            object_direction_qualifier,
        ])
        .collect()
        .unwrap();

    let final_output_distinct = base_path.join("edges_with_empty_columns.tsv");
    let mut file = fs::File::create(final_output_distinct.as_path()).unwrap();
    CsvWriter::new(&mut file).with_separator(b'\t').finish(&mut df).unwrap();

    info!("Duration: {}", format_duration(start.elapsed()).to_string());
    Ok(())
}
