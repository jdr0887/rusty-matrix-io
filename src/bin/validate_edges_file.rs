use async_once::AsyncOnce;
use clap::Parser;
use humantime::format_duration;
use in_place::InPlace;
use itertools::Itertools;
use lazy_static::lazy_static;
use log::{debug, info, warn};
use polars::prelude::*;
use rayon::prelude::*;
use reqwest::redirect::Policy;
use reqwest::{header, Client};
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::ser::CharEscape::AsciiControl;
use serde_with::skip_serializing_none;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::error;
use std::ffi::OsStr;
use std::fmt::{Display, Formatter};
use std::fs;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::io::BufWriter;
use std::path;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

#[derive(Parser, PartialEq, Debug)]
#[clap(author, version, about, long_about = None)]
struct Options {
    #[clap(short = 'e', long, required = true)]
    edges_file: path::PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn error::Error>> {
    let start = Instant::now();
    env_logger::init();

    let options = Options::parse();
    debug!("{:?}", options);

    let edges_path = options.edges_file;

    let curie_regex = regex::Regex::new(r"^[A-Za-z_]+:.+$").expect("Could not create curie regex");
    let starts_with_biolink_regex = regex::Regex::new("^biolink:.+$").expect("Could not create biolink regex");

    let subject_column_validation_infractions: Vec<_> = data
        .par_iter()
        .filter_map(|n| match curie_regex.is_match(n.id.as_str()) {
            true => None,
            false => Some(format!("subject column does not have a valid CURIE: {:?}", n)),
        })
        .collect();

    subject_column_validation_infractions.iter().for_each(|n| println!("{:?}", n));

    let predicate_column_validation_infractions: Vec<_> = data
        .par_iter()
        .filter_map(|n| match starts_with_biolink_regex.is_match(n.id.as_str()) {
            true => None,
            false => Some(format!("predicate column does not start with 'biolink': {:?}", n)),
        })
        .collect();

    predicate_column_validation_infractions.iter().for_each(|n| println!("{:?}", n));

    let object_column_validation_infractions: Vec<_> = data
        .par_iter()
        .filter_map(|n| match curie_regex.is_match(n.id.as_str()) {
            true => None,
            false => Some(format!("object column does not have a valid CURIE: {:?}", n)),
        })
        .collect();

    object_column_validation_infractions.iter().for_each(|n| println!("{:?}", n));

    info!("Duration: {}", format_duration(start.elapsed()).to_string());
    Ok(())
}
