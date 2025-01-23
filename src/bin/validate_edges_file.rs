use clap::Parser;
use humantime::format_duration;
use itertools::Itertools;
use log::{debug, info};
use rayon::prelude::*;
use rusty_matrix_io::Edge;
use std::error;
use std::fs;
use std::io;
use std::io::BufWriter;
use std::path;
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
    let data = read_edges_file(&edges_path);

    let curie_regex = regex::Regex::new(r"^[A-Za-z_]+:.+$").expect("Could not create curie regex");
    let starts_with_biolink_regex = regex::Regex::new("^biolink:.+$").expect("Could not create biolink regex");

    let subject_column_validation_infractions: Vec<_> = data
        .par_iter()
        .filter_map(|n| match curie_regex.is_match(n.subject.as_str()) {
            true => None,
            false => Some(format!("subject column does not have a valid CURIE: {:?}", n)),
        })
        .collect();

    subject_column_validation_infractions.iter().for_each(|n| println!("{:?}", n));

    let predicate_column_validation_infractions: Vec<_> = data
        .par_iter()
        .filter_map(|n| match starts_with_biolink_regex.is_match(n.predicate.as_str()) {
            true => None,
            false => Some(format!("predicate column does not start with 'biolink': {:?}", n)),
        })
        .collect();

    predicate_column_validation_infractions.iter().for_each(|n| println!("{:?}", n));

    let object_column_validation_infractions: Vec<_> = data
        .par_iter()
        .filter_map(|n| match curie_regex.is_match(n.object.as_str()) {
            true => None,
            false => Some(format!("object column does not have a valid CURIE: {:?}", n)),
        })
        .collect();

    object_column_validation_infractions.iter().for_each(|n| println!("{:?}", n));

    info!("Duration: {}", format_duration(start.elapsed()).to_string());
    Ok(())
}

fn read_edges_file(nodes_path: &path::PathBuf) -> Vec<Edge> {
    let nodes_file = fs::File::open(nodes_path.clone()).unwrap();
    let reader = io::BufReader::new(nodes_file);
    let mut rdr = csv::ReaderBuilder::new().has_headers(true).delimiter(b'\t').from_reader(reader);

    let mut edges = vec![];
    for result in rdr.deserialize() {
        let record: Edge = result.unwrap();
        edges.push(record.clone());
    }
    edges
}
