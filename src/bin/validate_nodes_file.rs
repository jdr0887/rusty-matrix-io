use clap::Parser;
use humantime::format_duration;
use log::{debug, info};
use rayon::prelude::*;
use rusty_matrix_io::Node;
use std::error;
use std::fs;
use std::io;
use std::io::BufWriter;
use std::path;
use std::time::Instant;

#[derive(Parser, PartialEq, Debug)]
#[clap(author, version, about, long_about = None)]
struct Options {
    #[clap(short = 'n', long, required = true)]
    nodes_file: path::PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn error::Error>> {
    let start = Instant::now();
    env_logger::init();

    let options = Options::parse();
    debug!("{:?}", options);

    let nodes_path = options.nodes_file;
    let data = read_nodes_file(&nodes_path);

    let curie_regex = regex::Regex::new(r"^[A-Za-z_]+:.+$").expect("Could not create curie regex");
    let starts_with_biolink_regex = regex::Regex::new("^biolink:.+$").expect("Could not create biolink regex");

    let id_column_validation_infractions: Vec<_> = data
        .par_iter()
        .filter_map(|n| match curie_regex.is_match(n.id.as_str()) {
            true => None,
            false => Some(format!("Id column does not have a valid CURIE: {:?}", n)),
        })
        .collect();

    id_column_validation_infractions.iter().for_each(|n| println!("{:?}", n));

    let category_column_validation_infractions: Vec<_> = data
        .par_iter()
        .filter_map(|n| match starts_with_biolink_regex.is_match(n.category.as_str()) {
            true => None,
            false => Some(format!("Category column does start with 'biolink': {:?}", n)),
        })
        .collect();

    category_column_validation_infractions.iter().for_each(|n| println!("{:?}", n));

    info!("Duration: {}", format_duration(start.elapsed()).to_string());
    Ok(())
}

fn read_nodes_file(nodes_path: &path::PathBuf) -> Vec<Node> {
    let nodes_file = fs::File::open(nodes_path.clone()).unwrap();
    let reader = io::BufReader::new(nodes_file);
    let mut rdr = csv::ReaderBuilder::new().has_headers(true).delimiter(b'\t').from_reader(reader);

    let mut nodes = vec![];
    for result in rdr.deserialize() {
        let record: Node = result.unwrap();
        nodes.push(record.clone());
    }
    nodes
}
