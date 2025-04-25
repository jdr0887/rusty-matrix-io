#[macro_use]
extern crate log;

use clap::Parser;
use humantime::format_duration;
use itertools::Itertools;
use log::{debug, info, warn};
use polars::prelude::*;
use std::error;
use std::io::BufRead;
use std::path;
use std::time::Instant;

#[derive(Parser, PartialEq, Debug)]
#[clap(author, version, about, long_about = None)]
struct Options {
    #[clap(short = 'i', long, required = true)]
    input: Vec<path::PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn error::Error>> {
    let start = Instant::now();
    env_logger::init();

    let options = Options::parse();
    debug!("{:?}", options);

    for input in options.input {
        let input_file = std::fs::File::open(input.as_path()).expect("Could not open edges file");
        let reader = std::io::BufReader::new(input_file);
        let lines = reader.lines().map(|l| l.expect("Could not parse line")).collect_vec();
        let header = lines.first().expect("Could not get header");
        let header_columns = header.split("\t").collect_vec();

        println!("Shape of {} is {:?}", input.to_string_lossy(), (lines.len(), header_columns.len()));
    }

    info!("Duration: {}", format_duration(start.elapsed()).to_string());
    Ok(())
}
