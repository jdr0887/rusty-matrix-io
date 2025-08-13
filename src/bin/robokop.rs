#[macro_use]
extern crate log;

use clap::{Parser, Subcommand};
use humantime::format_duration;
use itertools::Itertools;
use polars::prelude::{LazyCsvReader, LazyFileListReader};
use rayon::prelude::*;
use std::error::Error;
use std::fs;
use std::io::{BufRead, Write};
use std::path;
use std::time::Instant;

#[derive(Parser, PartialEq, Debug)]
#[command(author, version, about, long_about = None)]
struct Options {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, PartialEq, Debug)]
enum Commands {
    ConvertBooleanColumnsToLabels {
        #[arg(short, long, required = true)]
        input: path::PathBuf,

        #[arg(short, long, required = true)]
        output: path::PathBuf,
    },
}

fn main() -> Result<(), Box<dyn Error>> {
    let start = Instant::now();
    env_logger::init();

    let options = Options::parse();
    debug!("{:?}", options);

    match &options.command {
        Some(Commands::ConvertBooleanColumnsToLabels { input, output }) => {
            convert_boolean_columns_to_labels(input, output).expect("Could not convert boolean columns to labels");
        }
        None => {}
    }

    info!("Duration: {}", format_duration(start.elapsed()).to_string());
    Ok(())
}

fn convert_boolean_columns_to_labels(input: &path::PathBuf, output: &path::PathBuf) -> Result<(), Box<dyn Error>> {
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

    let header_columns: Vec<String> = tmp_df.get_column_names_str().iter().map(|a| a.to_string()).collect();

    let keep_columns = header_columns
        .iter()
        .enumerate()
        .filter(|(_idx, col)| !col.starts_with("CHEBI_ROLE") && !col.starts_with("MONDO_SUPERCLASS"))
        .map(|(idx, col)| {
            let col_name_split = col.split(":").collect_vec();
            let col_name = col_name_split.get(0).unwrap();
            (idx, col_name.to_string())
        })
        .collect_vec();

    debug!("columns to keep: {:?}", keep_columns);

    let mut new_header = keep_columns.iter().map(|(_idx, col)| col.clone()).collect_vec().join("\t");
    new_header.push_str("\tCHEBI_ROLE");
    new_header.push_str("\tMONDO_SUPERCLASS");

    debug!("new_header: {:?}", new_header);

    let chebi_role_colums: Vec<(usize, String)> = header_columns
        .iter()
        .enumerate()
        .filter(|(_idx, col)| col.starts_with("CHEBI_ROLE"))
        .map(|(idx, col)| {
            let mut ret = None;
            if let Some((prefix, _suffix)) = col.split_once(':') {
                ret = Some((idx, prefix.replace("CHEBI_ROLE_", "")));
            }
            ret
        })
        .filter_map(std::convert::identity)
        .collect_vec();

    // debug!("chebi_role_colums: {:?}", chebi_role_colums);

    let mondo_superclass_colums: Vec<(usize, String)> = header_columns
        .iter()
        .enumerate()
        .filter(|(_idx, col)| col.starts_with("MONDO_SUPERCLASS"))
        .map(|(idx, col)| {
            let mut ret = None;
            if let Some((prefix, _suffix)) = col.split_once(':') {
                ret = Some((idx, prefix.replace("MONDO_SUPERCLASS_", "")));
            }
            ret
        })
        .filter_map(std::convert::identity)
        .collect_vec();

    // debug!("mondo_superclass_colums: {:?}", mondo_superclass_colums);

    let parent_dir = output.parent().unwrap();
    debug!("writing output to: {:?}", parent_dir);

    fs::create_dir_all(parent_dir).expect("Could not create parent directory");

    let mut writer = std::io::BufWriter::new(fs::File::create(output).unwrap());
    writer.write_all(format!("{}\n", new_header).as_bytes()).expect("Could not write line");

    // https://www.ascii-code.com/
    let separator = char::from_u32(0x0000007C).unwrap();

    let reader = std::io::BufReader::new(fs::File::open(input).unwrap());
    reader.lines().skip(1).for_each(|line| {
        let line = line.unwrap();
        let line_split = line.split('\t').collect_vec();
        debug!("{:?}", line_split);

        let mut new_line = String::new();
        keep_columns.iter().cloned().for_each(|(idx, _col)| {
            let value = line_split.get(idx).expect(format!("Could not unwrap: {} at index: {}", _col, idx).as_str());
            new_line.push_str(format!("{}\t", value).as_str());
        });

        debug!("{}", new_line);

        // debug!("chebi_role_colums: {:?}", chebi_role_colums);

        let chebi_role_labels: Vec<String> = chebi_role_colums
            .par_iter()
            .filter_map(|(idx, col)| {
                let mut ret = None;
                let value = line_split.get(idx.clone()).unwrap();
                if "true".eq(*value) {
                    ret = Some(col.clone());
                }
                ret
            })
            .collect();

        new_line.push_str(format!("{}\t", chebi_role_labels.into_iter().join(format!("{}", separator).as_str())).as_str());

        let mondo_superclass_labels: Vec<String> = mondo_superclass_colums
            .par_iter()
            .filter_map(|(idx, col)| {
                let mut ret = None;
                let value = line_split.get(idx.clone()).unwrap();
                if "true".eq(*value) {
                    ret = Some(col.clone())
                }
                ret
            })
            .collect();
        new_line.push_str(mondo_superclass_labels.into_iter().join(format!("{}", separator).as_str()).as_str());

        writer.write_all(format!("{}\n", new_line).as_bytes()).expect("Could not write line");
    });
    Ok(())
}
