use clap::Parser;
use humantime::format_duration;
use itertools::Itertools;
use log::{debug, info};
use polars::functions::concat_df_horizontal;
use polars::prelude::*;
use std::path;
use std::time::Instant;
use std::{error, fs};

#[derive(Parser, PartialEq, Debug)]
#[clap(author, version, about, long_about = None)]
struct Options {
    #[clap(short = 'i', long, required = true)]
    input: path::PathBuf,

    #[clap(short = 'o', long, required = true)]
    output: path::PathBuf,

    #[clap(short = 'z', long)]
    columns_to_ignore: Option<Vec<String>>,
}

fn main() -> Result<(), Box<dyn error::Error>> {
    let start = Instant::now();
    env_logger::init();

    let options = Options::parse();
    debug!("{:?}", options);

    let df = LazyCsvReader::new(options.input.clone())
        .with_separator(b'\t')
        .with_infer_schema_length(Some(0))
        .with_ignore_errors(true)
        .with_truncate_ragged_lines(true)
        .with_has_header(true)
        .finish()
        .unwrap()
        .collect()
        .unwrap();

    let column_names = match options.columns_to_ignore {
        Some(cols) => df
            .get_column_names_str()
            .iter()
            .filter_map(|c| {
                if c.starts_with("_") && !cols.contains(&c.to_string()) {
                    Some(c.to_string())
                } else {
                    None
                }
            })
            .collect_vec(),
        None => df
            .get_column_names_str()
            .iter()
            .filter_map(|c| if c.starts_with("_") { Some(c.to_string()) } else { None })
            .collect_vec(),
    };

    let mut dfs = vec![];
    for cn in column_names {
        let tmp_df = df
            .column(cn.as_str())
            .unwrap()
            .as_series()
            .unwrap()
            .value_counts(true, true, "counts".into(), false)
            .unwrap();
        let head_df = tmp_df.head(None).lazy();
        let tail_df = tmp_df.tail(None).lazy();
        let combined_df = concat([head_df, tail_df], UnionArgs::default()).unwrap().collect().unwrap();
        dfs.push(combined_df);
    }

    let mut output = fs::File::create(options.output.clone()).unwrap();
    let mut combined_df = concat_df_horizontal(&dfs.as_slice(), false).unwrap();
    CsvWriter::new(&mut output).finish(&mut combined_df).unwrap();

    info!("Duration: {}", format_duration(start.elapsed()).to_string());
    Ok(())
}
