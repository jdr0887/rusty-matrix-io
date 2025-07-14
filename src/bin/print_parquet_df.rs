use clap::Parser;
use humantime::format_duration;
use log::{debug, info};
use polars::prelude::*;
use std::time::Instant;
use std::{error, path};

#[derive(Parser, PartialEq, Debug)]
#[clap(author, version, about, long_about = None)]
struct Options {
    #[clap(short = 'i', long, required = true)]
    input: Vec<path::PathBuf>,

    #[clap(short = 'o', long)]
    output: Option<path::PathBuf>,
}

fn main() -> Result<(), Box<dyn error::Error>> {
    let start = Instant::now();
    env_logger::init();

    let options = Options::parse();
    debug!("{:?}", options);

    let input_files = options.input;

    let first_input = input_files.first().expect("Could not get first input");
    let mut file = std::fs::File::open(first_input.as_path()).unwrap();
    let mut df = ParquetReader::new(&mut file).finish().unwrap();

    for input in input_files.iter().skip(1) {
        let mut file = std::fs::File::open(input.as_path()).unwrap();
        let tmp = ParquetReader::new(&mut file).finish().unwrap();
        df.extend(&tmp).expect("Could not extend DF");
    }
    println!("{:?}", df.shape());

    match options.output {
        Some(output_path) => {
            let mut file = std::fs::File::create(output_path.as_path()).unwrap();
            // CsvWriter::new(&mut file).finish(&mut df).unwrap();
            JsonWriter::new(&mut file).with_json_format(JsonFormat::Json).finish(&mut df).unwrap();
        }
        _ => {}
    }

    info!("Duration: {}", format_duration(start.elapsed()).to_string());
    Ok(())
}
// /home/jdr0887/workspace/github/everycure-org/matrix/pipelines/matrix/data/cache/normalization_source_rtx_kg2/api=nodenorm-2.3.18/0b2140a778dfc30f1cfb3841e8a446b62640241940cebfa28a674ddbadeb094b.parquet
// /home/jdr0887/workspace/github/everycure-org/matrix/pipelines/matrix/data/cache/normalization_source_rtx_kg2/api=nodenorm-2.3.18/2ae55c3c8b9a168f15746ff3effab4d1665a319430347b3e2f38cb88b99d8479.parquet
// /home/jdr0887/workspace/github/everycure-org/matrix/pipelines/matrix/data/cache/normalization_source_rtx_kg2/api=nodenorm-2.3.18/6f4e5fdec68aeace6a46419d1989797dc1d8a5443064129463659992834f17a0.parquet
// /home/jdr0887/workspace/github/everycure-org/matrix/pipelines/matrix/data/cache/normalization_source_rtx_kg2/api=nodenorm-2.3.18/30a6d951fb1d139c595be8751568d4a8c8639f4c5e8b71a4f7f434caea6472d2.parquet
// /home/jdr0887/workspace/github/everycure-org/matrix/pipelines/matrix/data/cache/normalization_source_rtx_kg2/api=nodenorm-2.3.18/a539986edf7d7e133c8f12c1d988a21a0d993d0a2441eed1a1f1f3690a132298.parquet
// /home/jdr0887/workspace/github/everycure-org/matrix/pipelines/matrix/data/cache/normalization_source_rtx_kg2/api=nodenorm-2.3.18/bd3de29210c17166477486acb3c42badc7dda2e8ab28e52dfb29d9070a023d4a.parquet
// /home/jdr0887/workspace/github/everycure-org/matrix/pipelines/matrix/data/cache/normalization_source_rtx_kg2/api=nodenorm-2.3.18/c55af988c9e9fa4dea883a9ef026c6cf92c52305e0df39c0caed65bb88609283.parquet
// /home/jdr0887/workspace/github/everycure-org/matrix/pipelines/matrix/data/cache/normalization_source_rtx_kg2/api=nodenorm-2.3.18/cb405248f1e206b2ab1e6c10367847ac11823db4bd5b12b875c1493641c79fe1.parquet
// /home/jdr0887/workspace/github/everycure-org/matrix/pipelines/matrix/data/cache/normalization_source_rtx_kg2/api=nodenorm-2.3.18/cf173ac5f310735b8e3e53f1f5824e00b6e976198d26a978863d1bdc1b917820.parquet
// /home/jdr0887/workspace/github/everycure-org/matrix/pipelines/matrix/data/cache/normalization_source_rtx_kg2/api=nodenorm-2.3.18/d1fefe3ec524d0390bbca5f960ce668782ea9971b89cedea424696efb2e179ca.parquet
// /home/jdr0887/workspace/github/everycure-org/matrix/pipelines/matrix/data/cache/normalization_source_rtx_kg2/api=nodenorm-2.3.18/e8d6e57d02ee374e67f66b9f1feb26b47776f66e8e32d9009167d74ad0d8ab16.parquet
// /home/jdr0887/workspace/github/everycure-org/matrix/pipelines/matrix/data/cache/normalization_source_rtx_kg2/api=nodenorm-2.3.18/ee42430791c0c2d57b7ebbf6f7a05e6a616ed8649f395f2d39385b3a9d690306.parquet
// /home/jdr0887/workspace/github/everycure-org/matrix/pipelines/matrix/data/cache/normalization_source_rtx_kg2/api=nodenorm-2.3.18/f9651701d6c13487ca033b105726e504e38642dac0e7ef5d06937edfd8abcf13.parquet
// /home/jdr0887/workspace/github/everycure-org/matrix/pipelines/matrix/data/cache/normalization_source_rtx_kg2/api=nodenorm-2.3.18/fbdf2c82a895b77766b0e73ca86c1b566369aa04f1c4b7775c13d69ebd9d9cfd.parquet
