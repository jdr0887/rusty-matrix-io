use clap::Parser;
use humantime::format_duration;
use itertools::{join, Itertools};
use log::{debug, info};
use polars::prelude::*;
use rand::distr::Uniform;
use rand::Rng;
use serde_yml::{to_value, Value};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::{BufWriter, Write};
use std::time::Instant;
use std::{error, path};

#[derive(Parser, PartialEq, Debug)]
#[clap(author, version, about, long_about = None)]
struct Options {
    #[clap(short = 'n', long, required = true)]
    nodes: path::PathBuf,

    #[clap(short = 'e', long, required = true)]
    edges: path::PathBuf,

    #[clap(short = 'o', long, required = true)]
    output: path::PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn error::Error>> {
    let start = Instant::now();
    env_logger::init();

    let options = Options::parse();
    debug!("{:?}", options);

    let edges_df = LazyCsvReader::new(options.edges.clone())
        .with_separator(b'\t')
        .with_truncate_ragged_lines(true)
        .with_has_header(true)
        .with_ignore_errors(true)
        .finish()
        .unwrap()
        .limit(10)
        .collect()
        .unwrap();

    let edge_id_columns_df = edges_df.clone().lazy().select([col("subject"), col("object")]).collect().unwrap();

    let edge_ids_df = concat(
        [edge_id_columns_df.clone().lazy().select([col("subject").alias("id")]), edge_id_columns_df.clone().lazy().select([col("object").alias("id")])],
        UnionArgs::default(),
    )
    .unwrap()
    .unique(None, UniqueKeepStrategy::First)
    .collect()
    .unwrap();

    let selected_edge_ids: Vec<Option<&str>> = edge_ids_df.column("id").unwrap().as_series().unwrap().str().unwrap().into_iter().collect();

    let re = format!("^({})$", join(selected_edge_ids.into_iter().filter_map(|a| a.map(|a| a.to_string())), "|"));

    let nodes_df = LazyCsvReader::new(options.nodes.clone())
        .with_separator(b'\t')
        .with_truncate_ragged_lines(true)
        .with_has_header(true)
        .with_ignore_errors(true)
        .finish()
        .unwrap()
        .filter(col("id").str().contains(lit(re), false))
        .collect()
        .unwrap();

    debug!("nodes_df.shape(): {:?}", nodes_df.shape());

    let mut map = BTreeMap::new();
    map.insert("nodes".to_string(), create_map(&nodes_df).expect("Could not create nodes map"));
    map.insert("edges".to_string(), create_map(&edges_df).expect("Could not create edges map"));

    let yaml = serde_yml::to_string(&map).unwrap();
    let output_file = fs::File::create(options.output).expect("Could not create output file");
    let mut bw = BufWriter::new(output_file);
    bw.write(yaml.as_bytes()).expect("Could not write to output buffer");

    info!("Duration: {}", format_duration(start.elapsed()).to_string());
    Ok(())
}

fn create_map(df: &DataFrame) -> Result<BTreeMap<String, Value>, Box<dyn error::Error>> {
    let primary_columns = df.get_column_names_str().iter().filter_map(|c| if !c.starts_with("_") { Some(c.to_string()) } else { None }).collect_vec();
    info!("primary columns: {:?}", primary_columns);

    let nodes_schema = df.schema();

    let mut columns_map = BTreeMap::new();

    let known_type_values = HashMap::from([
        (
            "agent_type".to_string(),
            vec![
                "manual_agent",
                "automated_agent",
                "data_analysis_pipeline",
                "computational_model",
                "text_mining_agent",
                "image_processing_agent",
                "manual_validation_of_automated_agent",
                "not_provided",
            ],
        ),
        ("knowledge_level".to_string(), vec!["knowledge_assertion", "logical_entailment", "prediction", "statistical_association", "observation", "not_provided"]),
        ("object_direction_qualifier".to_string(), vec!["knowledge_assertion", "logical_entailment", "prediction", "statistical_association", "observation", "not_provided"]),
    ]);

    for cn in primary_columns {
        let datatype = nodes_schema.get(&cn).unwrap();
        let mut column_map = BTreeMap::new();

        match cn.as_str() {
            "agent_type" | "knowledge_level" | "object_direction_qualifier" => {
                let cn_values_as_vec = known_type_values.get(&cn.to_string()).unwrap();
                debug!("column name: {}, datatype: {:?}, values: {:?}", cn, datatype, cn_values_as_vec);
                column_map.insert("type", to_value("generate_values").unwrap());
                column_map.insert("sample_values", to_value(cn_values_as_vec).unwrap());
            }
            _ => {
                if let Some(cn_series) = df.column(&cn).unwrap().unique().unwrap().as_series() {
                    let cn_values = cn_series.clone();
                    let cn_values_as_vec: Vec<Option<&str>> = cn_values.str().unwrap().into_iter().collect();
                    let cn_values_as_vec = cn_values_as_vec.into_iter().filter_map(|a| a.map(|s| s.to_string())).collect_vec();
                    debug!("column name: {}, datatype: {:?}, values: {:?}", cn, datatype, cn_values_as_vec);
                    column_map.insert("type", to_value("generate_values").unwrap());
                    column_map.insert("sample_values", to_value(cn_values_as_vec).unwrap());
                } else {
                    column_map.insert("type", to_value("generate_values").unwrap());
                    column_map.insert("sample_values", to_value(vec!["".to_string()]).unwrap());
                }
            }
        }

        columns_map.insert(cn.clone(), to_value(column_map).unwrap());
    }

    let mut map = BTreeMap::new();
    map.insert("columns".to_string(), to_value(columns_map).unwrap());
    map.insert("num_rows".to_string(), to_value(500).unwrap());

    Ok(map)
}

#[cfg(test)]
mod test {
    use serde_yml::to_value;
    use std::collections::BTreeMap;

    #[test]
    fn scratch() {
        let mut category_column = BTreeMap::new();
        category_column.insert("type", to_value("generate_random_arrays").unwrap());
        category_column.insert("delimiter", to_value("|").unwrap());
        category_column.insert("sample_values", to_value(vec!["biolink:Entity", "biolink:ThingWithTaxon"]).unwrap());

        let mut id_column = BTreeMap::new();
        id_column.insert("type", to_value("generate_unique_id").unwrap());
        id_column.insert("prefix", to_value("ROBO:").unwrap());
        id_column.insert("id_length", to_value(8).unwrap());

        let mut columns_map = BTreeMap::new();
        columns_map.insert("id".to_string(), to_value(id_column).unwrap());
        columns_map.insert("category".to_string(), to_value(category_column).unwrap());

        let mut nodes_map = BTreeMap::new();
        nodes_map.insert("columns".to_string(), to_value(columns_map).unwrap());
        nodes_map.insert("num_rows".to_string(), to_value(500).unwrap());

        let mut map = BTreeMap::new();
        map.insert("nodes".to_string(), nodes_map);

        let yaml = serde_yml::to_string(&map).unwrap();
        println!("Serialized YAML:\n{}", yaml);
    }
}
