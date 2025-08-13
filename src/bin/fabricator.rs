use clap::{Parser, Subcommand};
use humantime::format_duration;
use itertools::{join, Itertools};
use log::{debug, info};
use polars::prelude::*;
use serde_yml::{to_value, Value};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::{BufWriter, Write};
use std::time::Instant;
use std::{error, path};

#[derive(Parser, PartialEq, Debug)]
#[command(author, version, about, long_about = None)]
struct Options {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, PartialEq, Debug)]
enum Commands {
    CreateKGSchemaSnapshot {
        #[clap(short = 'n', long, required = true)]
        nodes: path::PathBuf,

        #[clap(short = 'e', long, required = true)]
        edges: path::PathBuf,
    },
    BuildYAMLFromKGSchemaSnapshot {
        #[arg(short = 'n', long, required = true)]
        nodes: path::PathBuf,

        #[arg(short = 'e', long, required = true)]
        edges: path::PathBuf,

        #[arg(short, long, required = true)]
        schema_snapshot: path::PathBuf,

        #[arg(short, long, default_value_t = 10)]
        limit: i32,

        #[arg(short = 'o', long, required = true)]
        output: path::PathBuf,
    },
    BuildYAMLFromKGX {
        #[arg(short = 'n', long, required = true)]
        nodes: path::PathBuf,

        #[arg(short = 'e', long, required = true)]
        edges: path::PathBuf,

        #[arg(short, long, default_value_t = 10)]
        limit: i32,

        #[arg(short = 'o', long, required = true)]
        output: path::PathBuf,
    },
}

fn main() -> Result<(), Box<dyn error::Error>> {
    let start = Instant::now();
    env_logger::init();

    let options = Options::parse();
    debug!("{:?}", options);

    match &options.command {
        Some(Commands::CreateKGSchemaSnapshot { nodes, edges }) => {
            create_kg_schema_snapshot(nodes, edges).expect("Could not create KG schema snapshot");
        }
        Some(Commands::BuildYAMLFromKGSchemaSnapshot {
            nodes,
            edges,
            schema_snapshot,
            limit,
            output,
        }) => {
            build_yaml_from_kg_schema_snapshot(nodes, edges, schema_snapshot, limit, output).expect("Could not build fabricator yaml from KG schema snapshot");
        }
        Some(Commands::BuildYAMLFromKGX { nodes, edges, limit, output }) => {
            build_yaml_from_kgx(nodes, edges, limit, output).expect("Could not build fabricator yaml from KGX");
        }
        None => {}
    }

    info!("Duration: {}", format_duration(start.elapsed()).to_string());
    Ok(())
}

fn build_yaml_from_kg_schema_snapshot(
    nodes: &path::PathBuf,
    edges: &path::PathBuf,
    schema_snapshot: &path::PathBuf,
    limit: &i32,
    output: &path::PathBuf,
) -> Result<(), Box<dyn error::Error>> {
    Ok(())
}

fn build_yaml_from_kgx(nodes: &path::PathBuf, edges: &path::PathBuf, limit: &i32, output: &path::PathBuf) -> Result<(), Box<dyn error::Error>> {
    let size: f32 = *limit as f32 / 2.0;
    let size: IdxSize = size.round() as IdxSize;

    let edges_df = LazyCsvReader::new(edges)
        .with_separator(b'\t')
        .with_truncate_ragged_lines(true)
        .with_has_header(true)
        .with_ignore_errors(true)
        .finish()
        .unwrap()
        .limit(size)
        .collect()
        .unwrap();

    let edge_id_columns_df = edges_df.clone().lazy().select([col("subject"), col("object")]).collect().unwrap();

    let edge_ids_df = concat(
        [
            edge_id_columns_df.clone().lazy().select([col("subject").alias("id")]),
            edge_id_columns_df.clone().lazy().select([col("object").alias("id")]),
        ],
        UnionArgs::default(),
    )
    .unwrap()
    .unique(None, UniqueKeepStrategy::First)
    .collect()
    .unwrap();

    let selected_edge_ids: Vec<Option<&str>> = edge_ids_df.column("id").unwrap().as_series().unwrap().str().unwrap().into_iter().collect();

    let re = format!("^({})$", join(selected_edge_ids.into_iter().filter_map(|a| a.map(|a| a.to_string())), "|"));

    let nodes_df = LazyCsvReader::new(nodes)
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
    map.insert("nodes".to_string(), create_map(&nodes_df, size).expect("Could not create nodes map"));
    map.insert("edges".to_string(), create_map(&edges_df, size).expect("Could not create edges map"));

    let yaml = serde_yml::to_string(&map).unwrap();
    let output_file = fs::File::create(output).expect("Could not create output file");
    let mut bw = BufWriter::new(output_file);
    bw.write(yaml.as_bytes()).expect("Could not write to output buffer");
    Ok(())
}

fn create_kg_schema_snapshot(nodes: &path::PathBuf, edges: &path::PathBuf) -> Result<(), Box<dyn error::Error>> {
    let edges_df = LazyCsvReader::new(edges)
        .with_separator(b'\t')
        .with_truncate_ragged_lines(true)
        .with_has_header(true)
        .with_ignore_errors(true)
        .finish()
        .unwrap()
        .limit(10)
        .collect()
        .unwrap();

    let mut edges_columns = vec![];

    let edges_column_names = edges_df.get_column_names_str();
    for cn in edges_column_names.iter() {
        let c = edges_df.column(cn).unwrap();
        let sample = c
            .filter(&c.is_not_null())
            .unwrap()
            .cast(&DataType::String)
            .unwrap()
            .sample_n(6, true, true, None)
            .unwrap();

        let cn_values_as_vec: Vec<Option<&str>> = sample.str().unwrap().into_iter().collect();
        let cn_values_as_vec = cn_values_as_vec.into_iter().filter_map(|a| a.map(|s| s.to_string())).dedup().collect_vec();

        edges_columns.push(rusty_matrix_io::Column {
            name: cn.parse().unwrap(),
            datatype: c.dtype().to_string(),
            samples: cn_values_as_vec,
        });
    }

    let mut nodes_columns = vec![];

    let nodes_df = LazyCsvReader::new(nodes)
        .with_separator(b'\t')
        .with_truncate_ragged_lines(true)
        .with_has_header(true)
        .with_ignore_errors(true)
        .finish()
        .unwrap()
        .limit(10)
        .collect()
        .unwrap();

    let nodes_column_names = nodes_df.get_column_names_str();
    for cn in nodes_column_names.iter() {
        let c = nodes_df.column(cn).unwrap();
        let sample = c
            .filter(&c.is_not_null())
            .unwrap()
            .cast(&DataType::String)
            .unwrap()
            .sample_n(6, true, true, None)
            .unwrap();

        let cn_values_as_vec: Vec<Option<&str>> = sample.str().unwrap().into_iter().collect();
        let cn_values_as_vec = cn_values_as_vec.into_iter().filter_map(|a| a.map(|s| s.to_string())).dedup().collect_vec();

        nodes_columns.push(rusty_matrix_io::Column {
            name: cn.parse().unwrap(),
            datatype: c.dtype().to_string(),
            samples: cn_values_as_vec,
        });
    }

    let profile = rusty_matrix_io::KGSchemaSnapshot {
        nodes: nodes_columns,
        edges: edges_columns,
    };

    println!("{}", serde_json::to_string_pretty(&profile).unwrap());
    Ok(())
}

fn create_map(df: &DataFrame, size: IdxSize) -> Result<BTreeMap<String, Value>, Box<dyn error::Error>> {
    let primary_columns = df
        .get_column_names_str()
        .iter()
        .filter_map(|c| if !c.starts_with("_") { Some(c.to_string()) } else { None })
        .collect_vec();
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
        (
            "knowledge_level".to_string(),
            vec![
                "knowledge_assertion",
                "logical_entailment",
                "prediction",
                "statistical_association",
                "observation",
                "not_provided",
            ],
        ),
        (
            "object_direction_qualifier".to_string(),
            vec![
                "knowledge_assertion",
                "logical_entailment",
                "prediction",
                "statistical_association",
                "observation",
                "not_provided",
            ],
        ),
        (
            "subject_direction_qualifier".to_string(),
            vec![
                "knowledge_assertion",
                "logical_entailment",
                "prediction",
                "statistical_association",
                "observation",
                "not_provided",
            ],
        ),
    ]);

    for cn in primary_columns {
        let datatype = nodes_schema.get(&cn).unwrap();
        let mut column_map = BTreeMap::new();

        match cn.as_str() {
            "agent_type" | "knowledge_level" | "object_direction_qualifier" | "subject_direction_qualifier" => {
                let cn_values_as_vec = known_type_values.get(&cn.to_string()).unwrap();
                debug!("column name: {}, datatype: {:?}, values: {:?}", cn, datatype, cn_values_as_vec);
                column_map.insert("type", to_value("generate_values").unwrap());
                column_map.insert("sample_values", to_value(cn_values_as_vec).unwrap());
            }
            _ => {
                if let Some(cn_series) = df.column(&cn).unwrap().as_series() {
                    let cn_values = cn_series.clone();
                    let cn_values_as_vec: Vec<Option<&str>> = cn_values.str().unwrap().into_iter().collect();
                    let mut cn_values_as_vec = cn_values_as_vec.into_iter().filter_map(|a| a.map(|s| s.to_string())).dedup().collect_vec();
                    if cn_values_as_vec.is_empty() {
                        cn_values_as_vec.push("".to_string());
                    }
                    debug!("column name: {}, datatype: {:?}, values: {:?}", cn, datatype, cn_values_as_vec);
                    column_map.insert("type", to_value("generate_values").unwrap());
                    column_map.insert("sample_values", to_value(cn_values_as_vec).unwrap());
                }
            }
        }

        columns_map.insert(cn.clone(), to_value(column_map).unwrap());
    }

    let mut map = BTreeMap::new();
    map.insert("columns".to_string(), to_value(columns_map).unwrap());
    map.insert("num_rows".to_string(), to_value(size * 2).unwrap());

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
