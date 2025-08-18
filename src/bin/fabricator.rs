use clap::{Parser, Subcommand};
use humantime::format_duration;
use indexmap::IndexMap;
use itertools::{any, join, Itertools};
use lazy_static::lazy_static;
use log::{debug, info};
use ordered_float::OrderedFloat;
use polars::prelude::*;
use serde_yml::{to_value, Value};
use std::collections::HashMap;
use std::fs;
use std::io::{BufWriter, Write};
use std::time::Instant;
use std::{error, path};

lazy_static! {
    pub static ref SEPARATOR: String = format!("{}", char::from_u32(0x0000001F).unwrap()).to_string();
    pub static ref KNOWN_TYPE_VALUES: HashMap<String, Vec<String>> = HashMap::from([
        (
            "agent_type".into(),
            vec![
                "manual_agent".into(),
                "automated_agent".into(),
                "data_analysis_pipeline".into(),
                "computational_model".into(),
                "text_mining_agent".into(),
                "image_processing_agent".into(),
                "manual_validation_of_automated_agent".into(),
                "not_provided".into(),
            ],
        ),
        (
            "knowledge_level".into(),
            vec![
                "knowledge_assertion".into(),
                "logical_entailment".into(),
                "prediction".into(),
                "statistical_association".into(),
                "observation".into(),
                "not_provided".into(),
            ],
        ),
        (
            "object_direction_qualifier".into(),
            vec![
                "knowledge_assertion".into(),
                "logical_entailment".into(),
                "prediction".into(),
                "statistical_association".into(),
                "observation".into(),
                "not_provided".into(),
            ],
        ),
        (
            "subject_direction_qualifier".into(),
            vec![
                "knowledge_assertion".into(),
                "logical_entailment".into(),
                "prediction".into(),
                "statistical_association".into(),
                "observation".into(),
                "not_provided".into(),
            ],
        ),
    ]);
}

#[derive(Parser, PartialEq, Debug)]
#[command(author, version, about, long_about = None)]
struct Options {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, PartialEq, Debug)]
enum Commands {
    CreateKGSchemaSnapshot {
        #[arg(short = 'n', long, required = true)]
        nodes: path::PathBuf,

        #[arg(short = 'e', long, required = true)]
        edges: path::PathBuf,

        #[arg(short = 'x', long)]
        nodes_columns_exclusions: Option<Vec<String>>,

        #[arg(short = 'y', long)]
        edges_columns_exclusions: Option<Vec<String>>,
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
        Some(Commands::CreateKGSchemaSnapshot {
            nodes,
            edges,
            nodes_columns_exclusions,
            edges_columns_exclusions,
        }) => {
            create_kg_schema_snapshot(nodes, edges, nodes_columns_exclusions, edges_columns_exclusions).expect("Could not create KG schema snapshot");
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
    let schema_snapshot_content = fs::read_to_string(schema_snapshot).expect("Could not read schema snapshot");
    let snapshot: rusty_matrix_io::KGSchemaSnapshot = serde_json::from_str(&*schema_snapshot_content).expect("Could not deserialize schema snapshot");

    let usable_edges_columns = snapshot.edges.iter().map(|a| col(a.name.clone())).collect_vec();

    let size: f32 = *limit as f32 / 2.0;
    let size: IdxSize = size.round() as IdxSize;

    let edges_df = LazyCsvReader::new(edges)
        .with_separator(b'\t')
        .with_truncate_ragged_lines(true)
        .with_has_header(true)
        .with_ignore_errors(true)
        .finish()
        .unwrap()
        .select(usable_edges_columns)
        .limit(size)
        .collect()
        .unwrap();

    let edge_id_columns_df = edges_df
        .clone()
        .lazy()
        .select([col("subject"), col("object")])
        .collect()
        .expect("Could not get the subject or object columns");

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

    let usable_nodes_columns = snapshot.nodes.iter().map(|a| col(a.name.clone())).collect_vec();
    let nodes_df = LazyCsvReader::new(nodes)
        .with_separator(b'\t')
        .with_truncate_ragged_lines(true)
        .with_has_header(true)
        .with_ignore_errors(true)
        .finish()
        .unwrap()
        .select(usable_nodes_columns)
        .filter(col("id").str().contains(lit(re), false))
        .collect()
        .unwrap();

    debug!("nodes_df.shape(): {:?}", nodes_df.shape());

    let mut map = IndexMap::new();
    map.insert("nodes".to_string(), create_nodes_map(&nodes_df, size).expect("Could not create nodes map"));
    map.insert("edges".to_string(), create_edges_map(&edges_df, size).expect("Could not create edges map"));

    let yaml = serde_yml::to_string(&map).unwrap();
    let output_file = fs::File::create(output).expect("Could not create output file");
    let mut bw = BufWriter::new(output_file);
    bw.write(yaml.as_bytes()).expect("Could not write to output buffer");
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

    let mut map = IndexMap::new();

    map.insert("nodes".to_string(), create_nodes_map(&nodes_df, size).expect("Could not create nodes map"));
    map.insert("edges".to_string(), create_edges_map(&edges_df, size).expect("Could not create edges map"));

    let yaml = serde_yml::to_string(&map).unwrap();
    let output_file = fs::File::create(output).expect("Could not create output file");
    let mut bw = BufWriter::new(output_file);
    bw.write(yaml.as_bytes()).expect("Could not write to output buffer");
    Ok(())
}

fn create_kg_schema_snapshot(
    nodes: &path::PathBuf,
    edges: &path::PathBuf,
    nodes_prefix_exclusions: &Option<Vec<String>>,
    edges_prefix_exclusions: &Option<Vec<String>>,
) -> Result<(), Box<dyn error::Error>> {
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

    let edges_column_names = match edges_prefix_exclusions {
        Some(prefixes) => edges_df
            .get_column_names_str()
            .iter()
            .filter(|a| !any(prefixes, |b| a.starts_with(b)))
            .map(|a| a.to_string())
            .collect_vec(),
        None => edges_df.get_column_names_str().iter().map(|a| a.to_string()).collect_vec(),
    };

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

    let nodes_column_names = match nodes_prefix_exclusions {
        Some(prefixes) => nodes_df
            .get_column_names_str()
            .iter()
            .filter(|a| !any(prefixes, |b| a.starts_with(b)))
            .map(|a| a.to_string())
            .collect_vec(),
        None => nodes_df.get_column_names_str().iter().map(|a| a.to_string()).collect_vec(),
    };

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

fn create_nodes_map(df: &DataFrame, size: IdxSize) -> Result<IndexMap<String, Value>, Box<dyn error::Error>> {
    let primary_columns = df
        .get_column_names_str()
        .iter()
        .filter_map(|c| if !c.starts_with("_") { Some(c.to_string()) } else { None })
        .collect_vec();
    info!("primary columns: {:?}", primary_columns);

    let schema = df.schema();

    let mut columns_map = IndexMap::new();

    for cn in primary_columns {
        let datatype = schema.get(&cn).unwrap();
        let mut column_map = IndexMap::new();

        match cn.as_str() {
            "id" => {
                if let Some(cn_series) = df.column(&cn).unwrap().as_series() {
                    let cn_values = cn_series.clone().cast(&DataType::String).unwrap();
                    let cn_values_as_vec: Vec<Option<&str>> = cn_values.str().unwrap().into_iter().collect_vec();
                    let mut cn_values_as_vec = cn_values_as_vec
                        .into_iter()
                        .filter_map(|a| a.map(|s| s.to_string()))
                        .map(|a| a.split_once(":").unwrap().0.to_string())
                        .dedup()
                        .collect_vec();
                    if cn_values_as_vec.is_empty() {
                        cn_values_as_vec.push("".to_string());
                    }
                    debug!("column name: {}, datatype: {:?}, values: {:?}", cn, datatype, cn_values_as_vec);
                    debug!("column name: {}, datatype: {:?}, values: {:?}", cn, datatype, cn_values_as_vec);
                    column_map.insert("type", to_value("generate_unique_id").unwrap());
                    column_map.insert("prefixes", to_value(cn_values_as_vec).unwrap());
                }
            }
            _ => {
                if let Some(cn_series) = df.column(&cn).unwrap().as_series() {
                    match cn_series.dtype() {
                        DataType::Float64 => {
                            let cn_values = cn_series.clone();
                            let cn_values_as_vec: Vec<Option<f64>> = cn_values.f64().unwrap().into_iter().collect();
                            let cn_values_as_vec = cn_values_as_vec
                                .into_iter()
                                .filter_map(|a| a)
                                .map(|a| OrderedFloat::from(a))
                                .sorted()
                                .dedup()
                                .map(|a| a.into_inner())
                                .collect_vec();
                            debug!("column name: {}, datatype: {:?}, values: {:?}", cn, datatype, cn_values_as_vec);
                            column_map.insert("type", to_value("generate_values").unwrap());
                            column_map.insert("sample_values", to_value(cn_values_as_vec).unwrap());
                        }
                        _ => {
                            let cn_values = cn_series.clone().cast(&DataType::String).unwrap();
                            let cn_values_as_vec: Vec<Option<&str>> = cn_values.str().unwrap().into_iter().collect();
                            let mut cn_values_as_vec = cn_values_as_vec
                                .into_iter()
                                .filter_map(|a| a.map(|s| s.to_string()))
                                .sorted()
                                .dedup()
                                .collect_vec();
                            if cn_values_as_vec.is_empty() {
                                cn_values_as_vec.push("".to_string());
                            }
                            debug!("column name: {}, datatype: {:?}, values: {:?}", cn, datatype, cn_values_as_vec);
                            column_map.insert("type", to_value("generate_values").unwrap());
                            column_map.insert("sample_values", to_value(cn_values_as_vec).unwrap());
                        }
                    }
                }
            }
        }

        columns_map.insert(cn.clone(), to_value(column_map).unwrap());
    }

    let mut map = IndexMap::new();
    map.insert("columns".to_string(), to_value(columns_map).unwrap());
    map.insert("num_rows".to_string(), to_value(size * 2).unwrap());

    Ok(map)
}

fn create_edges_map(df: &DataFrame, size: IdxSize) -> Result<IndexMap<String, Value>, Box<dyn error::Error>> {
    let primary_columns = df
        .get_column_names_str()
        .iter()
        .filter_map(|c| if !c.starts_with("_") { Some(c.to_string()) } else { None })
        .collect_vec();
    info!("primary columns: {:?}", primary_columns);

    let schema = df.schema();

    let mut columns_map = IndexMap::new();

    for cn in primary_columns {
        let datatype = schema.get(&cn).unwrap();
        let mut column_map = IndexMap::new();

        match cn.as_str() {
            "subject" | "object" => {
                column_map.insert("type", to_value("copy_column").unwrap());
                column_map.insert("source_column", to_value("nodes.id").unwrap());
                column_map.insert("seed", to_value(590590).unwrap());
                let mut sample_map = IndexMap::new();
                sample_map.insert("num_rows", to_value("@edges.num_rows").unwrap());
                column_map.insert("sample", to_value(sample_map).unwrap());
            }
            "agent_type" | "knowledge_level" | "object_direction_qualifier" | "subject_direction_qualifier" => {
                let cn_values_as_vec = KNOWN_TYPE_VALUES.get(&cn.to_string()).unwrap();
                debug!("column name: {}, datatype: {:?}, values: {:?}", cn, datatype, cn_values_as_vec);
                column_map.insert("type", to_value("generate_values").unwrap());
                column_map.insert("sample_values", to_value(cn_values_as_vec).unwrap());
            }
            _ => {
                if let Some(cn_series) = df.column(&cn).unwrap().as_series() {
                    match cn_series.dtype() {
                        DataType::Float64 => {
                            let cn_values = cn_series.clone();
                            let cn_values_as_vec: Vec<Option<f64>> = cn_values.f64().unwrap().into_iter().collect();
                            let cn_values_as_vec = cn_values_as_vec
                                .into_iter()
                                .filter_map(|a| a)
                                .map(|a| OrderedFloat::from(a))
                                .sorted()
                                .dedup()
                                .map(|a| a.into_inner())
                                .collect_vec();
                            debug!("column name: {}, datatype: {:?}, values: {:?}", cn, datatype, cn_values_as_vec);
                            column_map.insert("type", to_value("generate_values").unwrap());
                            column_map.insert("sample_values", to_value(cn_values_as_vec).unwrap());
                        }
                        _ => {
                            let cn_values = cn_series.clone().cast(&DataType::String).unwrap();
                            let cn_values_as_vec: Vec<Option<&str>> = cn_values.str().unwrap().into_iter().collect();
                            let mut cn_values_as_vec = cn_values_as_vec
                                .into_iter()
                                .filter_map(|a| a.map(|s| s.to_string()))
                                .sorted()
                                .dedup()
                                .collect_vec();
                            if cn_values_as_vec.is_empty() {
                                cn_values_as_vec.push("".to_string());
                            }
                            debug!("column name: {}, datatype: {:?}, values: {:?}", cn, datatype, cn_values_as_vec);
                            column_map.insert("type", to_value("generate_values").unwrap());
                            column_map.insert("sample_values", to_value(cn_values_as_vec).unwrap());
                        }
                    }
                }
            }
        }

        columns_map.insert(cn.clone(), to_value(column_map).unwrap());
    }

    let mut map = IndexMap::new();
    map.insert("columns".to_string(), to_value(columns_map).unwrap());
    map.insert("num_rows".to_string(), to_value(size * 2).unwrap());

    Ok(map)
}

#[cfg(test)]
mod test {
    use indexmap::IndexMap;
    use serde_yml::to_value;
    use std::collections::BTreeMap;

    #[test]
    fn scratch() {
        let mut category_column = IndexMap::new();
        category_column.insert("type", to_value("generate_random_arrays").unwrap());
        category_column.insert("delimiter", to_value("|").unwrap());
        category_column.insert("sample_values", to_value(vec!["biolink:Entity", "biolink:ThingWithTaxon"]).unwrap());

        let mut id_column = IndexMap::new();
        id_column.insert("type", to_value("generate_unique_id").unwrap());
        id_column.insert("prefix", to_value("ROBO:").unwrap());
        id_column.insert("id_length", to_value(8).unwrap());

        let mut columns_map = IndexMap::new();
        columns_map.insert("id".to_string(), to_value(id_column).unwrap());
        columns_map.insert("category".to_string(), to_value(category_column).unwrap());

        let mut nodes_map = IndexMap::new();
        nodes_map.insert("columns".to_string(), to_value(columns_map).unwrap());
        nodes_map.insert("num_rows".to_string(), to_value(500).unwrap());

        let mut map = IndexMap::new();
        map.insert("nodes".to_string(), nodes_map);

        let yaml = serde_yml::to_string(&map).unwrap();
        println!("Serialized YAML:\n{}", yaml);
    }
}
