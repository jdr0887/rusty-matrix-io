extern crate env_logger;
extern crate log;

use polars::prelude::*;
use polars::prelude::{coalesce, IntoLazy};
use serde_derive::{Deserialize, Serialize};
use std::{fs, io, path};

#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize, Ord, PartialOrd)]
pub struct Node {
    pub id: String,
    pub category: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize, Ord, PartialOrd)]
pub struct Edge {
    pub subject: String,
    pub predicate: String,
    pub object: String,
    pub primary_knowledge_source: String,
    pub aggregator_knowledge_source: Option<String>,
    pub knowledge_level: String,
    pub agent_type: String,
}

pub fn coalesce_columns(mut df: DataFrame, cols: Vec<&str>) -> DataFrame {
    for col in cols.into_iter() {
        let col_right = format!("{}_right", col);
        if df.get_column_names_str().contains(&col_right.as_str()) {
            df = df.clone().lazy().with_column(coalesce(&[col.into(), col_right.as_str().into()]).alias(col)).collect().unwrap();
            df.drop_in_place(col_right.as_str()).expect("Unable to remove column");
        }
    }
    df
}

pub fn read_edges_file(nodes_path: &path::PathBuf) -> Vec<Edge> {
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

pub fn read_nodes_file(nodes_path: &path::PathBuf) -> Vec<Node> {
    let nodes_file = fs::File::open(nodes_path.clone()).unwrap();
    let reader = io::BufReader::with_capacity(2_usize.pow(14), nodes_file);
    let mut rdr = csv::ReaderBuilder::new().has_headers(true).delimiter(b'\t').from_reader(reader);
    let mut nodes = vec![];
    for result in rdr.deserialize() {
        let record: Node = result.unwrap();
        nodes.push(record.clone());
    }
    nodes
}
