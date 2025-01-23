extern crate env_logger;
extern crate log;

use itertools::Itertools;
use serde_derive::{Deserialize, Serialize};
use std::io::BufRead;
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
