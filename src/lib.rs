extern crate env_logger;
#[macro_use]
extern crate log;

use polars::prelude::*;
use polars::prelude::{coalesce, IntoLazy};
use serde_derive::{Deserialize, Serialize};

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
