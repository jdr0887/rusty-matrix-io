use clap::Parser;
use humantime::format_duration;
use log::{debug, info};
use polars::prelude::*;
use std::time::Instant;
use std::{error, fs, path};

#[derive(Parser, PartialEq, Debug)]
#[clap(author, version, about, long_about = None)]
struct Options {
    #[clap(short = 'a', long, required = true)]
    drug_features: path::PathBuf,

    #[clap(short = 'b', long, required = true)]
    disease_features: path::PathBuf,

    #[clap(short = 'n', long, required = true)]
    nodes: path::PathBuf,

    #[clap(short = 'o', long, required = true)]
    nodes_output: path::PathBuf,
}

fn main() -> Result<(), Box<dyn error::Error>> {
    let start = Instant::now();
    env_logger::init();

    let options = Options::parse();
    debug!("{:?}", options);

    let join_args = JoinArgs::new(JoinType::Full).with_coalesce(JoinCoalesce::CoalesceColumns);
    let mut main_df = df!("node_index" => &Vec::<String>::new()).unwrap();

    // node_index,node_id,node_type,node_name,node_source
    let nodes_df = LazyCsvReader::new(options.nodes.clone())
        .with_infer_schema_length(Some(0))
        .with_ignore_errors(true)
        .with_truncate_ragged_lines(true)
        .with_has_header(true)
        .finish()
        .unwrap();

    main_df = main_df
        .clone()
        .lazy()
        .join(nodes_df.clone(), [col("node_index")], [col("node_index")], join_args.clone())
        .collect()
        .expect("Could not join");
    debug!("column names: {:?}", main_df.get_column_names_str());

    // node_index,description,half_life,indication,mechanism_of_action,protein_binding,pharmacodynamics,state,atc_1,atc_2,atc_3,atc_4,category,group,pathway,molecular_weight,tpsa,clogp
    let drug_features_df = LazyCsvReader::new(options.drug_features.clone())
        .with_infer_schema_length(Some(0))
        .with_ignore_errors(true)
        .with_truncate_ragged_lines(true)
        .with_has_header(true)
        .finish()
        .unwrap();
    main_df = main_df
        .clone()
        .lazy()
        .join(drug_features_df.clone(), [col("node_index")], [col("node_index")], join_args.clone())
        .collect()
        .expect("Could not join");
    main_df = rusty_matrix_io::coalesce_columns(main_df, vec!["node_index"]);
    debug!("column names: {:?}", main_df.get_column_names_str());
    debug!("adding drug features: {:?}", main_df.head(None));

    // node_index,mondo_id,mondo_name,group_id_bert,group_name_bert,mondo_definition,umls_description,orphanet_definition,orphanet_prevalence,orphanet_epidemiology,orphanet_clinical_description,orphanet_management_and_treatment,mayo_symptoms,mayo_causes,mayo_risk_factors,mayo_complications,mayo_prevention,mayo_see_doc
    let disease_features_df = LazyCsvReader::new(options.disease_features.clone())
        .with_infer_schema_length(Some(0))
        .with_ignore_errors(true)
        .with_truncate_ragged_lines(true)
        .with_has_header(true)
        .finish()
        .unwrap();
    main_df = main_df
        .clone()
        .lazy()
        .join(disease_features_df.clone(), [col("node_index")], [col("node_index")], join_args.clone())
        .collect()
        .expect("Could not join");
    main_df = rusty_matrix_io::coalesce_columns(main_df, vec!["node_index"]);
    debug!("column names: {:?}", main_df.get_column_names_str());
    debug!("adding disease features: {:?}", main_df.head(None));

    main_df = main_df
        .clone()
        .lazy()
        .with_column(
            when(col("node_source").str().contains_literal(lit("NCBI")))
                .then(concat_str([col("node_source"), col("node_id")], "Gene:", true))
                .otherwise(col("node_source"))
                .alias("node_source"),
        )
        .with_column(
            when(col("node_source").str().contains_literal(lit("REACTOME")))
                .then(concat_str([lit("REACT"), col("node_id")], ":", true))
                .otherwise(col("node_source"))
                .alias("node_source"),
        )
        .with_column(
            when(col("node_source").str().contains(lit("^(HPO|MONDO|UBERON)$"), true))
                .then(concat_str([col("node_source"), col("node_id").str().pad_start(7, '0')], ":", true))
                .otherwise(col("node_source"))
                .alias("node_source"),
        )
        .with_column(
            when(col("node_source").str().contains(lit("^(CTD|GO|DrugBank)$"), true))
                .then(concat_str([col("node_source"), col("node_id")], ":", true))
                .otherwise(col("node_source"))
                .alias("node_source"),
        )
        .with_column(
            when(col("node_source").str().contains(lit("MONDO_grouped"), true))
                .then(concat_str([lit("MONDO"), col("mondo_id").str().pad_start(7, '0')], ":", true))
                .otherwise(col("node_source"))
                .alias("node_source"),
        )
        .with_column(
            when(col("node_type").str().contains_literal(lit("exposure")))
                .then(lit("biolink:ChemicalExposure"))
                .otherwise(col("node_type"))
                .alias("node_type"),
        )
        .with_column(
            when(col("node_type").str().contains_literal(lit("effect/phenotype")))
                .then(lit("biolink:PhenotypicFeature"))
                .otherwise(col("node_type"))
                .alias("node_type"),
        )
        .with_column(
            when(col("node_type").str().contains_literal(lit("molecular_function")))
                .then(lit("biolink:MolecularActivity"))
                .otherwise(col("node_type"))
                .alias("node_type"),
        )
        .with_column(
            when(col("node_type").str().contains_literal(lit("cellular_component")))
                .then(lit("biolink:CellularComponent"))
                .otherwise(col("node_type"))
                .alias("node_type"),
        )
        .with_column(
            when(col("node_type").str().contains_literal(lit("biological_process")))
                .then(lit("biolink:BiologicalProcess"))
                .otherwise(col("node_type"))
                .alias("node_type"),
        )
        .with_column(
            when(col("node_type").str().contains_literal(lit("pathway")))
                .then(lit("biolink:Pathway"))
                .otherwise(col("node_type"))
                .alias("node_type"),
        )
        .with_column(
            when(col("node_type").str().contains_literal(lit("gene/protein")))
                .then(lit("biolink:Gene"))
                .otherwise(col("node_type"))
                .alias("node_type"),
        )
        .with_column(
            when(col("node_type").str().contains_literal(lit("disease")))
                .then(lit("biolink:Disease"))
                .otherwise(col("node_type"))
                .alias("node_type"),
        )
        .with_column(
            when(col("node_type").str().contains_literal(lit("drug")))
                .then(lit("biolink:SmallMolecule"))
                .otherwise(col("node_type"))
                .alias("node_type"),
        )
        .with_column(
            when(col("node_type").str().contains_literal(lit("anatomy")))
                .then(lit("biolink:GrossAnatomicalStructure"))
                .otherwise(col("node_type"))
                .alias("node_type"),
        )
        .drop(["node_id", "node_index"])
        .rename(["node_source", "node_name", "category"], ["id", "name", "drug_category"], true)
        .rename(["node_type"], ["category"], true)
        .collect()
        .unwrap();

    let mut file = fs::File::create(options.nodes_output.as_path()).unwrap();
    CsvWriter::new(&mut file).with_separator(b'\t').finish(&mut main_df).unwrap();

    info!("Duration: {}", format_duration(start.elapsed()).to_string());
    Ok(())
}
