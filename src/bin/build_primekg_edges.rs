use clap::Parser;
use humantime::format_duration;
use log::{debug, info};
use polars::prelude::DataType::String;
use polars::prelude::*;
use std::time::Instant;
use std::{error, fs, path};

#[derive(Parser, PartialEq, Debug)]
#[clap(author, version, about, long_about = None)]
struct Options {
    #[clap(short = 'i', long, required = true)]
    kg: path::PathBuf,

    #[clap(short = 'o', long, required = true)]
    edges_output: path::PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn error::Error>> {
    let start = Instant::now();
    env_logger::init();

    let options = Options::parse();
    debug!("{:?}", options);

    // relation,display_relation,x_index,x_id,x_type,x_name,x_source,y_index,y_id,y_type,y_name,y_source
    let mut edges_df = LazyCsvReader::new(options.kg.clone())
        .with_infer_schema_length(Some(0))
        .with_ignore_errors(true)
        .with_truncate_ragged_lines(true)
        .with_has_header(true)
        .finish()
        .unwrap()
        .collect()
        .unwrap();

    edges_df = edges_df
        .clone()
        .lazy()
        .with_columns([
            lit("prediction").alias("knowledge_level"),
            lit("computational_model").alias("agent_type"),
            lit(LiteralValue::Null).cast(String).alias("primary_knowledge_source"),
            lit(LiteralValue::Null).cast(String).alias("aggregator_knowledge_source"),
            lit(LiteralValue::Null).cast(String).alias("original_subject"),
            lit(LiteralValue::Null).cast(String).alias("original_object"),
            lit(LiteralValue::Null).cast(String).alias("publications"),
            lit(LiteralValue::Null).cast(String).alias("subject_aspect_qualifier"),
            lit(LiteralValue::Null).cast(String).alias("subject_direction_qualifier"),
            lit(LiteralValue::Null).cast(String).alias("object_aspect_qualifier"),
            lit(LiteralValue::Null).cast(String).alias("object_direction_qualifier"),
            lit(LiteralValue::Null).cast(String).alias("upstream_data_source"),
        ])
        .with_column(
            when(col("x_source").str().contains_literal(lit("NCBI"))).then(concat_str([col("x_source"), col("x_id")], "Gene:", true)).otherwise(col("x_source")).alias("subject"),
        )
        .with_column(
            when(col("x_source").str().contains(lit("REACTOME"), true)).then(concat_str([lit("REACT"), col("x_id")], ":", true)).otherwise(col("subject")).alias("subject"),
        )
        .with_column(
            when(col("x_source").str().contains(lit("^(HPO|MONDO|UBERON)$"), true))
                .then(concat_str([col("x_source"), col("x_id").str().pad_start(7, '0')], ":", true))
                .otherwise(col("subject"))
                .alias("subject"),
        )
        .with_column(
            when(col("x_source").str().contains(lit("^(CTD|GO|DrugBank)$"), true))
                .then(concat_str([col("x_source"), col("x_id")], ":", true))
                .otherwise(col("subject"))
                .alias("subject"),
        )
        .with_column(
            when(col("y_source").str().contains_literal(lit("NCBI"))).then(concat_str([col("y_source"), col("y_id")], "Gene:", true)).otherwise(col("x_source")).alias("object"),
        )
        .with_column(when(col("y_source").str().contains(lit("REACTOME"), true)).then(concat_str([lit("REACT"), col("y_id")], ":", true)).otherwise(col("object")).alias("object"))
        .with_column(
            when(col("y_source").str().contains(lit("^(HPO|MONDO|UBERON)$"), true))
                .then(concat_str([col("y_source"), col("y_id").str().pad_start(7, '0')], ":", true))
                .otherwise(col("object"))
                .alias("object"),
        )
        .with_column(
            when(col("y_source").str().contains(lit("^(CTD|GO|DrugBank)$"), true))
                .then(concat_str([col("y_source"), col("y_id")], ":", true))
                .otherwise(col("object"))
                .alias("object"),
        )
        .drop(["x_index", "x_id", "x_type", "x_name", "x_source", "y_index", "y_id", "y_type", "y_name", "y_source"])
        .rename(["relation"], ["predicate"], true)
        .with_column(
            // bioprocess_protein	interacts with
            // cellcomp_protein	interacts with
            // exposure_bioprocess	interacts with
            // exposure_cellcomp	interacts with
            // exposure_molfunc	interacts with
            // exposure_protein	interacts with
            // molfunc_protein	interacts with
            // pathway_protein	interacts with
            when(col("predicate").str().contains(
                lit("^(bioprocess_protein|cellcomp_protein|exposure_bioprocess|exposure_cellcomp|exposure_molfunc|exposure_protein|molfunc_protein|pathway_protein)$"),
                true,
            ))
            .then(lit("biolink:interacts_with"))
            .otherwise(col("predicate"))
            .alias("predicate"),
        )
        .with_column(
            // disease_protein	associated with
            // phenotype_protein	associated with
            when(col("predicate").str().contains(lit("^(disease_protein|phenotype_protein)$"), true))
                .then(lit("biolink:associated_with"))
                .otherwise(col("predicate"))
                .alias("predicate"),
        )
        .with_column(
            // anatomy_anatomy	parent-child
            // bioprocess_bioprocess	parent-child
            // cellcomp_cellcomp	parent-child
            // disease_disease	parent-child
            // exposure_exposure	parent-child
            // molfunc_molfunc	parent-child
            // pathway_pathway	parent-child
            // phenotype_phenotype	parent-child
            when(col("predicate").str().contains(
                lit("^(anatomy_anatomy|bioprocess_bioprocess|cellcomp_cellcomp|disease_disease|exposure_exposure|molfunc_molfunc|pathway_pathway|phenotype_phenotype)$"),
                true,
            ))
            .then(lit("biolink:superclass_of"))
            .otherwise(col("predicate"))
            .alias("predicate"),
        )
        .with_column(
            // protein_protein	ppi
            when(col("predicate").str().contains_literal(lit("protein_protein"))).then(lit("biolink:interacts_with")).otherwise(col("predicate")).alias("predicate"),
        )
        .with_column(
            // drug_effect	side effect
            when(col("predicate").str().contains_literal(lit("drug_effect"))).then(lit("biolink:has_side_effect")).otherwise(col("predicate")).alias("predicate")
        )
        .with_column(
            // contraindication	contraindication
            when(col("predicate").str().contains_literal(lit("contraindication"))).then(lit("biolink:contraindicated_in")).otherwise(col("predicate")).alias("predicate")
        )
        // .with_column(when(col("predicate").str().contains_literal(lit("anatomy_protein_absent"))).then(lit("biolink:expresses")).otherwise(col("predicate")).alias("predicate"))
        // .with_column(when(col("predicate").str().contains_literal(lit("anatomy_protein_present"))).then(lit("biolink:expressed_in")).otherwise(col("predicate")).alias("predicate"))
        // .with_column(when(col("predicate").str().contains_literal(lit("disease_phenotype_negative"))).then(lit("biolink:expresses")).otherwise(col("predicate")).alias("predicate"))
        // .with_column(
        //     when(col("predicate").str().contains_literal(lit("disease_phenotype_positive"))).then(lit("biolink:expressed_in")).otherwise(col("predicate")).alias("predicate"),
        // )
        // .with_column(
        //     // drug_protein	enzyme
        //     when(col("predicate").str().contains_literal(lit("drug_protein"))).then(lit("biolink:directly_physically_interacts_with")).otherwise(col("predicate")).alias("predicate")
        // )
        // .with_column(
        //     // drug_protein	target
        //     when(col("predicate").str().contains_literal(lit("drug_protein"))).then(lit("biolink:directly_physically_interacts_with")).otherwise(col("predicate")).alias("predicate")
        // )
        // .with_column(
        //     // exposure_disease	linked to
        //     when(col("predicate").str().contains_literal(lit("exposure_disease"))).then(lit("biolink:causes_increase???")).otherwise(col("predicate")).alias("predicate")
        // )
        .with_column(
            // indication	indication
            when(col("predicate").str().contains_literal(lit("indication"))).then(lit("biolink:indicated_in")).otherwise(col("predicate")).alias("predicate")
        )
        .with_column(
            // off-label use	off-label use
            when(col("predicate").str().contains_literal(lit("off-label use"))).then(lit("biolink:applied_to_treat")).otherwise(col("predicate")).alias("predicate")
        )
        .collect()
        .unwrap();

    // anatomy_protein_absent	expression absent -> biolink:subject_aspect_qualifier  -> biolink:subject_direction_qualifier -> decreased
    // anatomy_protein_present	expression present -> biolink:subject_aspect_qualifier  -> biolink:subject_direction_qualifier -> increased
    // disease_phenotype_negative	phenotype absent
    // disease_phenotype_positive	phenotype present
    // drug_drug	synergistic interaction -> causes increased effect/potency
    // drug_protein	carrier
    // drug_protein	transporter
    // exposure_disease	linked to

    let mut file = fs::File::create(options.edges_output.as_path()).unwrap();
    CsvWriter::new(&mut file).with_separator(b'\t').finish(&mut edges_df).unwrap();

    info!("Duration: {}", format_duration(start.elapsed()).to_string());
    Ok(())
}
