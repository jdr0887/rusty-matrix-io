use clap::Parser;
use humantime::format_duration;
use log::{debug, info};
use polars::prelude::AnyValue::Null;
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

fn main() -> Result<(), Box<dyn error::Error>> {
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

    // need to explode MONDO_grouped
    let mondo_grouped_exploded = |edges_df: DataFrame, x_or_y: String| -> DataFrame {
        let source = format!("{}_source", x_or_y);
        let id = format!("{}_id", x_or_y);
        let source_mondo_grouped_df = edges_df
            .clone()
            .lazy()
            .filter(col(source.as_str()).eq(lit("MONDO_grouped")))
            .with_column(col(id.as_str()).str().split(lit("_")).alias(id.as_str()))
            .with_column(
                when(col(source.as_str()).eq(lit("MONDO_grouped")))
                    .then(lit("MONDO"))
                    .otherwise(col(source.as_str()))
                    .alias(source.as_str()),
            )
            .explode([col(id)]);

        let source_not_mondo_grouped = edges_df.clone().lazy().filter(col(source.as_str()).eq(lit("MONDO_grouped")).not());

        concat([source_mondo_grouped_df, source_not_mondo_grouped], UnionArgs::default())
            .unwrap()
            .collect()
            .unwrap()
    };

    edges_df = mondo_grouped_exploded(edges_df, "x".into());
    edges_df = mondo_grouped_exploded(edges_df, "y".into());

    edges_df = edges_df
        .clone()
        .lazy()
        .with_columns([
            lit("knowledge_assertion").alias("knowledge_level"),
            lit("not_provided").alias("agent_type"),
            lit("infores:primekg").alias("primary_knowledge_source"),
            lit(LiteralValue::untyped_null()).cast(DataType::String).alias("aggregator_knowledge_source"),
            lit(LiteralValue::untyped_null()).cast(DataType::String).alias("original_subject"),
            lit(LiteralValue::untyped_null()).cast(DataType::String).alias("original_object"),
            lit(LiteralValue::untyped_null()).cast(DataType::String).alias("negated"),
            lit(LiteralValue::untyped_null()).cast(DataType::String).alias("publications"),
            lit(LiteralValue::untyped_null()).cast(DataType::String).alias("subject_aspect_qualifier"),
            lit(LiteralValue::untyped_null()).cast(DataType::String).alias("subject_direction_qualifier"),
            lit(LiteralValue::untyped_null()).cast(DataType::String).alias("object_aspect_qualifier"),
            lit(LiteralValue::untyped_null()).cast(DataType::String).alias("object_direction_qualifier"),
            lit(LiteralValue::untyped_null()).cast(DataType::String).alias("upstream_data_source"),
        ])
        .with_column(
            when(col("x_source").eq(lit("NCBI")))
                .then(concat_str([col("x_source"), col("x_id")], "Gene:", true))
                .otherwise(col("x_source"))
                .alias("subject"),
        )
        .with_column(
            when(col("x_source").str().contains(lit("REACTOME"), true))
                .then(concat_str([lit("REACT"), col("x_id")], ":", true))
                .otherwise(col("subject"))
                .alias("subject"),
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
            when(col("y_source").eq(lit("NCBI")))
                .then(concat_str([col("y_source"), col("y_id")], "Gene:", true))
                .otherwise(col("y_source"))
                .alias("object"),
        )
        .with_column(
            when(col("y_source").str().contains(lit("REACTOME"), true))
                .then(concat_str([lit("REACT"), col("y_id")], ":", true))
                .otherwise(col("object"))
                .alias("object"),
        )
        .with_column(
            when(col("y_source").str().contains(lit("^(HPO||MONDO|UBERON)$"), true))
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
        .drop([
            "x_index", "x_id", "x_type", "x_name", "x_source", "y_index", "y_id", "y_type", "y_name", "y_source",
        ])
        .rename(["relation"], ["predicate"], true)
        // bioprocess_protein	interacts with
        // cellcomp_protein	interacts with
        // exposure_bioprocess	interacts with
        // exposure_cellcomp	interacts with
        // exposure_molfunc	interacts with
        // exposure_protein	interacts with
        // molfunc_protein	interacts with
        // pathway_protein	interacts with
        .with_column(
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
        // anatomy_anatomy	parent-child
        // bioprocess_bioprocess	parent-child
        // cellcomp_cellcomp	parent-child
        // disease_disease	parent-child
        // exposure_exposure	parent-child
        // molfunc_molfunc	parent-child
        // pathway_pathway	parent-child
        // phenotype_phenotype	parent-child
        .with_column(
            when(col("predicate").str().contains(
                lit("^(anatomy_anatomy|bioprocess_bioprocess|cellcomp_cellcomp|disease_disease|exposure_exposure|molfunc_molfunc|pathway_pathway|phenotype_phenotype)$"),
                true,
            ))
                .then(lit("biolink:superclass_of"))
                .otherwise(col("predicate"))
                .alias("predicate"),
        )
        // protein_protein	ppi
        .with_column(
            when(col("predicate").eq(lit("protein_protein"))).then(lit("biolink:interacts_with")).otherwise(col("predicate")).alias("predicate"),
        )
        // drug_effect	side effect
        .with_column(
            when(col("predicate").eq(lit("drug_effect"))).then(lit("biolink:has_side_effect")).otherwise(col("predicate")).alias("predicate")
        )
        // contraindication	contraindication
        .with_column(
            when(col("predicate").eq(lit("contraindication"))).then(lit("biolink:contraindicated_in")).otherwise(col("predicate")).alias("predicate")
        )
        // anatomy_protein_absent	expression absent	prediction	computational_model											NCBIGene:4948	UBERON:0001476
        .with_column(when(col("predicate").eq(lit("anatomy_protein_absent"))).then(lit("true")).otherwise(lit(LiteralValue::untyped_null())).alias("negated"))
        .with_column(when(col("predicate").eq(lit("anatomy_protein_absent"))).then(lit("biolink:expressed_in")).otherwise(col("predicate")).alias("predicate"))
        // anatomy_protein_present	expression present	prediction	computational_model											NCBIGene:81887	UBERON:0001323
        .with_column(when(col("predicate").eq(lit("anatomy_protein_present"))).then(lit("biolink:expressed_in")).otherwise(col("predicate")).alias("predicate"))
        // disease_phenotype_negative	phenotype absent	prediction	computational_model											MONDO:0019309	HPO:0004386
        .with_column(when(col("predicate").eq(lit("disease_phenotype_negative"))).then(lit("true")).otherwise(lit(LiteralValue::untyped_null())).alias("negated"))
        .with_column(when(col("predicate").eq(lit("disease_phenotype_negative"))).then(lit("biolink:has_phenotype")).otherwise(col("predicate")).alias("predicate"))
        // disease_phenotype_positive	phenotype present	prediction	computational_model											MONDO:0007058	HPO:0009611
        .with_column(when(col("predicate").eq(lit("disease_phenotype_positive"))).then(lit("biolink:has_phenotype")).otherwise(col("predicate")).alias("predicate"))
        // exposure_disease	linked to	prediction	computational_model											CTD:C051786	MONDO:0002691
        .with_column(when(col("predicate").eq(lit("exposure_disease"))).then(lit("biolink:correlated_with")).otherwise(col("predicate")).alias("predicate"))
        // indication    indication      prediction      computational_model                                                                                     DrugBank:DB00264        MONDO:0005044
        .with_column(
            when(col("predicate").eq(lit("indication"))).then(lit("biolink:treats")).otherwise(col("predicate")).alias("predicate")
        )
        // off-label use        off-label use   prediction      computational_model                                                                                     DrugBank:DB00796        MONDO:0005016
        .with_column(
            when(col("predicate").eq(lit("off-label use"))).then(lit("biolink:applied_to_treat")).otherwise(col("predicate")).alias("predicate")
        )
        // drug_drug       synergistic interaction prediction      computational_model                                                                                     DrugBank:DB01431        DrugBank:DB06605
        .with_column(
            when(col("predicate").eq(lit("drug_drug"))).then(lit("biolink:directly_physically_interacts_with")).otherwise(col("predicate")).alias("predicate")
        )
        // drug_protein    enzyme  prediction      computational_model                                                                                     DrugBank:DB00908        NCBIGene:1565
        .with_column(
            when(col("predicate").eq(lit("drug_protein")).and(col("display_relation").eq(lit("enzyme")))).then(lit("amount")).otherwise(lit(LiteralValue::untyped_null())).alias("subject_aspect_qualifier")
        )
        .with_column(
            when(col("predicate").eq(lit("drug_protein")).and(col("display_relation").eq(lit("enzyme")))).then(lit("biolink:affected_by")).otherwise(col("predicate")).alias("predicate")
        )
        // drug_protein    target  prediction      computational_model                                                                                     DrugBank:DB00334        NCBIGene:1128
        .with_column(
            when(col("predicate").eq(lit("drug_protein")).and(col("display_relation").eq(lit("target")))).then(lit("biolink:directly_physically_interacts_with")).otherwise(col("predicate")).alias("predicate")
        )
        // drug_protein    carrier prediction      computational_model                                                                                     DrugBank:DB00451        NCBIGene:6906
        .with_column(
            when(col("predicate").eq(lit("drug_protein")).and(col("display_relation").eq(lit("carrier")))).then(lit("biolink:affected_by")).otherwise(col("predicate")).alias("predicate")
        )
        // drug_protein    transporter     prediction      computational_model                                                                                     DrugBank:DB00257        NCBIGene:10599
        .with_column(when(col("predicate").eq(lit("drug_protein")).and(col("display_relation").eq(lit("transporter")))).then(lit("transport")).otherwise(lit(LiteralValue::untyped_null())).alias("subject_aspect_qualifier"))
        .with_column(when(col("predicate").eq(lit("drug_protein")).and(col("display_relation").eq(lit("transporter")))).then(lit("increased")).otherwise(lit(LiteralValue::untyped_null())).alias("subject_direction_qualifier"))
        .with_column(
            when(col("predicate").eq(lit("drug_protein")).and(col("display_relation").eq(lit("transporter")))).then(lit("biolink:affected_by")).otherwise(col("predicate")).alias("predicate")
        )
        .drop(["display_relation"])
        .unique(Some(vec!["subject".into(), "predicate".into(), "object".into()]), UniqueKeepStrategy::First)
        .collect()
        .unwrap();

    let mut file = fs::File::create(options.edges_output.as_path()).unwrap();
    CsvWriter::new(&mut file).with_separator(b'\t').finish(&mut edges_df).unwrap();

    info!("Duration: {}", format_duration(start.elapsed()).to_string());
    Ok(())
}
