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

    edges_df = edges_df
        .clone()
        .lazy()
        .with_columns([
            lit("prediction").alias("knowledge_level"),
            lit("computational_model").alias("agent_type"),
            lit(LiteralValue::untyped_null()).cast(DataType::String).alias("primary_knowledge_source"),
            lit(LiteralValue::untyped_null()).cast(DataType::String).alias("aggregator_knowledge_source"),
            lit(LiteralValue::untyped_null()).cast(DataType::String).alias("original_subject"),
            lit(LiteralValue::untyped_null()).cast(DataType::String).alias("original_object"),
            lit(LiteralValue::untyped_null()).cast(DataType::String).alias("publications"),
            lit(LiteralValue::untyped_null()).cast(DataType::String).alias("subject_aspect_qualifier"),
            lit(LiteralValue::untyped_null()).cast(DataType::String).alias("subject_direction_qualifier"),
            lit(LiteralValue::untyped_null()).cast(DataType::String).alias("object_aspect_qualifier"),
            lit(LiteralValue::untyped_null()).cast(DataType::String).alias("object_direction_qualifier"),
            lit(LiteralValue::untyped_null()).cast(DataType::String).alias("upstream_data_source"),
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
            when(col("predicate").str().contains_literal(lit("protein_protein"))).then(lit("biolink:interacts_with")).otherwise(col("predicate")).alias("predicate"),
        )
        // drug_effect	side effect
        .with_column(
            when(col("predicate").str().contains_literal(lit("drug_effect"))).then(lit("biolink:has_side_effect")).otherwise(col("predicate")).alias("predicate")
        )
        // contraindication	contraindication
        .with_column(
            when(col("predicate").str().contains_literal(lit("contraindication"))).then(lit("biolink:contraindicated_in")).otherwise(col("predicate")).alias("predicate")
        )
        // subject_aspect_qualifier & object_aspect_qualifier enum values: stability|abundance|expression|exposure
        // DirectionQualifierEnum values: increased|upregulated|decreased|downregulated

        // anatomy_protein_absent	expression absent	prediction	computational_model											NCBIGene:4948	UBERON:0001476
        // I read this as 'the absence of the expression of NCBIGene:4948 affects UBERON:0001476'...is this right?
        .with_column(when(col("predicate").str().contains_literal(lit("anatomy_protein_absent"))).then(lit("expression")).otherwise(lit(LiteralValue::untyped_null())).alias("subject_aspect_qualifier"))
        .with_column(when(col("predicate").str().contains_literal(lit("anatomy_protein_absent"))).then(lit("decreased")).otherwise(lit(LiteralValue::untyped_null())).alias("subject_direction_qualifier"))
        .with_column(when(col("predicate").str().contains_literal(lit("anatomy_protein_absent"))).then(lit("biolink:affects")).otherwise(col("predicate")).alias("predicate"))

        // anatomy_protein_present	expression present	prediction	computational_model											NCBIGene:81887	UBERON:0001323
        // I read this as 'the presence of the expression of NCBIGene:81887 affects UBERON:0001323'...is this right?
        .with_column(when(col("predicate").str().contains_literal(lit("anatomy_protein_present"))).then(lit("expression")).otherwise(lit(LiteralValue::untyped_null())).alias("subject_aspect_qualifier"))
        .with_column(when(col("predicate").str().contains_literal(lit("anatomy_protein_present"))).then(lit("increased")).otherwise(lit(LiteralValue::untyped_null())).alias("subject_direction_qualifier"))
        .with_column(when(col("predicate").str().contains_literal(lit("anatomy_protein_present"))).then(lit("biolink:affects")).otherwise(col("predicate")).alias("predicate"))

        // disease_phenotype_negative	phenotype absent	prediction	computational_model											MONDO:0019309	HPO:0004386
        // I read this as 'the lack of MONDO:0019309 does not cause HPO:0004386'...is this right?
        .with_column(when(col("predicate").str().contains_literal(lit("disease_phenotype_negative"))).then(lit("exposure")).otherwise(lit(LiteralValue::untyped_null())).alias("subject_aspect_qualifier"))
        .with_column(when(col("predicate").str().contains_literal(lit("disease_phenotype_negative"))).then(lit("decreased")).otherwise(lit(LiteralValue::untyped_null())).alias("subject_direction_qualifier"))
        .with_column(when(col("predicate").str().contains_literal(lit("disease_phenotype_negative"))).then(lit("biolink:causes")).otherwise(col("predicate")).alias("predicate"))

        // disease_phenotype_positive	phenotype present	prediction	computational_model											MONDO:0007058	HPO:0009611
        // I read this as 'the negative of the expression of MONDO:0019309 affects HPO:0004386'...is this right?
        .with_column(when(col("predicate").str().contains_literal(lit("disease_phenotype_positive"))).then(lit("expression")).otherwise(lit(LiteralValue::untyped_null())).alias("subject_aspect_qualifier"))
        .with_column(when(col("predicate").str().contains_literal(lit("disease_phenotype_positive"))).then(lit("increased")).otherwise(lit(LiteralValue::untyped_null())).alias("subject_direction_qualifier"))
        .with_column(when(col("predicate").str().contains_literal(lit("disease_phenotype_positive"))).then(lit("biolink:affects")).otherwise(col("predicate")).alias("predicate"))

        // exposure_disease	linked to	prediction	computational_model											CTD:C051786	MONDO:0002691
        // I read this as 'increased exposure to CTD:C051786 causes MONDO:0002691'
        .with_column(when(col("predicate").str().contains_literal(lit("exposure_disease"))).then(lit("exposure")).otherwise(lit(LiteralValue::untyped_null())).alias("subject_aspect_qualifier"))
        .with_column(when(col("predicate").str().contains_literal(lit("exposure_disease"))).then(lit("increased")).otherwise(lit(LiteralValue::untyped_null())).alias("subject_direction_qualifier"))
        .with_column(when(col("predicate").str().contains_literal(lit("exposure_disease"))).then(lit("biolink:causes")).otherwise(col("predicate")).alias("predicate"))

        // indication    indication      prediction      computational_model                                                                                     DrugBank:DB00264        MONDO:0005044
        // indication	indication
        .with_column(
            when(col("predicate").str().contains_literal(lit("indication"))).then(lit("biolink:indicated_in")).otherwise(col("predicate")).alias("predicate")
        )

        // off-label use        off-label use   prediction      computational_model                                                                                     DrugBank:DB00796        MONDO:0005016
        // off-label use	off-label use
        .with_column(
            when(col("predicate").str().contains_literal(lit("off-label use"))).then(lit("biolink:applied_to_treat")).otherwise(col("predicate")).alias("predicate")
        )

        // drug_drug       synergistic interaction prediction      computational_model                                                                                     DrugBank:DB01431        DrugBank:DB06605
        // drug_drug	synergistic interaction -> causes increased effect/potency
        .with_column(
            when(col("predicate").str().contains_literal(lit("drug_drug"))).then(lit("biolink:directly_physically_interacts_with")).otherwise(col("predicate")).alias("predicate")
        )

        // drug_protein    enzyme  prediction      computational_model                                                                                     DrugBank:DB00908        NCBIGene:1565
        // drug_protein	enzyme
        // .with_column(
        //     when(col("predicate").str().contains_literal(lit("drug_protein")).and(col("display_relation").eq(lit("enzyme")))).then(lit("biolink:directly_physically_interacts_with")).otherwise(col("predicate")).alias("predicate")
        // )

        // drug_protein    target  prediction      computational_model                                                                                     DrugBank:DB00334        NCBIGene:1128
        // drug_protein	target
        // .with_column(
        //     when(col("predicate").str().contains_literal(lit("drug_protein")).and(col("display_relation").eq(lit("target")))).then(lit("biolink:directly_physically_interacts_with")).otherwise(col("predicate")).alias("predicate")
        // )
        // drug_protein    carrier prediction      computational_model                                                                                     DrugBank:DB00451        NCBIGene:6906
        // drug_protein	carrier
        // .with_column(
        //     when(col("predicate").str().contains_literal(lit("drug_protein")).and(col("display_relation").eq(lit("carrier")))).then(lit("biolink:???")).otherwise(col("predicate")).alias("predicate")
        // )
        // drug_protein    transporter     prediction      computational_model                                                                                     DrugBank:DB00257        NCBIGene:10599
        // drug_protein	transporter
        // I read this as 'increased abundance of DrugBank:DB00257 affects NCBIGene:10599'
        // .with_column(when(col("predicate").str().contains_literal(lit("drug_protein")).and(col("display_relation").eq(lit("transporter")))).then(lit("abundance")).otherwise(lit(LiteralValue::untyped_null())).alias("subject_aspect_qualifier"))
        // .with_column(when(col("predicate").str().contains_literal(lit("drug_protein")).and(col("display_relation").eq(lit("transporter")))).then(lit("increased")).otherwise(lit(LiteralValue::untyped_null())).alias("subject_direction_qualifier"))
        // .with_column(
        //     when(col("predicate").str().contains_literal(lit("drug_protein")).and(col("display_relation").eq(lit("transporter")))).then(lit("biolink:affects")).otherwise(col("predicate")).alias("predicate")
        // )
        .collect()
        .unwrap();

    let mut file = fs::File::create(options.edges_output.as_path()).unwrap();
    CsvWriter::new(&mut file).with_separator(b'\t').finish(&mut edges_df).unwrap();

    info!("Duration: {}", format_duration(start.elapsed()).to_string());
    Ok(())
}
