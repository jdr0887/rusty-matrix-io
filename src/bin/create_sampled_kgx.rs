use clap::Parser;
use humantime::format_duration;
use log::{debug, info};
use polars::prelude::*;
use rand::distr::Uniform;
use rand::Rng;
use std::fs;
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
    output_dir: path::PathBuf,

    #[clap(short = 's', long, default_value_t = 100)]
    size: u32,

    #[clap(short = 'r', long, default_value_t = false)]
    random: bool,
}

fn main() -> Result<(), Box<dyn error::Error>> {
    let start = Instant::now();
    env_logger::init();

    let options = Options::parse();
    debug!("{:?}", options);

    let edge_id_columns_df = LazyCsvReader::new(options.edges.clone())
        .with_separator(b'\t')
        .with_truncate_ragged_lines(true)
        .with_has_header(true)
        .with_ignore_errors(true)
        .finish()
        .unwrap()
        .select([col("subject"), col("object")])
        .collect()
        .unwrap();

    let edge_ids_df = concat(
        [edge_id_columns_df.clone().lazy().select([col("subject").alias("id")]), edge_id_columns_df.clone().lazy().select([col("object").alias("id")])],
        UnionArgs::default(),
    )
    .unwrap()
    .unique(None, UniqueKeepStrategy::First)
    .collect()
    .unwrap();

    let edge_ids_series = edge_ids_df.column("id").unwrap().as_series().unwrap();

    let selected_ids_series = match options.random {
        true => {
            let mut rng = rand::rng();
            let dist = Uniform::new(0, edge_ids_series.len() as i32).expect("Could not create Uniform from range");
            let sampled_indexes: Vec<i32> = (&mut rng).sample_iter(dist).take(options.size.clone() as usize).collect();
            let selected_ids: Vec<_> = sampled_indexes.iter().map(|a| edge_ids_series.get(*a as usize).unwrap().str_value()).collect();
            Series::new("id".into(), selected_ids)
        }
        false => edge_ids_series.limit(options.size.clone() as usize),
    };

    let mut edges_df = LazyCsvReader::new(options.edges.clone())
        .with_separator(b'\t')
        .with_truncate_ragged_lines(true)
        .with_has_header(true)
        .with_ignore_errors(true)
        .finish()
        .unwrap()
        .filter(col("subject").str().contains_any(lit(selected_ids_series.clone()), false).or(col("object").str().contains_any(lit(selected_ids_series.clone()), false)))
        .collect()
        .unwrap();

    let mut edges_file_name = options.nodes.clone().file_name().unwrap().to_str().unwrap().to_string();
    edges_file_name = edges_file_name.replace("nodes.", format!("edges_{}.", options.size.clone()).as_str());

    debug!("edges_file_name: {}", edges_file_name);
    let mut output_edges_file = fs::File::create(format!("{}/{}", options.output_dir.to_string_lossy(), edges_file_name)).unwrap();
    CsvWriter::new(&mut output_edges_file).with_separator(b'\t').finish(&mut edges_df).unwrap();

    let selected_edge_ids_df =
        concat([edges_df.clone().lazy().select([col("subject").alias("id")]), edges_df.clone().lazy().select([col("object").alias("id")])], UnionArgs::default())
            .unwrap()
            .collect()
            .unwrap();
    let selected_edge_ids = selected_edge_ids_df.column("id").unwrap().as_series().unwrap().clone();

    let mut nodes_df = LazyCsvReader::new(options.nodes.clone())
        .with_separator(b'\t')
        .with_truncate_ragged_lines(true)
        .with_has_header(true)
        .with_ignore_errors(true)
        .finish()
        .unwrap()
        .filter(col("id").str().contains_any(lit(selected_edge_ids.clone()), false))
        .collect()
        .unwrap();

    let mut nodes_file_name = options.nodes.clone().file_name().unwrap().to_str().unwrap().to_string();
    nodes_file_name = nodes_file_name.replace("nodes.", format!("nodes_{}.", options.size.clone()).as_str());

    debug!("nodes_file_name: {}", nodes_file_name);
    let mut output_nodes_file = fs::File::create(format!("{}/{}", options.output_dir.to_string_lossy(), nodes_file_name)).unwrap();
    CsvWriter::new(&mut output_nodes_file).with_separator(9u8).finish(&mut nodes_df).unwrap();

    info!("Duration: {}", format_duration(start.elapsed()).to_string());
    Ok(())
}

#[cfg(test)]
mod test {
    use polars::prelude::*;
    use rand::distr::Uniform;
    use rand::Rng;

    #[test]
    fn test_random_edges() {
        let ids_sorted_deduped = Series::new(
            "id".into(),
            [
                "biolink:actively_involved_in",
                "biolink:activity_decreased_by",
                "biolink:activity_increased_by",
                "biolink:acts_upstream_of_negative_effect",
                "biolink:acts_upstream_of_or_within",
                "biolink:acts_upstream_of_or_within_negative_effect",
                "biolink:acts_upstream_of_or_within_positive_effect",
                "biolink:acts_upstream_of_positive_effect",
                "biolink:address",
                "biolink:adverse_event_caused_by",
                "biolink:affects_molecular_modification_of",
                "biolink:affects_synthesis_of",
                "biolink:affects_uptake_of",
                "biolink:affiliation",
                "biolink:agent_id",
                "biolink:aggregate_statistic",
                "biolink:AnatomicalEntityToAnatomicalEntityOntogenicAssociation",
                "biolink:anatomical_entity_to_anatomical_entity_ontogenic_association_subject",
                "biolink:AnatomicalEntityToAnatomicalEntityPartOfAssociation",
                "biolink:anatomical_entity_to_anatomical_entity_part_of_association_subject",
                "biolink:animal_model_available_from",
                "biolink:article_iso_abbreviation",
                "biolink:article_published_in",
                "biolink:Association",
                "biolink:authors",
                "biolink:available_from",
                "biolink:BiologicalSex",
                "biolink:book_chapter_published_in",
                "biolink:capable_of",
                "biolink:CategoryType",
                "biolink:caused_by",
                "biolink:causes_adverse_event",
                "biolink:chapter",
                "biolink:ChemicalToChemicalDerivationAssociation",
                "biolink:chemical_to_chemical_derivation_association_catalyst_qualifier",
                "biolink:ClinicalAttribute",
                "biolink:clinical_finding_has_attribute",
                "biolink:condition_associated_with_gene",
                "biolink:contributor_association_qualifiers",
                "biolink:created_with",
                "biolink:creation_date",
                "biolink:dataset_download_url",
                "biolink:decreases_activity_of",
                "biolink:decreases_degradation_of",
                "biolink:decreases_expression_of",
                "biolink:decreases_folding_of",
                "biolink:decreases_metabolic_processing_of",
                "biolink:decreases_molecular_interaction",
                "biolink:decreases_molecular_modification_of",
                "biolink:decreases_splicing_of",
                "biolink:decreases_synthesis_of",
                "biolink:decreases_transport_of",
                "biolink:decreases_uptake_of",
                "biolink:directly_interacts_with",
                "biolink:DiseaseOrPhenotypicFeatureToLocationAssociation",
                "biolink:disrupted_by",
                "biolink:distribution_download_url",
                "biolink:download_url",
                "biolink:DrugToGeneAssociation",
                "biolink:end_interbase_coordinate",
                "biolink:entity_positively_regulated_by_entity",
                "biolink:entity_positively_regulates_entity",
                "biolink:expressed_in",
                "biolink:filler",
                "biolink:folding_decreased_by",
                "biolink:folding_increased_by",
                "biolink:food_component_of",
                "biolink:format",
                "biolink:full_name",
                "biolink:gene_associated_with_condition",
                "biolink:gene_expression_mixin_quantifier_qualifier",
                "biolink:genetically_interacts_with",
                "biolink:GeneToDiseaseAssociation",
                "biolink:gene_to_expression_site_association_quantifier_qualifier",
                "biolink:GeneToGeneCoexpressionAssociation",
                "biolink:gene_to_go_term_association_subject",
                "biolink:GeneToPhenotypicFeatureAssociation",
                "biolink:genotype_as_a_model_of_disease_association_subject",
                "biolink:has_biological_sequence",
                "biolink:has_chemical_formula",
                "biolink:has_constituent",
                "biolink:has_count",
                "biolink:has_dataset",
                "biolink:has_device",
                "biolink:has_distribution",
                "biolink:has_drug",
                "biolink:has_frameshift_variant",
                "biolink:has_gene_or_gene_product",
                "biolink:has_increased_amount",
                "biolink:has_manifestation",
                "biolink:has_molecular_consequence",
                "biolink:has_nutrient",
                "biolink:has_part",
                "biolink:has_percentage",
                "biolink:has_phenotype",
                "biolink:has_procedure",
                "biolink:has_quotient",
                "biolink:has_receptor",
                "biolink:has_route",
                "biolink:has_splice_site_variant",
                "biolink:has_stressor",
                "biolink:has_topic",
                "biolink:has_total",
                "biolink:has_zygosity",
                "biolink:increases_activity_of",
                "biolink:increases_expression_of",
                "biolink:increases_folding_of",
                "biolink:increases_metabolic_processing_of",
                "biolink:increases_molecular_interaction",
                "biolink:increases_molecular_modification_of",
                "biolink:increases_splicing_of",
                "biolink:increases_transport_of",
                "biolink:ingest_date",
                "biolink:IriType",
                "biolink:is_metabolite",
                "biolink:iso_abbreviation",
                "biolink:issue",
                "biolink:is_supplement",
                "biolink:keywords",
                "biolink:latitude",
                "biolink:license",
                "biolink:located_in",
                "biolink:longitude",
                "biolink:mesh_terms",
                "biolink:metabolic_processing_decreased_by",
                "biolink:metabolic_processing_increased_by",
                "biolink:molecular_interaction_decreased_by",
                "biolink:molecular_interaction_increased_by",
                "biolink:molecular_modification_decreased_by",
                "biolink:molecular_modification_increased_by",
                "biolink:nutrient_of",
                "biolink:Onset",
                "biolink:organismal_entity_has_attribute",
                "biolink:OrganismAttribute",
                "biolink:organism_taxon_subclass_of",
                "biolink:OrganismTaxonToEnvironmentAssociation",
                "biolink:OrganismTaxonToOrganismTaxonAssociation",
                "biolink:organism_taxon_to_organism_taxon_interaction_object",
                "biolink:organism_taxon_to_organism_taxon_interaction_subject",
                "biolink:organism_taxon_to_organism_taxon_specialization_object",
                "biolink:organism_taxon_to_organism_taxon_specialization_subject",
                "biolink:pages",
                "biolink:pairwise_molecular_interaction_id",
                "biolink:part_of",
                "biolink:PathognomonicityQuantifier",
                "biolink:physically_interacts_with",
                "biolink:positively_regulated_by",
                "biolink:PredicateType",
                "biolink:process_positively_regulated_by_process",
                "biolink:process_positively_regulates_process",
                "biolink:publication_id",
                "biolink:publication_pages",
                "biolink:published_in",
                "biolink:ReactionToCatalystAssociation",
                "biolink:ReactionToParticipantAssociation",
                "biolink:retrieved_on",
                "biolink:rights",
                "biolink:same_as",
                "biolink:sequence_variant_has_biological_sequence",
                "biolink:sequence_variant_id",
                "biolink:SeverityValue",
                "biolink:small_molecule_id",
                "biolink:SocioeconomicAttribute",
                "biolink:socioeconomic_exposure_has_attribute",
                "biolink:source_logo",
                "biolink:source_web_page",
                "biolink:splicing_decreased_by",
                "biolink:splicing_increased_by",
                "biolink:start_interbase_coordinate",
                "biolink:summary",
                "biolink:symbol",
                "biolink:synonym",
                "biolink:systematic_synonym",
                "biolink:timepoint",
                "biolink:TimeType",
                "biolink:trade_name",
                "biolink:transport_decreased_by",
                "biolink:transport_increased_by",
                "biolink:update_date",
                "biolink:variant_as_a_model_of_disease_association_subject",
                "biolink:VariantToGeneExpressionAssociation",
                "biolink:version",
                "biolink:version_of",
                "biolink:volume",
                "biolink:xref",
                "biolink:Zygosity",
            ],
        );

        let mut rng = rand::rng();
        let step = Uniform::new(0, ids_sorted_deduped.len() as i32).expect("Could not create Uniform from range");
        let v: Vec<i32> = (&mut rng).sample_iter(step).take(20usize).collect();
        println!("{:?}", v);
        let asdf: Vec<_> = v.iter().map(|a| ids_sorted_deduped.get(*a as usize).unwrap().str_value()).collect();
        println!("{:?}", asdf);

        assert_eq!(true, true);
    }
}
