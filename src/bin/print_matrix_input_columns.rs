use humantime::format_duration;
use itertools::Itertools;
use log::{info, warn};
use polars::prelude::*;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::fmt::{Display, Formatter};
use std::fs;
use std::io;
use std::io::prelude::*;
use std::path;
use std::time::Instant;

#[tokio::main]
async fn main() {
    let categories = vec![
        ("Anatomy", "biolink:GrossAnatomicalStructure", "UBERON"),
        ("BiologicalProcess", "biolink:BiologicalProcess", "GO"),
        ("CellLine", "biolink:CellLine", "CLO"),
        ("CellType", "biolink:Cell", "CL"),
        ("CellularComponent", "biolink:CellularComponent", "GO"),
        ("Complex", "biolink:MacromolecularComplex", ""), // ???
        ("Compound", "biolink:SmallMolecule", ""),
        ("Cytoband", "", ""), // ???
        ("Disease", "biolink:Disease", ""),
        ("EC", "biolink:ReactionToCatalystAssociation", ""), // ???  EC == Enzyme Commission
        ("Food", "biolink:Food", ""),
        ("Gene", "biolink:Gene", ""),
        ("Haplotype", "biolink:Haplotype", ""),
        ("MiRNA", "biolink:MicroRNA", ""),
        ("MolecularFunction", "biolink:MolecularActivity", ""),
        ("Organism", "biolink:Bacterium", ""),
        ("Pathway", "biolink:Pathway", ""),
        ("PharmacologicClass", "", ""), // ???
        ("Protein", "biolink:Protein", ""),
        ("ProteinDomain", "biolink:ProteinDomain", ""),
        ("ProteinFamily", "biolink:ProteinFamily", ""),
        ("PwGroup", "biolink:Pathway", ""),                       // ???
        ("Reaction", "biolink:ChemicalToPathwayAssociation", ""), // ??? biolink:ChemicalToPathwayAssociation, biolink:ChemicalToChemicalAssociation, biolink:ChemicalToChemicalDerivationAssociation
        ("SideEffect", "biolink:DiseaseOrPhenotypicFeature", ""), // ???
        ("Symptom", "biolink:DiseaseOrPhenotypicFeature", ""),
        ("Variant", "biolink:SequenceVariant", ""),
    ];

    for c in categories {
        if let Ok(response) = reqwest::get(format!("https://biolink-lookup.ci.transltr.io/bl/biolink%3A{}/ancestors?version=v4.2.2", c.1)).await {
            println!("category: {}, response: {:?}", c.0, response.text().await.unwrap());
        }
    }

    let nodes_file_path = path::PathBuf::from("/media/jdr0887/backup/home/jdr0887/matrix/data_01_RAW_KGs_spoke_sample_spoke_nodes.tsv");
    let edges_file_path = path::PathBuf::from("/media/jdr0887/backup/home/jdr0887/matrix/data_01_RAW_KGs_spoke_sample_spoke_edges.tsv");

    let mut nodes_file = fs::File::open(nodes_file_path.clone()).unwrap();
    // let mut edges_file = fs::File::open(edges_file_path).unwrap();

    let edge_file_contents = std::fs::read_to_string(&edges_file_path.as_path()).unwrap();
    let edge_lines = edge_file_contents.lines().skip(1).map(String::from).collect_vec();
    let get_edge_line = |lines: &Vec<String>, x: &String| -> Option<String> { lines.par_iter().find_first(|line| line.starts_with(x)).cloned() };

    let reader = io::BufReader::new(nodes_file);

    let mut rdr = csv::ReaderBuilder::new().has_headers(true).delimiter(b'\t').from_reader(reader);
    for result in rdr.deserialize() {
        let record: Input = result.unwrap();
        match record.category.as_str() {
            "Gene" => {
                println!("{:?}", serde_json::to_string(&record).unwrap());
                let output = record.to_output();
                println!("{:?}", serde_json::to_string(&output).unwrap());
                break;
            }
            &_ => {}
        }
    }
}

#[skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
struct Input {
    pub category: String,
    pub id: String,
    pub name: String,
    pub source: Option<String>,
    pub bto: Option<String>,
    pub identifier: Option<String>,
    pub mesh_id: Option<String>,
    pub Allele_ID: Option<String>,
    pub frequency_Latin_American_1: Option<String>,
    pub frequency_European: Option<String>,
    pub frequency_East_Asian: Option<String>,
    pub frequency_Asian: Option<String>,
    pub frequency_African_Others: Option<String>,
    pub frequency_African_American: Option<String>,
    pub frequency_African: Option<String>,
    pub position: Option<String>,
    pub frequency_Total: Option<String>,
    pub frequency_South_Asian: Option<String>,
    pub frequency_Other_Asian: Option<String>,
    pub alternative_allele: Option<String>,
    pub frequency_Other: Option<String>,
    pub Variant_Chromosome: Option<String>,
    pub frequency_Latin_American_2: Option<String>,
    pub reference_allele: Option<String>,
    pub aliases: Option<String>,
    pub reaction: Option<String>,
    pub class: Option<String>,
    pub subclass: Option<String>,
    pub subsubclass: Option<String>,
    pub serial: Option<String>,
    pub synonyms: Option<String>,
    pub evidence_code: Option<String>,
    pub complex_portal: Option<String>,
    pub assembly: Option<String>,
    pub sources: Option<String>,
    pub description: Option<String>,
    pub purification_method: Option<String>,
    pub corum: Option<String>,
    pub ensembl: Option<String>,
    pub license: Option<String>,
    pub chromosome: Option<String>,
    pub vestige: Option<String>,
    pub chembl_id: Option<String>,
    pub accession: Option<String>,
    pub pre_mirna: Option<String>,
    pub reactome_id: Option<String>,
    pub version: Option<String>,
    pub standardized_smiles: Option<String>,
    pub pdb_ligand_ids: Option<String>,
    pub pubchem_compound_ids: Option<String>,
    pub smiles: Option<String>,
    pub chembl_ids: Option<String>,
    pub CHEBI_ids: Option<String>,
    pub drugbank_ids: Option<String>,
    pub max_phase: Option<String>,
    pub kegg_compound_ids: Option<String>,
    pub biocyc_ids: Option<String>,
    pub kegg_drug_ids: Option<String>,
    pub vestige_ids: Option<String>,
    pub url: Option<String>,
    pub level: Option<String>,
    pub database: Option<String>,
    pub ICD10: Option<String>,
    pub SNOMEDCT: Option<String>,
    pub EFO: Option<String>,
    pub ICD9: Option<String>,
    pub obsolete: Option<String>,
    pub subset_list: Option<String>,
    pub representative_strain: Option<String>,
    pub genome_size: Option<String>,
    pub strain: Option<String>,
    pub isolation_country: Option<String>,
    pub host_name: Option<String>,
    pub host_is_human: Option<String>,
    pub isolation_source: Option<String>,
    pub genbank_accessions: Option<String>,
    pub collection_date: Option<String>,
    pub usa_isolation_country: Option<String>,
    pub host_health: Option<String>,
    pub host_age: Option<String>,
    pub host_is_chicken: Option<String>,
    pub host_gender: Option<String>,
    pub host_is_pig: Option<String>,
    pub has_amr_phenotype: Option<String>,
    pub gram_stain: Option<String>,
    pub oxygen_requirement: Option<String>,
    pub temperature_range: Option<String>,
    pub motility: Option<String>,
    pub salinity: Option<String>,
    pub sporulation: Option<String>,
    pub host_is_cow: Option<String>,
    pub serovar: Option<String>,
    pub biovar: Option<String>,
    pub pathovar: Option<String>,
    pub disease: Option<String>,
    pub entryName: Option<String>,
    pub org_name: Option<String>,
    pub reviewed: Option<String>,
    pub EC: Option<String>,
    pub org_ncbi_id: Option<String>,
    pub gene: Option<String>,
    pub start: Option<String>,
    pub end: Option<String>,
    pub polyprotein: Option<String>,
    pub isoform: Option<String>,
    pub omim_list: Option<String>,
    pub mesh_list: Option<String>,
    pub Linkout: Option<String>,
    pub class_type: Option<String>,
    pub hybrid_node_name: Option<String>,
    pub reference: Option<String>,
    pub endpos: Option<String>,
    pub startpos: Option<String>,
    pub population_sex: Option<String>,
    pub accession_number: Option<String>,
    pub population: Option<String>,
    pub population_age: Option<String>,
    pub xrefs: Option<String>,
}

// impl Display for Input {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         write!(f, "{}\t{}\t{}", self.id, self.name, self.category)
//     }
// }

impl Input {
    // fn as_gene_output(&self) -> Output {
    //     Output { id: self.id.clone(), name: self.name.clone(), category: self.category.clone(), ..Default::default() }
    // }

    fn to_output(&self) -> Output {
        match self.category.as_str() {
            "Gene" => Output {
                id: format!("NCBIGene:{}", self.identifier.clone().unwrap()),
                name: self.name.clone(),
                category: self.category.clone(),
                description: self.description.clone(),
                ensembl: self.ensembl.clone(),
                license: self.license.clone(),
                chromosome: self.chromosome.clone(),
                ..Default::default()
            },
            _ => Output { id: self.id.clone(), name: self.name.clone(), category: self.category.clone(), ..Default::default() },
        }
    }
}

// id	name	category	equivalent_identifiers	information_content	description	cd_formula	cd_molweight	clogp	alogs	tpsa	lipinski	mrdef	arom_c	sp3_c	sp2_c	sp_c	halogen	hetero_sp2_c	rotb	o_n	oh_nh	smiles	rgb	status	fda_labels	definition	url	locus_group	symbol	location	taxon	NCBITaxon	CHEBI_ROLE	MONDO_SUPERCLASS
// NCBIGene:5770	PTPN1	biolink:ThingWithTaxonbiolink:BiologicalEntitybiolink:ChemicalEntityOrProteinOrPolypeptidebiolink:MacromolecularMachineMixinbiolink:NamedThingbiolink:Proteinbiolink:OntologyClassbiolink:Genebiolink:GeneProductMixinbiolink:Polypeptidebiolink:PhysicalEssencebiolink:PhysicalEssenceOrOccurrentbiolink:GeneOrGeneProductbiolink:GenomicEntitybiolink:ChemicalEntityOrGeneOrGeneProduct	ENSEMBL:ENSG00000196396ENSEMBL:ENSP00000360683HGNC:9642OMIM:176885UMLS:C1335281UniProtKB:B4DSN5ENSEMBL:ENSP00000437732UniProtKB:P18031NCBIGene:5770ENSEMBL:ENSP00000437732.1PR:P18031ENSEMBL:ENSP00000360683.3UMLS:C1335056UniProtKB:A8K3M3	100.0	A tyrosine-protein phosphatase non-receptor type 1 that is encoded in the genome of human.																							protein-coding gene	PTPN1	20q13.13	NCBITaxon:9606	36237
// Gene	2491903	FLT3	Entrez Gene		2322																														fms related receptor tyrosine kinase 3			ENSG00000122025	CC0 1.0	13		CHEMBL1974

#[skip_serializing_none]
#[derive(serde::Serialize, Debug, Default)]
struct Output {
    pub id: String,
    pub name: String,
    pub category: String,
    pub source: Option<String>,
    pub bto: Option<String>,
    // pub identifier: Option<String>,
    pub mesh_id: Option<String>,
    pub Allele_ID: Option<String>,
    pub frequency_Latin_American_1: Option<String>,
    pub frequency_European: Option<String>,
    pub frequency_East_Asian: Option<String>,
    pub frequency_Asian: Option<String>,
    pub frequency_African_Others: Option<String>,
    pub frequency_African_American: Option<String>,
    pub frequency_African: Option<String>,
    pub position: Option<String>,
    pub frequency_Total: Option<String>,
    pub frequency_South_Asian: Option<String>,
    pub frequency_Other_Asian: Option<String>,
    pub alternative_allele: Option<String>,
    pub frequency_Other: Option<String>,
    pub Variant_Chromosome: Option<String>,
    pub frequency_Latin_American_2: Option<String>,
    pub reference_allele: Option<String>,
    pub aliases: Option<String>,
    pub reaction: Option<String>,
    pub class: Option<String>,
    pub subclass: Option<String>,
    pub subsubclass: Option<String>,
    pub serial: Option<String>,
    pub synonyms: Option<String>,
    pub evidence_code: Option<String>,
    pub complex_portal: Option<String>,
    pub assembly: Option<String>,
    pub sources: Option<String>,
    pub description: Option<String>,
    pub purification_method: Option<String>,
    pub corum: Option<String>,
    pub ensembl: Option<String>,
    pub license: Option<String>,
    pub chromosome: Option<String>,
    pub vestige: Option<String>,
    pub chembl_id: Option<String>,
    pub accession: Option<String>,
    pub pre_mirna: Option<String>,
    pub reactome_id: Option<String>,
    pub version: Option<String>,
    pub standardized_smiles: Option<String>,
    pub pdb_ligand_ids: Option<String>,
    pub pubchem_compound_ids: Option<String>,
    pub smiles: Option<String>,
    pub chembl_ids: Option<String>,
    pub CHEBI_ids: Option<String>,
    pub drugbank_ids: Option<String>,
    pub max_phase: Option<String>,
    pub kegg_compound_ids: Option<String>,
    pub biocyc_ids: Option<String>,
    pub kegg_drug_ids: Option<String>,
    pub vestige_ids: Option<String>,
    pub url: Option<String>,
    pub level: Option<String>,
    pub database: Option<String>,
    pub ICD10: Option<String>,
    pub SNOMEDCT: Option<String>,
    pub EFO: Option<String>,
    pub ICD9: Option<String>,
    pub obsolete: Option<String>,
    pub subset_list: Option<String>,
    pub representative_strain: Option<String>,
    pub genome_size: Option<String>,
    pub strain: Option<String>,
    pub isolation_country: Option<String>,
    pub host_name: Option<String>,
    pub host_is_human: Option<String>,
    pub isolation_source: Option<String>,
    pub genbank_accessions: Option<String>,
    pub collection_date: Option<String>,
    pub usa_isolation_country: Option<String>,
    pub host_health: Option<String>,
    pub host_age: Option<String>,
    pub host_is_chicken: Option<String>,
    pub host_gender: Option<String>,
    pub host_is_pig: Option<String>,
    pub has_amr_phenotype: Option<String>,
    pub gram_stain: Option<String>,
    pub oxygen_requirement: Option<String>,
    pub temperature_range: Option<String>,
    pub motility: Option<String>,
    pub salinity: Option<String>,
    pub sporulation: Option<String>,
    pub host_is_cow: Option<String>,
    pub serovar: Option<String>,
    pub biovar: Option<String>,
    pub pathovar: Option<String>,
    pub disease: Option<String>,
    pub entryName: Option<String>,
    pub org_name: Option<String>,
    pub reviewed: Option<String>,
    pub EC: Option<String>,
    pub org_ncbi_id: Option<String>,
    pub gene: Option<String>,
    pub polyprotein: Option<String>,
    pub isoform: Option<String>,
    pub omim_list: Option<String>,
    pub mesh_list: Option<String>,
    pub Linkout: Option<String>,
    pub class_type: Option<String>,
    pub hybrid_node_name: Option<String>,
    pub reference: Option<String>,
    pub endpos: Option<String>,
    pub startpos: Option<String>,
    pub population_sex: Option<String>,
    pub accession_number: Option<String>,
    pub population: Option<String>,
    pub population_age: Option<String>,
    pub xrefs: Option<String>,
}

impl Display for Output {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}\t{}\t{}", self.id, self.name, self.category)
    }
}

// 0 category
// 1 id
// 2 name
// 3 source
// 4 bto
// 5 identifier
// 6 mesh_id
// 7 Allele_ID
// 8 frequency_Latin_American_1
// 9 frequency_European
// 10 frequency_East_Asian
// 11 frequency_Asian
// 12 frequency_African_Others
// 13 frequency_African_American
// 14 frequency_African
// 15 position
// 16 frequency_Total
// 17 frequency_South_Asian
// 18 frequency_Other_Asian
// 19 alternative_allele
// 20 frequency_Other
// 21 Variant_Chromosome
// 22 frequency_Latin_American_2
// 23 reference_allele
// 24 aliases
// 25 reaction
// 26 class
// 27 subclass
// 28 subsubclass
// 29 serial
// 30 synonyms
// 31 evidence_code
// 32 complex_portal
// 33 assembly
// 34 sources
// 35 description
// 36 purification_method
// 37 corum
// 38 ensembl
// 39 license
// 40 chromosome
// 41 vestige
// 42 chembl_id
// 43 accession
// 44 pre_mirna
// 45 reactome_id
// 46 version
// 47 standardized_smiles
// 48 pdb_ligand_ids
// 49 pubchem_compound_ids
// 50 smiles
// 51 chembl_ids
// 52 CHEBI_ids
// 53 drugbank_ids
// 54 max_phase
// 55 kegg_compound_ids
// 56 biocyc_ids
// 57 kegg_drug_ids
// 58 vestige_ids
// 59 url
// 60 level
// 61 database
// 62 ICD10
// 63 SNOMEDCT
// 64 EFO
// 65 ICD9
// 66 obsolete
// 67 subset_list
// 68 representative_strain
// 69 genome_size
// 70 strain
// 71 isolation_country
// 72 host_name
// 73 host_is_human
// 74 isolation_source
// 75 genbank_accessions
// 76 collection_date
// 77 usa_isolation_country
// 78 host_health
// 79 host_age
// 80 host_is_chicken
// 81 host_gender
// 82 host_is_pig
// 83 has_amr_phenotype
// 84 gram_stain
// 85 oxygen_requirement
// 86 temperature_range
// 87 motility
// 88 salinity
// 89 sporulation
// 90 host_is_cow
// 91 serovar
// 92 biovar
// 93 pathovar
// 94 disease
// 95 entryName
// 96 org_name
// 97 reviewed
// 98 EC
// 99 org_ncbi_id
// 100 gene
// 101 start
// 102 end
// 103 polyprotein
// 104 isoform
// 105 omim_list
// 106 mesh_list
// 107 Linkout
// 108 class_type
// 109 hybrid_node_name
// 110 reference
// 111 endpos
// 112 startpos
// 113 population_sex
// 114 accession_number
// 115 population
// 116 population_age
// 117 xrefs
