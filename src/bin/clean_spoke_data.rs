use async_once::AsyncOnce;
use humantime::format_duration;
use in_place::InPlace;
use itertools::Itertools;
use lazy_static::lazy_static;
use log::{debug, info};
use rayon::prelude::*;
use reqwest::header;
use reqwest::redirect::Policy;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{BTreeMap, HashMap};
use std::error;
use std::fs;
use std::io;
use std::io::BufWriter;
use std::io::prelude::*;
use std::path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;

lazy_static! {
    pub static ref REQWEST_CLIENT: AsyncOnce<reqwest::Client> = AsyncOnce::new(async {
        let mut headers = header::HeaderMap::new();
        headers.insert(header::ACCEPT, header::HeaderValue::from_static("application/json"));
        headers.insert(header::CONTENT_TYPE, header::HeaderValue::from_static("application/json"));
        let result = reqwest::Client::builder()
            .redirect(Policy::limited(5))
            .timeout(Duration::from_secs(900))
            .default_headers(headers)
            .build();

        match result {
            Ok(request_client) => request_client,
            Err(e) => panic!("Could not create Reqwest Client: {}", e),
        }
    });
    pub static ref SEPARATOR: String = format!("{}", char::from_u32(0x0000001F).unwrap()).to_string();
}

#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize, Ord, PartialOrd)]
pub struct Node {
    pub id: String,
    pub category: String,
    pub identifier: String,
    pub remainder: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize, Ord, PartialOrd)]
pub struct Edge {
    pub subject: String,
    pub predicate: String,
    pub object: String,
    pub remainder: String,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct NNResponse {
    pub id: NNResponseId,
    pub equivalent_identifiers: Vec<NNResponseEquivalentIdentifiers>,
    #[serde(rename = "type")]
    pub type_ancestors: Vec<String>,
    pub information_content: f32,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct NNResponseId {
    pub identifier: String,
    pub label: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct NNResponseEquivalentIdentifiers {
    pub identifier: String,
    pub label: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn error::Error>> {
    let start = Instant::now();
    env_logger::init();

    let category_ancestor_mapping = create_category_mapping().await;
    debug!("{:?}", category_ancestor_mapping);

    let base_path = path::PathBuf::from("/media/jdr0887/backup/home/jdr0887/matrix/data/01_RAW/KGs/spoke/V5");
    let nodes_path = base_path.join("nodes");
    let edges_path = base_path.join("edges");

    let category_info_path = nodes_path.join("category_file_info.tsv");
    let category_file_name_mapping: BTreeMap<String, String> = read_category_info(&category_info_path);
    // println!("category_file_name_mapping: {:?}", category_file_name_mapping);

    let predicate_info_path = edges_path.join("predicate_file_info.tsv");
    let spo_file_name_mapping: BTreeMap<(String, String, String), String> = read_predicate_info(&predicate_info_path);
    // println!("spo_file_name_mapping: {:?}", spo_file_name_mapping);

    for (category, file_name) in category_file_name_mapping.iter() {
        info!("category: {}, file_name: {}", category, file_name);

        let nodes_file_path = nodes_path.join(format!("{}.tsv", file_name));

        let nodes = read_nodes_file(&nodes_file_path);
        // nodes.iter().take(120).for_each(|n| println!("nodes: {:?}", n));

        let node_output = fs::File::create(nodes_path.join(format!("{}_new.tsv", file_name))).unwrap();
        let mut node_output_bf = BufWriter::new(node_output);

        writeln!(node_output_bf, "id\tcategory\t{}", get_header_remainder(&nodes_file_path, 3)).expect("Could not write to node_output_buf");

        let mut edge_files_by_category = spo_file_name_mapping
            .iter()
            .filter_map(|((s, _p, o), v)| {
                if s.eq(category) || o.eq(category) {
                    return Some(v.clone());
                }
                None
            })
            .collect_vec();
        edge_files_by_category.sort();
        info!("edge_files_by_category: {:?}", edge_files_by_category);

        for (idx, chunk) in nodes.chunks(2000).enumerate() {
            info!("chunk index: {}", idx);
            debug!("assert no empty identiers: {:?}", chunk.iter().all(|n| !n.identifier.is_empty()));

            // FIRST: UPDATE THE NODES
            let identifiers = chunk.iter().map(|n| n.identifier.clone()).collect_vec();
            let nn_payload = json!({
              "curies": identifiers,
              "conflate": true,
              "description": false,
              "drug_chemical_conflate": true
            });

            let request_client = REQWEST_CLIENT.get().await;

            debug!("nn_payload: {:?}", serde_json::to_string_pretty(&nn_payload));
            if let Ok(response) = request_client
                .post("https://nodenormalization-sri.renci.org/1.5/get_normalized_nodes")
                .json(&nn_payload)
                .send()
                .await
            {
                debug!("response.status(): {}", response.status());
                if let Ok(response_json) = response.json::<HashMap<String, Option<NNResponse>>>().await {
                    for (k, v) in response_json.iter() {
                        if let Some(n) = chunk.iter().find_or_first(|c| c.identifier.eq(k)) {
                            match v {
                                None => {
                                    let ancestors = category_ancestor_mapping
                                        .get(&n.category)
                                        .expect(format!("Could not get ancestors: {:?}", n).as_str());
                                    writeln!(node_output_bf, "{}\t{}\t{}", n.identifier, ancestors, n.remainder).expect("Could not write to node_output_buf");
                                    node_output_bf.flush().unwrap();
                                }
                                Some(nn_response) => {
                                    let ancestors = nn_response.type_ancestors.join(SEPARATOR.as_str());
                                    writeln!(node_output_bf, "{}\t{}\t{}", n.identifier, ancestors, n.remainder).expect("Could not write to node_output_buf");
                                    node_output_bf.flush().unwrap();
                                }
                            }
                        }
                    }
                }
            }
        }
        let id_identifier_map: HashMap<String, String> = nodes.into_par_iter().map(|n| (n.id.clone(), n.identifier.clone())).collect();

        info!("id_identifier_map.len(): {:?}", id_identifier_map.len());

        // SECOND: UPDATE THE EDGES
        for edge_file_name in edge_files_by_category.iter() {
            let start_modifying_edge_file = Instant::now();

            let edge_file_path = edges_path.join(format!("{}.tsv", edge_file_name));

            let mut edges = read_edges_file(&edge_file_path);
            info!("edges.len(): {}", edges.len());

            edges.par_iter_mut().for_each(|e| {
                if let Some(map_value) = id_identifier_map.get(&e.subject) {
                    e.subject = map_value.clone();
                }
            });

            // id_identifier_map.iter().for_each(|(map_key, map_value)| {
            //     edges.par_iter_mut().for_each(|e| {
            //         if e.subject.eq(map_key) {
            //             e.subject = map_value.clone();
            //         }
            //         if e.object.eq(map_key) {
            //             e.object = map_value.clone();
            //         }
            //         if !e.predicate.starts_with("biolink:") {
            //             e.predicate = format!("biolink:{}", e.predicate);
            //         }
            //     });
            // });

            edges.sort_by(|a, b| a.subject.cmp(&b.subject).then(a.object.cmp(&b.object)));

            let edge_header = format!("subject\tobject\tpredicate\t{}", get_header_remainder(&edge_file_path, 3));
            let edge_file_output = fs::File::create(edge_file_path.clone()).unwrap();
            let mut edge_writer = BufWriter::new(edge_file_output);

            writeln!(edge_writer, "{}", edge_header).expect("Could not write to edges file");

            edges.iter().for_each(|e| {
                writeln!(edge_writer, "{}\t{}\t{}\t{}", e.subject, e.object, e.predicate, e.remainder).unwrap();
            });
            // let mut df = LazyCsvReader::new(edge_file_path.clone())
            //     .with_separator(b'\t')
            //     .with_truncate_ragged_lines(true)
            //     .with_has_header(true)
            //     .with_ignore_errors(true)
            //     .finish()
            //     .unwrap()
            //     .with_columns([col("subject").strict_cast(DataType::String), col("object").strict_cast(DataType::String)])
            //     .collect()
            //     .unwrap();
            //
            // id_identifier_map.iter().for_each(|(map_key, map_value)| {
            //     df = df
            //         .clone()
            //         .lazy()
            //         .with_columns([
            //             when(col("subject").eq(lit(map_key.clone()))).then(lit(map_value.clone())).otherwise(col("subject")).alias("subject"),
            //             when(col("object").eq(lit(map_key.clone()))).then(lit(map_value.clone())).otherwise(col("object")).alias("object"),
            //         ])
            //         // .with_column(lit("biolink:").append(col("predicate"), false))
            //         .collect()
            //         .unwrap();
            // });
            //
            // df.apply("predicate", |c| {
            //     c.str().unwrap().into_iter().map(|opt_name: Option<&str>| opt_name.map(|name: &str| format!("biolink:{}", name))).collect::<StringChunked>().into_column()
            // })
            // .expect("Could not modify df");
            //
            // let mut file = fs::File::create(edge_file_path.as_path()).unwrap();
            // CsvWriter::new(&mut file).with_separator(b'\t').finish(&mut df).unwrap();
            info!(
                "Duration to write {:?}: {}",
                edge_file_path,
                format_duration(start_modifying_edge_file.elapsed()).to_string()
            );
        }

        // break;
    }
    info!("Duration: {}", format_duration(start.elapsed()).to_string());
    Ok(())
}

fn get_header_remainder(file_path: &path::PathBuf, split_at_idx: usize) -> String {
    let file = fs::File::open(file_path).unwrap();
    let reader = std::io::BufReader::new(file);
    let file_header = reader.lines().take(1).next().expect("Could not get header").unwrap();
    let file_header_split = file_header.split('\t').collect_vec();
    let (_header_left, header_right) = file_header_split.split_at(split_at_idx);
    header_right.join("\t")
}

fn read_nodes_file(nodes_file_path: &path::PathBuf) -> Vec<Node> {
    let nodes_file = fs::File::open(nodes_file_path).unwrap();
    let reader = io::BufReader::new(nodes_file);
    let mut nodes: Vec<Node> = reader
        .lines()
        .skip(1)
        .filter_map(|line| {
            let line = line.unwrap();
            let split = line.split('\t').collect_vec();
            if split.get(2).is_some() {
                let (left, right) = split.split_at(3);
                return Some(Node {
                    id: left[0].to_string(),
                    category: left[1].to_string(),
                    identifier: left[2].to_string(),
                    remainder: right.join("\t"),
                });
            }
            None
        })
        .collect();
    nodes.sort_by(|a, b| a.identifier.cmp(&b.identifier));
    nodes
}

fn read_edges_file(edges_file_path: &path::PathBuf) -> Vec<Edge> {
    let edges_file = fs::File::open(edges_file_path).unwrap();
    let reader = io::BufReader::new(edges_file);
    let edges: Vec<Edge> = reader
        .lines()
        .skip(1)
        .filter_map(|line| {
            let line = line.unwrap();
            let split = line.split('\t').collect_vec();
            if split.get(2).is_some() {
                let (left, right) = split.split_at(3);
                return Some(Edge {
                    subject: left[0].to_string(),
                    predicate: left[2].to_string(),
                    object: left[1].to_string(),
                    remainder: right.join("\t"),
                });
            }
            None
        })
        .collect();
    edges
}

fn read_predicate_info(predicate_info_path: &PathBuf) -> BTreeMap<(String, String, String), String> {
    let predicate_info_contents = fs::read_to_string(predicate_info_path).unwrap();
    predicate_info_contents
        .lines()
        .skip(1)
        .map(|line| {
            let split = line.split('\t').collect_vec();
            ((split[0].to_string(), split[1].to_string(), split[2].to_string()), split[3].to_string())
        })
        .collect()
}

fn read_category_info(category_info_path: &path::PathBuf) -> BTreeMap<String, String> {
    let category_file_contents = fs::read_to_string(category_info_path).unwrap();

    let include = vec![
        "Anatomy", // good
                  // "BiologicalProcess", // good
                  // "CellLine" // bad...skip b/c the identifier column is null
                  // "CellType",          // good...fix: a few lines need to fixed due to newlines
                  // "CellularComponent", // good
                  // "Complex", // bad...skip b/c the identifier column is null
                  // "Compound", // bad...issue with parsing the node file
                  // "Cytoband", // bad...skip b/c no curie for sequence region...maybe to MONDO???
                  // "Disease",            // issue with parsing the node file
                  // "EC",                // good...fix: lots of newlines messing up the columns
                  // "Food",              // good
                  // "Gene",              // good
                  // "Haplotype",         // good
                  // "MiRNA",             // good
                  // "MolecularFunction", // good
                  // "Organism",          // good, however, takes forever to run
                  // "Pathway",           // good
                  // "PharmacologicClass", // bad...category not known in NN or Biolink Lookup
                  // "Protein",       // good, however, takes forever to run
                  // "ProteinDomain", // good
                  // "ProteinFamily", // good
                  // "PwGroup",       // good
                  // "Reaction",      // bad...skip b/c the identifier column is null
                  // "SideEffect", // good
                  // "Symptom",    // good
                  // "Variant",    // good, however, takes forever to run
    ];

    // 4,6, 21, 24, 27, 33, 36, 39, 47, 49, 50, 53, 59, 61, 62, 63, 69, 72, 73, 75, 84, 88
    category_file_contents
        .lines()
        .skip(1)
        .filter(|line| include.iter().any(|l| line.contains(l)))
        .map(|line| {
            let split = line.split('\t').collect_vec();
            (split[0].to_string(), split[1].to_string())
        })
        .collect()
}

async fn create_category_mapping() -> BTreeMap<String, String> {
    let category_mapping = vec![
        ("Anatomy", "biolink:GrossAnatomicalStructure"),
        ("BiologicalProcess", "biolink:BiologicalProcess"),
        ("CellLine", "biolink:CellLine"),
        ("CellType", "biolink:Cell"),
        ("CellularComponent", "biolink:CellularComponent"),
        ("Complex", "biolink:MacromolecularComplex"), // ???
        ("Compound", "biolink:SmallMolecule"),
        ("Cytoband", ""), // ???
        ("Disease", "biolink:Disease"),
        ("EC", "biolink:ReactionToCatalystAssociation"), // ???  EC == Enzyme Commission
        ("Food", "biolink:Food"),
        ("Gene", "biolink:Gene"),
        ("Haplotype", "biolink:Haplotype"),
        ("MiRNA", "biolink:MicroRNA"),
        ("MolecularFunction", "biolink:MolecularActivity"),
        ("Organism", "biolink:Bacterium"),
        ("Pathway", "biolink:Pathway"),
        ("PharmacologicClass", ""), // ???
        ("Protein", "biolink:Protein"),
        ("ProteinDomain", "biolink:ProteinDomain"),
        ("ProteinFamily", "biolink:ProteinFamily"),
        ("PwGroup", "biolink:Pathway"),                       // ???
        ("Reaction", "biolink:ChemicalToPathwayAssociation"), // ??? biolink:ChemicalToPathwayAssociation, biolink:ChemicalToChemicalAssociation, biolink:ChemicalToChemicalDerivationAssociation
        ("SideEffect", "biolink:DiseaseOrPhenotypicFeature"), // ???
        ("Symptom", "biolink:DiseaseOrPhenotypicFeature"),
        ("Variant", "biolink:SequenceVariant"),
    ];
    let category_mapping: BTreeMap<String, String> = category_mapping.into_iter().map(|a| (a.0.to_string(), a.1.to_string())).collect();

    let request_client = REQWEST_CLIENT.get().await;

    let mut ret: BTreeMap<String, String> = BTreeMap::new();
    for (k, v) in category_mapping.iter() {
        if !v.is_empty() {
            if let Ok(response) = request_client
                .get(format!("https://biolink-lookup.ci.transltr.io/bl/biolink%3A{}/ancestors?version=v4.2.2", v))
                .send()
                .await
            {
                // println!("{:?}", response.text().await);
                let ancestors: Vec<String> = response.json().await.expect("Could not get ancestors");
                ret.insert(k.clone(), ancestors.join(SEPARATOR.as_str()));
            }
        }
    }
    ret
}

#[cfg(test)]
mod test {

    use crate::{NNResponse, Node};
    use in_place::InPlace;
    use itertools::Itertools;
    use polars::prelude::*;
    use serde_json::json;
    use std::collections::HashMap;
    use std::io::BufWriter;
    use std::io::{BufRead, BufReader, Write};
    use std::{fs, io, path};

    #[test]
    fn scratch() {
        let response: HashMap<String, Option<NNResponse>> = serde_json::from_value(
            json!(
                {
                    "UBERON:0003233":{"id":{"identifier":"UBERON:0003233","label":"epithelium of shoulder"},"equivalent_identifiers":[{"identifier":"UBERON:0003233","label":"epithelium of shoulder"}],"type":["biolink:GrossAnatomicalStructure","biolink:AnatomicalEntity","biolink:PhysicalEssence","biolink:OrganismalEntity","biolink:SubjectOfInvestigation","biolink:BiologicalEntity","biolink:ThingWithTaxon","biolink:NamedThing","biolink:PhysicalEssenceOrOccurrent"],"information_content":100.0},
                    "UBERON:2001901":{"id":{"identifier":"UBERON:2001901","label":"ceratobranchial 3 element"},"equivalent_identifiers":[{"identifier":"UBERON:2001901","label":"ceratobranchial 3 element"}],"type":["biolink:GrossAnatomicalStructure","biolink:AnatomicalEntity","biolink:PhysicalEssence","biolink:OrganismalEntity","biolink:SubjectOfInvestigation","biolink:BiologicalEntity","biolink:ThingWithTaxon","biolink:NamedThing","biolink:PhysicalEssenceOrOccurrent"],"information_content":90.9},
                    "UBERON:0004321":{"id":{"identifier":"UBERON:0004321","label":"middle phalanx of manual digit 3"},"equivalent_identifiers":[{"identifier":"UBERON:0004321","label":"middle phalanx of manual digit 3"}],"type":["biolink:GrossAnatomicalStructure","biolink:AnatomicalEntity","biolink:PhysicalEssence","biolink:OrganismalEntity","biolink:SubjectOfInvestigation","biolink:BiologicalEntity","biolink:ThingWithTaxon","biolink:NamedThing","biolink:PhysicalEssenceOrOccurrent"],"information_content":76.0},
                    "UBERON:0002414":{"id":{"identifier":"UBERON:0002414","label":"lumbar vertebra"},"equivalent_identifiers":[{"identifier":"UBERON:0002414","label":"lumbar vertebra"},{"identifier":"UMLS:C0024091","label":"Bone structure of lumbar vertebra"},{"identifier":"MESH:D008159","label":"Lumbar Vertebrae"},{"identifier":"NCIT:C12744","label":"Lumbar Vertebra"}],"type":["biolink:GrossAnatomicalStructure","biolink:AnatomicalEntity","biolink:PhysicalEssence","biolink:OrganismalEntity","biolink:SubjectOfInvestigation","biolink:BiologicalEntity","biolink:ThingWithTaxon","biolink:NamedThing","biolink:PhysicalEssenceOrOccurrent"],"information_content":74.6},
                    "UBERON:2005118":{"id":{"identifier":"UBERON:2005118","label":"middle lateral line primordium"},"equivalent_identifiers":[{"identifier":"UBERON:2005118","label":"middle lateral line primordium"}],"type":["biolink:GrossAnatomicalStructure","biolink:AnatomicalEntity","biolink:PhysicalEssence","biolink:OrganismalEntity","biolink:SubjectOfInvestigation","biolink:BiologicalEntity","biolink:ThingWithTaxon","biolink:NamedThing","biolink:PhysicalEssenceOrOccurrent"],"information_content":100.0},
                    "UBERON:0034769":{"id":{"identifier":"UBERON:0034769","label":"lymphomyeloid tissue"},"equivalent_identifiers":[{"identifier":"UBERON:0034769","label":"lymphomyeloid tissue"},{"identifier":"UMLS:C1512398","label":"Hematopoietic and Lymphoid Tissue"},{"identifier":"NCIT:C41168","label":"Hematopoietic and Lymphoid Tissue"}],"type":["biolink:GrossAnatomicalStructure","biolink:AnatomicalEntity","biolink:PhysicalEssence","biolink:OrganismalEntity","biolink:SubjectOfInvestigation","biolink:BiologicalEntity","biolink:ThingWithTaxon","biolink:NamedThing","biolink:PhysicalEssenceOrOccurrent"],"information_content":47.7},
                    "UBERON:0000100":{"id":{"identifier":"UBERON:0000100","label":"blastopore"},"equivalent_identifiers":[{"identifier":"UBERON:0000100","label":"blastopore"}],"type":["biolink:AnatomicalEntity","biolink:PhysicalEssence","biolink:OrganismalEntity","biolink:SubjectOfInvestigation","biolink:BiologicalEntity","biolink:ThingWithTaxon","biolink:NamedThing","biolink:PhysicalEssenceOrOccurrent"],"information_content":90.9},
                    "UBERON:3000044":null,
                    "UBERON:0022346":{"id":{"identifier":"UBERON:0022346","label":"dentate gyrus molecular layer middle"},"equivalent_identifiers":[{"identifier":"UBERON:0022346","label":"dentate gyrus molecular layer middle"}],"type":["biolink:AnatomicalEntity","biolink:PhysicalEssence","biolink:OrganismalEntity","biolink:SubjectOfInvestigation","biolink:BiologicalEntity","biolink:ThingWithTaxon","biolink:NamedThing","biolink:PhysicalEssenceOrOccurrent"],"information_content":100.0},
                    "UBERON:0001623":{"id":{"identifier":"UBERON:0001623","label":"dorsal nasal artery"},"equivalent_identifiers":[{"identifier":"UBERON:0001623","label":"dorsal nasal artery"},{"identifier":"UMLS:C0226194","label":"Structure of dorsal nasal artery"},{"identifier":"NCIT:C52983","label":"Dorsal Nasal Artery"}],"type":["biolink:GrossAnatomicalStructure","biolink:AnatomicalEntity","biolink:PhysicalEssence","biolink:OrganismalEntity","biolink:SubjectOfInvestigation","biolink:BiologicalEntity","biolink:ThingWithTaxon","biolink:NamedThing","biolink:PhysicalEssenceOrOccurrent"],"information_content":100.0}
                })).unwrap();
        assert_eq!(true, true);
    }

    #[test]
    fn test_read_nodes_file() {
        let base_path = path::PathBuf::from("/media/jdr0887/backup/home/jdr0887/matrix/KGs/spoke/V5");
        let nodes_path = base_path.join("nodes");

        let nodes_file_path = nodes_path.join(format!("{}.tsv", "node_0"));
        let nodes_file = std::fs::File::open(nodes_file_path).unwrap();

        let reader = io::BufReader::new(nodes_file);
        let mut nodes: Vec<Node> = reader
            .lines()
            .skip(1)
            .map(|line| {
                let line = line.unwrap();
                let split = line.split('\t').collect_vec();
                let (left, right) = split.split_at(3);
                Node {
                    id: left[0].to_string(),
                    category: left[1].to_string(),
                    identifier: left[2].to_string(),
                    remainder: right.join("\t"),
                }
            })
            .collect();
        nodes.sort_by(|a, b| a.identifier.cmp(&b.identifier));

        nodes.iter().take(120).for_each(|n| println!("nodes: {:?}", n));
        assert_eq!(true, true);
    }

    #[test]
    fn test_edit_in_place() {
        let base_path = path::PathBuf::from("/media/jdr0887/backup/home/jdr0887/matrix/KGs/spoke/V5");
        let edges_path = base_path.join("edges");
        let edge_file_path = edges_path.join(format!("{}.tsv", "edge_36"));
        let inp = InPlace::new(edge_file_path.as_path()).open().unwrap();
        let reader = io::BufReader::new(inp.reader());
        let mut writer = inp.writer();
        let chunk = vec![Node {
            id: "5306".to_string(),
            category: "Anatomy".to_string(),
            identifier: "UBERON:0000000".to_string(),
            remainder: "processual entity\tUberon\t[]\t".to_string(),
        }];
        let parse_options = CsvParseOptions::default().with_separator(b'\t');

        let mut df = CsvReadOptions::default()
            .with_parse_options(parse_options.clone())
            .with_has_header(true)
            .with_ignore_errors(true)
            .try_into_reader_with_file_path(Some(edge_file_path.clone()))
            .unwrap()
            .finish()
            .unwrap();

        println!("df: {:?}", df);
        df.apply("predicate", |c| {
            c.str()
                .unwrap()
                .into_iter()
                .map(|opt_name: Option<&str>| opt_name.map(|name: &str| format!("biolink:{}", name)))
                .collect::<StringChunked>()
                .into_column()
        })
        .expect("Could not modify df");
        df = df.lazy().with_columns([col("subject").strict_cast(DataType::String)]).collect().unwrap();
        df = df.lazy().with_columns([col("object").strict_cast(DataType::String)]).collect().unwrap();

        chunk.iter().for_each(|c| {
            df = df
                .clone()
                .lazy()
                .with_columns([
                    when(col("subject").eq(lit(c.id.clone())))
                        .then(lit(c.identifier.clone()))
                        .otherwise(col("subject"))
                        .alias("subject"),
                    when(col("object").eq(lit(c.id.clone())))
                        .then(lit(c.identifier.clone()))
                        .otherwise(col("object"))
                        .alias("object"),
                ])
                .collect()
                .unwrap();
        });

        println!("df: {:?}", df);

        let mut file = fs::File::create(edge_file_path.as_path()).unwrap();
        CsvWriter::new(&mut file).with_separator(b'\t').finish(&mut df).unwrap();

        // chunk.iter().for_each(|c| {
        // df.select([col("predicate")]).unwrap();

        // for row in df.iter() {
        //     //asdf
        //     row.se["predicate"] = format!("biolink:{}", row["predicate"]);
        // }
        // });

        // for line in reader.lines() {
        //     let mut line = line.unwrap();
        //     let split = line.split('\t').collect_vec();
        //     let (left, right) = split.split_at(3);
        //     println!("left: {}, right: {}", left.join("\t"), right.join("\t"));
        //     chunk.iter().for_each(|c| {
        //         line = format!(
        //             "{}\t{}\tbiolink:{}\t{}",
        //             left[0].to_string().replace(&c.id, &c.identifier),
        //             left[1].to_string().replace(&c.id, &c.identifier),
        //             left[2].to_string().replace(&c.id, &c.identifier),
        //             right.join("\t")
        //         );
        //     });
        //     // writeln!(writer, "{line}").expect("Could not write in place");
        // }
        // inp.save().expect("Could not save");
    }
}
