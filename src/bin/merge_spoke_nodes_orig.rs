use itertools::Itertools;
use polars::prelude::*;
use std::ffi::OsStr;
use std::fs;
use std::path;

fn main() {
    let nodes_path = path::PathBuf::from("/media/jdr0887/backup/home/jdr0887/matrix/KGs/spoke/V5/nodes");
    let mut node_files = fs::read_dir(nodes_path)
        .unwrap()
        .map(|res| res.map(|e| e.path()))
        .filter_map(Result::ok)
        .filter(|p| p.file_name().and_then(OsStr::to_str).is_some_and(|n| n.starts_with("node_")))
        .collect_vec();

    node_files.sort();
    println!("{:?}", node_files);

    let mut main_df = df!(
        "identifier" => &Vec::<String>::new(),
                   "name" => &Vec::<String>::new(),
                   "category" => &Vec::<String>::new(),
    )
    .unwrap();

    let parse_options = CsvParseOptions::default().with_separator(b'\t');

    let join_args = JoinArgs::new(JoinType::Full).with_coalesce(JoinCoalesce::CoalesceColumns);

    let first_nodes_path = path::PathBuf::from("/media/jdr0887/backup/home/jdr0887/matrix/KGs/spoke/V5/nodes/node_0.tsv");
    let first_df = CsvReadOptions::default()
        .with_parse_options(parse_options.clone())
        .with_has_header(true)
        .with_ignore_errors(true)
        .try_into_reader_with_file_path(Some(first_nodes_path.clone()))
        .unwrap()
        .finish()
        .unwrap();

    let second_nodes_path = path::PathBuf::from("/media/jdr0887/backup/home/jdr0887/matrix/KGs/spoke/V5/nodes/node_1.tsv");
    let second_df = CsvReadOptions::default()
        .with_parse_options(parse_options.clone())
        .with_has_header(true)
        .with_ignore_errors(true)
        .try_into_reader_with_file_path(Some(second_nodes_path.clone()))
        .unwrap()
        .finish()
        .unwrap();

    main_df = main_df.join(&first_df, ["identifier", "name", "category"], ["identifier", "name", "category"], join_args.clone(), None).expect("Could not join");
    let mut asdf = main_df.select(["id", "identifier"]).unwrap();

    println!("id & identifier columsn: {:?}", asdf);
    // main_df = main_df.join(&second_df, ["identifier", "name", "category"], ["identifier", "name", "category"], join_args.clone()).expect("Could not join");
    let mut file = fs::File::create("/media/jdr0887/backup/home/jdr0887/matrix/KGs/spoke/V5/id_identifier_map.json").unwrap();
    JsonWriter::new(&mut file).with_json_format(JsonFormat::JsonLines).finish(&mut asdf).unwrap();

    // let mut file = fs::File::create("/media/jdr0887/backup/home/jdr0887/matrix/KGs/spoke/V5/nodes_merged.csv").unwrap();
    // CsvWriter::new(&mut file).with_separator(b'\t').finish(&mut main_df).unwrap();

    // for node_file in node_files.iter() {
    //     let mut df = CsvReadOptions::default()
    //         .with_parse_options(parse_options.clone())
    //         .with_has_header(true)
    //         .with_ignore_errors(true)
    //         .try_into_reader_with_file_path(Some(node_file.clone()))
    //         .unwrap()
    //         .finish()
    //         .unwrap();
    //
    //     let join_args = JoinArgs::new(JoinType::Full).with_coalesce(JoinCoalesce::CoalesceColumns);
    //     main_df = main_df.join(&df, ["identifier", "name"], ["identifier", "name"], join_args).expect("Could not join");
    //
    //     // df.join(&main_df, vec!["identifier", "name"], vec!["identifier", "name"], JoinArgs::new(JoinType::Left)).expect("Could not join");
    //
    //     println!("file: {:?} has column names: {:?}", node_file, df.get_column_names());
    //     break;
    // }

    println!("main dataframe column names: {:?}", main_df.get_column_names());
}
