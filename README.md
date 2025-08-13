# Welcome to Rusty Matrix IO

### Install Rust

https://www.rust-lang.org/tools/install

### Install Rusty Matrix IO

```shell
cargo install --git https://github.com/jdr0887/rusty-matrix-io.git
```

## For Developers:

### Building:

```shell
cargo build --release
```

### Running:

Once compiled, binaries will be found in ./target/release/

For example, there is a binary to assert that all the 'subject' & 'object' identifiers found in an edges file are used
within the nodes file. To run that command you would do the following:

```shell
./target/release/assert_all_edges_ids_exist_in_nodes -n <path_to_nodes_tsv_file> -e <path_to_edges_tsv_file>
```

All binaries include a '--help' option to further inspect the required/optional flags.

```shell
./target/release/assert_all_edges_ids_exist_in_nodes -h
```
