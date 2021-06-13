use std::convert::TryFrom;
use std::error::Error;
use std::fs;
use std::process;

// (Full example with detailed comments in examples/01d_quick_example.rs)
//
// This example demonstrates clap's full 'custom derive' style of creating arguments which is the
// simplest method of use, but sacrifices some flexibility.
use clap::{AppSettings, Clap};
use j4rs::{ClasspathEntry, InvocationArg, Jvm, JvmBuilder};
use serde_json::Value;
use tantivy::collector::TopDocs;
use tantivy::directory::MmapDirectory;
use tantivy::query::QueryParser;
use tantivy::schema::Schema;
use tantivy::schema::INDEXED;
use tantivy::schema::STORED;
use tantivy::schema::TEXT;
use tantivy::DocAddress;
use tantivy::Document;
use tantivy::Index;
use tantivy::Score;

/// This doc string acts as a help message when the user runs '--help'
/// as do all doc strings on fields
#[derive(Clap)]
#[clap(version = "1.0", author = "Alex MN. <moore.niemi@gmail.com>")]
#[clap(setting = AppSettings::ColoredHelp)]
struct Opts {
    /// Sets a custom config file. Could have been an Option<T> with no default too
    #[clap(short, long, default_value = "default.conf")]
    config: String,
    /// Some input. Because this isn't an Option<T> it's required to be used
    input: String,
    /// A level of verbosity, and can be used multiple times
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Clap)]
enum SubCommand {
    #[clap(version = "1.3", author = "Someone E. <someone_else@other.com>")]
    Test(Test),
}

/// A subcommand for controlling testing
#[derive(Clap)]
struct Test {
    /// Print debug info
    #[clap(short)]
    debug: bool,
}

fn run() -> Result<(), Box<Error>> {
    // destination index details
    let mut schema_builder = Schema::builder();
    // fields
    let title = schema_builder.add_text_field("title", TEXT | STORED);
    let content = schema_builder.add_text_field("content", TEXT | STORED);
    let last_updated = schema_builder.add_date_field("last_updated", INDEXED);
    // This is ES specific in a sense
    let source = schema_builder.add_text_field("source", STORED);

    let schema = schema_builder.build();
    dbg!(&schema);

    // safe to rm -rf /tmp/lucky/tantivy-idx
    let index_path = "/tmp/lucky/tantivy-idx";
    fs::create_dir_all(index_path)?;

    let directory = MmapDirectory::open(&index_path)?;
    let index = Index::open_or_create(directory, schema.clone())?;

    let entry = ClasspathEntry::new("./java_wrapper/target/lucky-java-1.0-SNAPSHOT.jar");
    let jvm: Jvm = JvmBuilder::new()
        .classpath_entry(entry)
        .build()
        .expect("can build JVM");

    let instantiation_args = vec![InvocationArg::try_from("tests/resources/")?];
    let instance = jvm.create_instance("org.lucky.ShardReader", instantiation_args.as_ref())?;

    let chain = jvm.chain(&instance)?;
    let iterator = chain.invoke("batches", &[])?;

    // Indexing documents

    // Here we use a buffer of 100MB that will be split
    // between indexing threads.
    let mut index_writer = index.writer(100_000_000)?;

    // FIXME: tantivy DocId is incremental so this creates duplication on each run
    // FIXME: see also https://docs.rs/tantivy/0.15.0/tantivy/type.DocId.html
    while iterator.invoke("hasNext", &[])?.to_rust()? {
        let batch: Vec<String> = iterator.invoke("next", &[])?.to_rust()?;
        batch.iter().for_each(|doc_source| {
            // there is also a parse_document method we could use specific to tantivy
            // but it errors on any keys not in the schema so the below is more flexible right now
            // let doc: Document = schema.parse_document(&doc_source)?;
            let v: Value = serde_json::from_str(&doc_source).unwrap();
            // dbg!(v);

            let mut doc = Document::new();
            doc.add_text(title, v["title"].as_str().unwrap_or(""));
            doc.add_text(content, v["content"].as_str().unwrap_or(""));
            // TODO: chrono timestamp
            // doc.add_date(content, v["last_updated"].as_str().unwrap_or(""));
            doc.add_text(source, doc_source);

            index_writer.add_document(doc);
        });
    }

    index_writer.commit()?;

    // # Searching

    let reader = index.reader()?;

    let searcher = reader.searcher();
    dbg!(searcher.num_docs());

    let query_parser = QueryParser::for_index(&index, vec![title, content]);

    let query = query_parser.parse_query("lint")?;
    dbg!(&query);

    let top_docs: Vec<(Score, DocAddress)> = searcher.search(&query, &TopDocs::with_limit(10))?;

    println!("results");
    for (_score, doc_address) in top_docs {
        let retrieved_doc = searcher.doc(doc_address)?;
        dbg!(retrieved_doc);
        // println!("{}", schema.to_json(&retrieved_doc));
    }

    Ok(())
}

fn main() {
    // FIXME: this is old Rust 2015 format...
    if let Err(e) = run() {
        println!("Application error: {}", e);
        process::exit(1);
    }
}
