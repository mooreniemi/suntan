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
    let title = schema_builder.add_text_field("name", TEXT | STORED);
    let body = schema_builder.add_text_field("slug", TEXT);
    let schema = schema_builder.build();

    let index_path = "/tmp/lucky/tantivy-idx";
    fs::create_dir_all(index_path)?;

    let directory = MmapDirectory::open(&index_path)?;
    let index = Index::open_or_create(directory, schema.clone())?;

    let entry = ClasspathEntry::new("./java_wrapper/target/lucky-java-1.0-SNAPSHOT.jar");
    let jvm: Jvm = JvmBuilder::new()
        .classpath_entry(entry)
        .build()
        .expect("can build JVM");

    // this example shard was generated with some faker data in Latin
    let instantiation_args = vec![InvocationArg::try_from("tests/resources/")?];
    let instance = jvm.create_instance("org.lucky.ShardReader", instantiation_args.as_ref())?;

    let chain = jvm.chain(&instance)?;
    let iterator = chain.invoke("iterator", &[])?;

    // Indexing documents

    // Here we use a buffer of 100MB that will be split
    // between indexing threads.
    let mut index_writer = index.writer(100_000_000)?;

    // FIXME: likely better to just grab all the docs at once in a chunked series than this
    // FIXME: not clear how to set id on Document in tantivy so this is duplication on each run
    while iterator.invoke("hasNext", &[])?.to_rust()? {
        let doc_source: String = iterator.invoke("next", &[])?.to_rust()?;
        // there is also a parse_document method we could use specific to tantivy
        // but it errors on any keys not in the schema
        // let doc: Document = schema.parse_document(&doc_source)?;
        let v: Value = serde_json::from_str(&doc_source)?;
        // dbg!(v);

        let mut doc = Document::new();
        doc.add_text(title, v["name"].as_str().unwrap_or(""));
        doc.add_text(body, v["slug"].as_str().unwrap_or(""));

        index_writer.add_document(doc);
    }

    // We need to call .commit() explicitly to force the
    // index_writer to finish processing the documents in the queue,
    // flush the current index to the disk, and advertise
    // the existence of new documents.
    index_writer.commit()?;

    // # Searching

    let reader = index.reader()?;

    let searcher = reader.searcher();

    let query_parser = QueryParser::for_index(&index, vec![title, body]);

    // QueryParser may fail if the query is not in the right
    // format. For user facing applications, this can be a problem.
    // A ticket has been opened regarding this problem.
    let query = query_parser.parse_query("magnam")?;

    // Perform search.
    // `topdocs` contains the 10 most relevant doc ids, sorted by decreasing scores...
    let top_docs: Vec<(Score, DocAddress)> = searcher.search(&query, &TopDocs::with_limit(10))?;

    for (_score, doc_address) in top_docs {
        // Retrieve the actual content of documents given its `doc_address`.
        let retrieved_doc = searcher.doc(doc_address)?;
        println!("{}", schema.to_json(&retrieved_doc));
    }

    Ok(())
}

fn main() {
    if let Err(e) = run() {
        println!("Application error: {}", e);
        process::exit(1);
    }
}
