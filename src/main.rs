use std::convert::TryFrom;
use std::error::Error;
use std::fs;
use std::process;

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

/// This is a tool for dumping Elasticsearch Lucene shards into Tantivy indices.
/// Elasticsearch stores fields in a particular way which is why it's not "just" Lucene.
#[derive(Clap)]
#[clap(version = "0.1.1", author = "Alex MN. <moore.niemi@gmail.com>")]
#[clap(setting = AppSettings::ColoredHelp)]
struct Opts {
    /// The location of the Elasticsearch Lucene index
    #[clap(short, long, default_value = "tests/resources")]
    input: String,
    /// The location of the Tantivy output index
    #[clap(short, long, default_value = "/tmp/lucky/tantivy-idx")]
    output: String,
    /// To test that the docs still match, this is sent in as test query
    #[clap(short, long, default_value = "lint")]
    test_query: String,
}

fn run(input: String, output: String, test_query: String) -> Result<(), Box<Error>> {
    // destination index details
    let mut schema_builder = Schema::builder();
    // TODO: this is the annoying part, we need to map schema to schema
    let title = schema_builder.add_text_field("title", TEXT | STORED);
    let content = schema_builder.add_text_field("content", TEXT | STORED);
    // TODO: handling time means bringing in chrono etc
    let _last_updated = schema_builder.add_date_field("last_updated", INDEXED);
    // This is ES specific in that ES uses StoredField to contain entire JSON doc that was stored
    let source = schema_builder.add_text_field("source", STORED);

    let schema = schema_builder.build();
    dbg!(&schema);

    // safe to rm -rf /tmp/lucky/tantivy-idx
    fs::create_dir_all(&output)?;

    let directory = MmapDirectory::open(&output)?;
    let index = Index::open_or_create(directory, schema.clone())?;

    let entry = ClasspathEntry::new("./java_wrapper/target/lucky-java-1.0-SNAPSHOT.jar");
    let jvm: Jvm = JvmBuilder::new()
        .classpath_entry(entry)
        .build()
        .expect("The system must support compatible JVM.");

    // The ShardReader class takes a path to an index and can return a batching iterator of doc _source
    let instantiation_args = vec![InvocationArg::try_from(input)?];
    let instance = jvm.create_instance("org.lucky.ShardReader", instantiation_args.as_ref())?;
    let chain = jvm.chain(&instance)?;
    let iterator = chain.invoke("batches", &[])?;

    // # Indexing documents

    // Tantivy buffer of 100MB that will be split between indexing threads.
    let mut index_writer = index.writer(100_000_000)?;

    // FIXME: tantivy DocId is incremental so this creates duplication on each run
    //        see also https://docs.rs/tantivy/0.15.0/tantivy/type.DocId.html
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

    // Like Lucene, Tantivy has to close writers before opening readers
    index_writer.commit()?;

    // # Searching

    let reader = index.reader()?;

    let searcher = reader.searcher();
    dbg!(searcher.num_docs());

    let query_parser = QueryParser::for_index(&index, vec![title, content]);

    let query = query_parser.parse_query(&test_query)?;
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
    let opts: Opts = Opts::parse();

    // FIXME: this is old Rust 2015 format...
    if let Err(e) = run(opts.input, opts.output, opts.test_query) {
        println!("Application error: {}", e);
        process::exit(1);
    }
}
