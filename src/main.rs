use std::convert::TryFrom;
use std::env;
use std::error::Error;
use std::fs;
use std::path::Path;
use std::process;

use clap::{AppSettings, Clap};
use j4rs::{ClasspathEntry, InvocationArg, Jvm, JvmBuilder};
use serde_json::Value;
use tantivy::collector::TopDocs;
use tantivy::directory::MmapDirectory;
use tantivy::query::QueryParser;
use tantivy::schema::Schema;
use tantivy::DocAddress;
use tantivy::Document;
use tantivy::Index;
use tantivy::Score;

/// This is a tool for dumping Elasticsearch Lucene shards into Tantivy indices.
/// Elasticsearch stores fields in a particular way which is why it's not "just" Lucene.
#[derive(Clap)]
#[clap(version = "0.1.2", author = "Alex MN. <moore.niemi@gmail.com>")]
#[clap(setting = AppSettings::ColoredHelp)]
struct Opts {
    /// The location of the Elasticsearch Lucene index
    #[clap(short, long, default_value = "tests/resources/es-idx")]
    input: String,
    /// The location of the Tantivy schema
    #[clap(short, long, default_value = "tests/resources/tantivy-schema.json")]
    schema_path: String,
    /// The location of the Tantivy output index
    #[clap(short, long, default_value = "/tmp/suntan/tantivy-idx")]
    output: String,
    /// To test that the docs still match, this is sent in as test query
    #[clap(short, long, default_value = "lint")]
    test_query: String,
}

fn run(
    input: String,
    output: String,
    schema_path: String,
    test_query: String,
) -> Result<(), Box<Error>> {
    // destination index details
    let schema: Schema = serde_json::from_str(&fs::read_to_string(schema_path)?)?;
    dbg!(&schema);

    // safe to rm -rf /tmp/suntan/tantivy-idx
    fs::create_dir_all(&output)?;

    let directory = MmapDirectory::open(&output)?;
    let index = Index::open_or_create(directory, schema.clone())?;

    // j4rs Rust -> Java setup
    // FIXME: is this the best way to handle the underlying java dependency?
    // the jar must be prebuilt already with maven, using mvn package, then build.rs moves it into jassets
    let jassets_path = Path::new(&env!("CARGO_MANIFEST_DIR")).join("jassets/suntan.jar");
    let entry = ClasspathEntry::new(
        jassets_path
            .as_path()
            .to_str()
            .expect("valid jassets classpath"),
    );
    let jvm: Jvm = JvmBuilder::new()
        .classpath_entry(entry)
        .build()
        .expect("The system must support compatible JVM.");

    // The ShardReader class takes a path to an index and can return a batching iterator of doc _source
    let instantiation_args = vec![InvocationArg::try_from(input)?];
    let instance = jvm.create_instance("org.suntan.ShardReader", instantiation_args.as_ref())?;
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
            let v: Value = serde_json::from_str(&doc_source).expect("must be valid doc");
            // dbg!(v);

            let mut doc = Document::new();

            // FIXME: only handling exact field to field export for text
            schema.fields().for_each(|(field, field_entry)| {
                if field_entry.name().eq("source") {
                    doc.add_text(field, doc_source);
                } else {
                    match field_entry.field_type() {
                        tantivy::schema::FieldType::Str(_) => {
                            doc.add_text(field, v[field_entry.name()].as_str().unwrap_or(""));
                        }
                        tantivy::schema::FieldType::U64(_) => {
                            doc.add_text(field, v[field_entry.name()].as_u64().unwrap_or(0));
                        }
                        tantivy::schema::FieldType::I64(_) => {
                            doc.add_text(field, v[field_entry.name()].as_i64().unwrap_or(0));
                        }
                        tantivy::schema::FieldType::F64(_) => {
                            doc.add_text(field, v[field_entry.name()].as_i64().unwrap_or(0));
                        }
                        tantivy::schema::FieldType::Date(_) => {
                            // TODO: need to bring in chrono etc
                            // doc.add_date(content, v["last_updated"].as_str().unwrap_or(""));
                            todo!()
                        }
                        tantivy::schema::FieldType::HierarchicalFacet(_) => {
                            todo!()
                        }
                        tantivy::schema::FieldType::Bytes(_) => {
                            doc.add_bytes(
                                field,
                                v[field_entry.name()].as_str().unwrap_or("").as_bytes(),
                            );
                        }
                    }
                }
            });

            index_writer.add_document(doc);
        });
    }

    // Like Lucene, Tantivy has to close writers before opening readers
    index_writer.commit()?;

    // # Searching
    // We read the created index and send a test query into it, to confirm that we successfully exported

    let reader = index.reader()?;

    let searcher = reader.searcher();
    dbg!(searcher.num_docs());

    // search all fields except unindexed (eg. source) for the test query
    let all_fields = schema
        .fields()
        .filter_map(|(field, field_entry)| field_entry.is_indexed().then(|| field))
        .collect();
    let query_parser = QueryParser::for_index(&index, all_fields);

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
    if let Err(e) = run(opts.input, opts.output, opts.schema_path, opts.test_query) {
        println!("Application error: {}", e);
        process::exit(1);
    }
}
