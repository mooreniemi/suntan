use chrono::{DateTime, Utc};
use log::{debug, error};
use log::{info, warn};
use std::convert::TryFrom;
use std::env;
use std::error::Error;
use std::fs;
use std::path::Path;
use std::process;
use std::str::FromStr;
use std::time::Instant;
use tantivy::fastfield::FastValue;

use clap::{AppSettings, Clap};
use j4rs::{ClasspathEntry, InvocationArg, Jvm, JvmBuilder};
use serde_json::Value;
use tantivy::collector::TopDocs;
use tantivy::directory::MmapDirectory;
use tantivy::query::QueryParser;
use tantivy::schema::{Field, FieldEntry, Schema};
use tantivy::DocAddress;
use tantivy::Document;
use tantivy::Index;
use tantivy::Score;

/// This is a tool for dumping Elasticsearch Lucene shards into Tantivy indices.
/// Elasticsearch stores fields in a particular way which is why it's not "just" Lucene.
/// If you use a schema mapping with _es_source as a listed field, a copy of the entire ES JSON will be placed there
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
    info!("Found Schema: {:?}", schema);
    schema.get_field("_es_source").and_then(|f| {
        info!("Found {:?} in Schema so will retain ES source document", f);
        Some(f)
    });

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
            .expect("Valid jassets classpath setup with build.rs"),
    );
    let jvm: Jvm = JvmBuilder::new()
        .classpath_entry(entry)
        .build()
        .expect("The system must support compatible JVM.");

    // The ShardReader class takes a path to an index and can return a batching iterator of doc _source
    let instantiation_args = vec![InvocationArg::try_from(input)?];
    let instance = jvm.create_instance("org.suntan.ShardReader", instantiation_args.as_ref())?;
    let chain = jvm.chain(&instance)?;

    let doc_count: u64 = chain.clone_instance()?.invoke("docCount", &[])?.to_rust()?;

    let iterator = chain.invoke("batches", &[])?;

    // # Indexing documents

    // FIXME: should probably be configurable
    // Tantivy buffer of 100MB that will be split between indexing threads.
    let mut index_writer = index.writer(100_000_000)?;

    // FIXME: tantivy DocId is incremental so this creates duplication on each run
    //        see also https://docs.rs/tantivy/0.15.0/tantivy/type.DocId.html
    let now = Instant::now();
    let mut batches: u64 = 0;
    let mut docs: u64 = 0;
    while iterator.invoke("hasNext", &[])?.to_rust()? {
        batches += 1;
        debug!("Found another batch ({})", batches);
        let batch: Vec<String> = iterator.invoke("next", &[])?.to_rust()?;
        batch.iter().for_each(|doc_source| {
            // there is also a parse_document method we could use specific to tantivy
            // but it errors on any keys not in the schema so the below is more flexible right now
            // let doc: Document = schema.parse_document(&doc_source)?;
            if let Ok(v) = serde_json::from_str(&doc_source) {
                let v: Value = v;
                // dbg!(v);
                let mut doc = Document::new();

                // FIXME: only handling exact field to field export for text
                for (field, field_entry) in schema.fields() {
                    if field_entry.name().eq("_es_source") {
                        doc.add_text(field, doc_source);
                    } else {
                        match add_to_doc(field, field_entry, &v, &mut doc) {
                            Ok(_) => {}
                            Err(_) => {
                                error!("Failed to add {:?} to {:?} on {:?}", v, field_entry, doc)
                            }
                        }
                    }
                }
                index_writer.add_document(doc);
                docs += 1;
            } else {
                error!("Failed to parse doc_source into Value: {:?}", doc_source);
            }
        });
    }
    info!(
        "Finished {} batches, {} docs, in {} seconds.",
        batches,
        docs,
        now.elapsed().as_secs()
    );

    if docs < doc_count {
        warn!(
            "# docs ingested on this run {} < # docs in source index {}; likely some docs errored.",
            docs, doc_count
        );
    }

    // Like Lucene, Tantivy has to close writers before opening readers
    index_writer.commit()?;

    // # Searching
    // We read the created index and send a test query into it, to confirm that we successfully exported

    let reader = index.reader()?;

    let searcher = reader.searcher();
    info!(
        "Found {} docs in exported Tantivy index.",
        searcher.num_docs()
    );

    if searcher.num_docs() != docs {
        warn!(
            "Tantivy index # docs {} didn't match # docs {} ingested on this run; were you ingesting onto an existing index?",
            searcher.num_docs(),
            docs
        );
    }

    // search all fields except unindexed (eg. source) for the test query
    let all_fields = schema
        .fields()
        .filter_map(|(field, field_entry)| field_entry.is_indexed().then(|| field))
        .collect();
    let query_parser = QueryParser::for_index(&index, all_fields);

    let query = query_parser.parse_query(&test_query)?;
    info!("Testing query {:?} against exported Tantivy index.", query);

    let top_docs: Vec<(Score, DocAddress)> = searcher.search(&query, &TopDocs::with_limit(10))?;

    if top_docs.len() > 0 {
        info!(
            "Test query {:?} found {} results.",
            test_query,
            top_docs.len()
        );
        for (_score, doc_address) in top_docs {
            let retrieved_doc = searcher.doc(doc_address)?;
            info!("Found in Tantivy index: {:?}", retrieved_doc);
            // println!("{}", schema.to_json(&retrieved_doc));
        }
    } else {
        warn!("Test query {:?} failed to find results.", test_query);
    }

    Ok(())
}

// FIXME: do I really need this or can I just use what Tantivy already has for json parsing?
// TODO: error msgs + HierarchicalFacet
fn add_to_doc(
    field: Field,
    field_entry: &FieldEntry,
    v: &Value,
    doc: &mut Document,
) -> anyhow::Result<()> {
    match field_entry.field_type() {
        tantivy::schema::FieldType::Str(_) => {
            let text = v[field_entry.name()]
                .as_str()
                .ok_or(anyhow::anyhow!("bad text"))?;
            doc.add_text(field, text);
            Ok(())
        }
        tantivy::schema::FieldType::U64(_) => {
            let num = v[field_entry.name()]
                .as_u64()
                .ok_or(anyhow::anyhow!("bad u64"))?;
            doc.add_u64(field, num);
            Ok(())
        }
        tantivy::schema::FieldType::I64(_) => {
            let num = v[field_entry.name()]
                .as_i64()
                .ok_or(anyhow::anyhow!("bad i64"))?;
            doc.add_i64(field, num);
            Ok(())
        }
        tantivy::schema::FieldType::F64(_) => {
            let num = v[field_entry.name()]
                .as_f64()
                .ok_or(anyhow::anyhow!("bad f64"))?;
            doc.add_f64(field, num);
            Ok(())
        }
        tantivy::schema::FieldType::Date(_) => {
            // NOTE: only rf3339? https://github.com/tantivy-search/tantivy/pull/721/files
            let fv = &v[field_entry.name()];
            let datetime_str = fv.as_str().ok_or(anyhow::anyhow!("bad date"))?;

            let datetime = DateTime::<Utc>::from_str(&datetime_str)?;
            doc.add_date(field, &datetime);
            Ok(())
        }
        tantivy::schema::FieldType::HierarchicalFacet(_) => {
            todo!()
        }
        tantivy::schema::FieldType::Bytes(_) => {
            let b = v[field_entry.name()]
                .as_str()
                .ok_or(anyhow::anyhow!("bad  (bytes)"))?
                .as_bytes();
            doc.add_bytes(field, b);
            Ok(())
        }
    }
}

fn main() {
    let opts: Opts = Opts::parse();
    env_logger::init();

    // FIXME: this is old Rust 2015 format...
    if let Err(e) = run(opts.input, opts.output, opts.schema_path, opts.test_query) {
        println!("Application error: {}", e);
        process::exit(1);
    }
}

// FIXME: clean up these tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_doc_field_text_parsing() {
        let mut doc = Document::new();
        let tantivy_f_title = r#"
        [{
            "name": "title",
            "type": "text",
            "options": {
              "indexing": {
                "record": "position",
                "tokenizer": "default"
              },
              "stored": false
            }
        }]
        "#;
        let schema: Schema = serde_json::from_str(&tantivy_f_title).expect("test data is valid");
        let es_source = r#"
        {
            "title" : "barters waistlines",
            "content" : "intrusion's decapitations drawbridge's trouping timepiece's peerage proctoring stinted ferrous gunfire bicyclist coverings perfumeries tyro plume's Sellers's hundredth",
            "last_update" : 1623646848000,
            "created" : 1622229665000
          }
        "#;
        let v: Value = serde_json::from_str(&es_source).expect("test data is valid");
        let title_f = schema
            .get_field("title")
            .expect("have title field in schema");
        let title_fe = schema.get_field_entry(title_f);
        let res = add_to_doc(title_f, title_fe, &v, &mut doc);
        assert!(res.is_ok());
    }

    #[test]
    fn test_doc_field_date_str_parsing() {
        let mut doc = Document::new();
        let tantivy_f_created = r#"
        [{
            "name": "created",
            "type": "date",
            "options": {
                "indexed": true,
                "stored": false
            }
        }]
        "#;
        let schema: Schema = serde_json::from_str(&tantivy_f_created).expect("test data is valid");
        let es_source = r#"
        {
            "title" : "barters waistlines",
            "content" : "intrusion's decapitations drawbridge's trouping timepiece's peerage proctoring stinted ferrous gunfire bicyclist coverings perfumeries tyro plume's Sellers's hundredth",
            "last_update" : 1623646848000,
            "created" : "1996-12-20T00:39:57+00:00"
        }
        "#;
        let v: Value = serde_json::from_str(&es_source).expect("test data is valid");
        let created_f = schema
            .get_field("created")
            .expect("have created field in schema");
        let title_fe = schema.get_field_entry(created_f);
        let res = add_to_doc(created_f, title_fe, &v, &mut doc);
        assert!(res.is_ok());
    }

    #[test]
    fn test_doc_field_date_num_parsing() {
        let mut doc = Document::new();
        let tantivy_f_created = r#"
        [{
            "name": "last_update",
            "type": "date",
            "options": {
                "indexed": true,
                "stored": false
            }
        }]
        "#;
        let schema: Schema = serde_json::from_str(&tantivy_f_created).expect("test data is valid");
        let es_source = r#"
        {
            "title" : "barters waistlines",
            "content" : "intrusion's decapitations drawbridge's trouping timepiece's peerage proctoring stinted ferrous gunfire bicyclist coverings perfumeries tyro plume's Sellers's hundredth",
            "last_update" : 1623646848000,
            "created" : "1996-12-20T00:39:57+00:00"
        }
        "#;
        let v: Value = serde_json::from_str(&es_source).expect("test data is valid");
        let last_update_f = schema
            .get_field("last_update")
            .expect("have last_update field in schema");
        let title_fe = schema.get_field_entry(last_update_f);
        let res = add_to_doc(last_update_f, title_fe, &v, &mut doc);
        // only date format of string is supported
        assert!(res.is_err());
    }
}
