// https://github.com/astonbitecode/j4rs#Basics
// https://docs.rs/crate/j4rs/0.12.0#Java-arrays-and-variadics
//
//use std::{thread, time};
use j4rs::{ClasspathEntry, InvocationArg, Jvm, JvmBuilder};
use serde_json::Value;
use std::convert::TryFrom;

fn main() {
    let entry = ClasspathEntry::new("/home/alex/git/lucky-java/target/lucky-java-1.0-SNAPSHOT.jar");
    let jvm: Jvm = JvmBuilder::new()
        .classpath_entry(entry)
        .build()
        .expect("can build JVM");

    // this example shard was generated with some faker data in Latin
    let instantiation_args = vec![InvocationArg::try_from("tests/resources/").unwrap()];
    let instance = jvm
        .create_instance("org.lucky.ShardReader", instantiation_args.as_ref())
        .unwrap();

    let field_and_value = vec![
        InvocationArg::try_from("name").unwrap(),
        InvocationArg::try_from("magnam").unwrap(),
    ];
    let names: Vec<String> = jvm
        .chain(&instance)
        .unwrap()
        .invoke("queryName", &field_and_value)
        .unwrap()
        .to_rust()
        .expect("should be able to execute queryName and convert to vec");
    dbg!(names);

    let first_doc: String = jvm
        .chain(&instance)
        .unwrap()
        .invoke("iterator", &[])
        .unwrap()
        .invoke("next", &[])
        .unwrap()
        .to_rust()
        .expect("get first doc");
    let v: Value = serde_json::from_str(&first_doc).unwrap();
    dbg!(v);

    let all_docs: Vec<String> = jvm
        .chain(&instance)
        .unwrap()
        .invoke("allDocSource", &[])
        .unwrap()
        .to_rust()
        .expect("get all docs at once");
    dbg!(all_docs.len());
}
