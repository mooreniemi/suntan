use std::convert::TryFrom;

// (Full example with detailed comments in examples/01d_quick_example.rs)
//
// This example demonstrates clap's full 'custom derive' style of creating arguments which is the
// simplest method of use, but sacrifices some flexibility.
use clap::{AppSettings, Clap};
use j4rs::{ClasspathEntry, InvocationArg, Jvm, JvmBuilder};
use serde_json::Value;

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

fn main() {
    // more program logic goes here...
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

    let chain = jvm.chain(&instance).unwrap();
    let iterator = chain.invoke("iterator", &[]).unwrap();

    while iterator.invoke("hasNext", &[]).unwrap().to_rust().unwrap() {
        let doc_source: String = iterator
            .invoke("next", &[])
            .unwrap()
            .to_rust()
            .expect("get first doc");
        let v: Value = serde_json::from_str(&doc_source).unwrap();

        dbg!(v);
    }
}
