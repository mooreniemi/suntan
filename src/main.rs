// https://github.com/astonbitecode/j4rs#Basics
// https://docs.rs/crate/j4rs/0.12.0#Java-arrays-and-variadics
//
//use std::{thread, time};
use j4rs::{InvocationArg, Jvm, JvmBuilder, ClasspathEntry};
use std::convert::TryFrom;

fn main() {
  // Create a JVM
  let entry = ClasspathEntry::new("/home/alex/git/lucky-java/target/lucky-java-1.0-SNAPSHOT.jar");
  let jvm: Jvm = JvmBuilder::new()
    .classpath_entry(entry)
    .build().unwrap();

    let string_instance = jvm.create_instance(
      "java.lang.String",
      &vec![InvocationArg::try_from(" a string ").unwrap()],
    ).unwrap();

    // Perform chained operations on the instance
    let string_size: isize = jvm.chain(string_instance)
      .invoke("trim", &[]).unwrap()
      .invoke("length", &[]).unwrap()
      .to_rust().unwrap();

    // Assert that the string was trimmed
    assert!(string_size == 8);

    println!("{}", string_size);

    let string_instance = jvm.create_instance(
      "java.lang.String",
      &vec![InvocationArg::try_from("/var/lib/elasticsearch/nodes/0/indices/9XubvRRIQjC10BJKHhG1ug/0/index/_0.cfs").unwrap()],
    ).unwrap();

    // The instances returned from invocations and instantiations can be viewed as pointers to Java Objects.
    // They can be used for further Java calls.
    // For example, the following invokes the `isEmpty` method of the created java.lang.String instance
    let boolean_instance = jvm.invoke(
      &string_instance,       // The String instance created above
      "isEmpty",              // The method of the String instance to invoke
      &Vec::new(),            // The `InvocationArg`s to use for the invocation - empty for this example
    ).unwrap();

    // If we need to transform an `Instance` to Rust value, the `to_rust` should be called
    let rust_boolean: bool = jvm.to_rust(boolean_instance).unwrap();
    println!("The isEmpty() method of the java.lang.String instance returned {}", rust_boolean);

    // Static invocation
    let _static_invocation_result = jvm.invoke_static(
      "java.lang.System",     // The Java class to invoke
      "currentTimeMillis",    // The static method of the Java class to invoke
      &Vec::new(),            // The `InvocationArg`s to use for the invocation - empty for this example
    ).unwrap();

    let time: isize = jvm.to_rust(_static_invocation_result).unwrap();
    println!("{}", time);

    let _static_invocation_result = jvm.invoke_static(
      "org.lucky.ShardReader",     // The Java class to invoke
      "docCount",    // The static method of the Java class to invoke
      &vec![InvocationArg::try_from("/var/lib/elasticsearch/nodes/0/indices/9XubvRRIQjC10BJKHhG1ug/0/index/").unwrap()],
    ).unwrap();

    //thread::sleep(time::Duration::from_secs(30));

    let doc_count: isize = jvm.to_rust(_static_invocation_result).unwrap();
    println!("{}", doc_count);

    let _static_invocation_result = jvm.invoke_static(
      "org.lucky.ShardReader",     // The Java class to invoke
      "queryName",    // The static method of the Java class to invoke
      &vec![InvocationArg::try_from("mary").unwrap()],
    ).unwrap();

    let names: Vec<String> = jvm.to_rust(_static_invocation_result).unwrap();
    println!("{:?}", names);
}
