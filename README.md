```
    .-.-.
 __ \   / __
(  ` \.'.'  )
(__.', \ .__)
    /   \`===,
    `-^-'

niki
```
# lucky

This is just a proof-of-concept tool to dump Lucene shards into Tantivy. 

It relies on `j4rs`. This is a trade-off. On one hand, it means it's easy to deal with Lucene codecs changing version since you can rely on the underlying Java code to read. On the other hand, it means that you have an inter-process communication layer between Rust and the JVM, which is slow to process.

- java_wrapper should probably be made into a git submodule. Right now I `rsync` from another repo.