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

This is just a proof-of-concept tool to dump Elasticsearch Lucene shards into Tantivy. 

It relies on `j4rs`. This is a trade-off. On one hand, it means it's easy to deal with Lucene codecs changing version since you can rely on the underlying Java code to read. On the other hand, it means that you have an inter-process communication layer between Rust and the JVM, which is slow to process.

To generate Elasticsearch data I use [elasticsearch-test-data](https://github.com/oliver006/elasticsearch-test-data). A copy of test data is kept in `tests/resources`. The schema is simply title, content and last_updated.

- java_wrapper should probably be made into a git submodule. Right now I `rsync` from another repo.