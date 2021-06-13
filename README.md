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

This is just a proof-of-concept tool to dump Elasticsearch Lucene shards into Tantivy. There's also a couple `examples` of calling Lucene through Rust for querying.

We rely on [`j4rs`](https://github.com/astonbitecode/j4rs). This is a trade-off. On one hand, it means it's easy to deal with Lucene codecs changing version since you can depend directly on the underlying Java code to read. On the other hand, it means that you have an inter-process communication layer between Rust and the JVM, which is slow to process. In the `main` function we make use of a batching iterator which pulls 1000 docs out of the Lucene shard at a time to try and minimize the overhead.

I have made some headway patching [rucene](https://github.com/zhihu/rucene) in order to read Lucene directly from Rust. I got far enough for what I'd need (to pull out `StoredField` and even to do basic text search) but not further to things like `DocValues`. When/if I have time I may try to incorporate that here as an optimization. The danger would be with the next breaking version we'd have to again patch.

## test_data

To generate Elasticsearch data I use [elasticsearch-test-data](https://github.com/oliver006/elasticsearch-test-data). A copy of test data is kept in `tests/resources`. Here's what I used to generate the `test_data` index:

```
python es_test_data.py --es_url=http://localhost:9200 --format=title:dict:1:6,content:dict:10:20,last_update:ts,created:ts --count=1000 --dict_file=/usr/share/dict/words
```

## high level todos

- schema to schema mapping to make this a fully parameterized CLI tool. 
- java_wrapper should probably be made into a git submodule. Right now I `rsync` from another repo.