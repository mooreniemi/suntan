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

To generate Elasticsearch data I use [elasticsearch-test-data](https://github.com/oliver006/elasticsearch-test-data). A copy of test data is kept in `tests/resources`. Here's what I used to generate the `test_data` index:

```
python es_test_data.py --es_url=http://localhost:9200 --format=title:dict:1:6,content:dict:10:20,last_update:ts,created:ts --count=1000 --dict_file=/usr/share/dict/words
```

## high level todos

- java_wrapper should probably be made into a git submodule. Right now I `rsync` from another repo.