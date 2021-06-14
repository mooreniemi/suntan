```
        .
      \ | /
    '-.;;;.-'
   -==;;;;;==-
    .-';;;'-.
      / | \
jgs     '
```
# suntan

This is a proof-of-concept CLI tool to dump Elasticsearch Lucene shards into Tantivy. There's also a couple `examples` of calling Lucene through Rust for querying. You provide input, output, and the Tantivy output schema and the tool dumps into it. Your Tantivy schema must be just like or a subset of the Elasticsearch schema. Not all types are supported yet.

```
# this creates a tantivy index at /tmp/suntan/tantivy-idx given the test resources
suntan -i tests/resources/es-idx/ -s tests/resources/tantivy-schema.json
```

## cli

If run without arguments the cli will attempt to use test resources.

```
./target/debug/suntan --help
suntan 0.1.2
Alex MN. <moore.niemi@gmail.com>
This is a tool for dumping Elasticsearch Lucene shards into Tantivy indices. Elasticsearch stores
fields in a particular way which is why it's not "just" Lucene

USAGE:
    suntan [OPTIONS]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -i, --input <input>                The location of the Elasticsearch Lucene index [default:
                                       tests/resources/es-idx]
    -o, --output <output>              The location of the Tantivy output index [default:
                                       /tmp/suntan/tantivy-idx]
    -s, --schema-path <schema-path>    The location of the Tantivy schema [default:
                                       tests/resources/tantivy-schema.json]
    -t, --test-query <test-query>      To test that the docs still match, this is sent in as test
                                       query [default: lint]
```

## about

We rely on [`j4rs`](https://github.com/astonbitecode/j4rs). This is a trade-off. On one hand, it means it's easy to deal with Lucene codecs changing version since you can depend directly on the underlying Java code to read. On the other hand, it means that you have an inter-process communication layer between Rust and the JVM, which is slow to process. In the `main` function we make use of a batching iterator which pulls 1000 docs out of the Lucene shard at a time to try and minimize the overhead.

I have made some headway patching [rucene](https://github.com/zhihu/rucene) in order to read Lucene directly from Rust. I got far enough for what I'd need (to pull out `StoredField` and even to do basic text search) but not further to things like `DocValues`. When/if I have time I may try to incorporate that here as an optimization. The danger would be with the next breaking version we'd have to again patch.

## development

I worked with Java 8 and [maven](https://maven.apache.org/what-is-maven.html). 

`mvn package` is all you need to generate the jar. Then `build.rs` will copy it into `jassets/suntan.jar`.

## test_data

To generate Elasticsearch data I use [elasticsearch-test-data](https://github.com/oliver006/elasticsearch-test-data). A copy of test data is kept in `tests/resources`. Here's what I used to generate the `test_data` index:

```
python es_test_data.py --es_url=http://localhost:9200 --format=title:dict:1:6,content:dict:10:20,last_update:ts,created:ts --count=1000 --dict_file=/usr/share/dict/words
```

On Linux, Elasticsearch lives under `/var/lib`. Here's what the origin of the test data looks like:

```
ls -lah /var/lib/elasticsearch/nodes/0/indices/TvG2djXSQgqg4PWZSrv2wQ/0/index//0/indices/TvG2djXSQgqg4PWZSrv2wQ/0/index/
total 3.8M
drwxr-xr-x 2 elasticsearch elasticsearch 4.0K Jun 12 22:17 .
drwxr-xr-x 5 elasticsearch elasticsearch 4.0K Jun 12 21:21 ..
-rw-r--r-- 1 elasticsearch elasticsearch  405 Jun 12 21:21 _0.cfe
-rw-r--r-- 1 elasticsearch elasticsearch 802K Jun 12 21:21 _0.cfs
-rw-r--r-- 1 elasticsearch elasticsearch  367 Jun 12 21:21 _0.si
-rw-r--r-- 1 elasticsearch elasticsearch  405 Jun 12 22:17 _1.cfe
-rw-r--r-- 1 elasticsearch elasticsearch 3.0M Jun 12 22:17 _1.cfs
-rw-r--r-- 1 elasticsearch elasticsearch  367 Jun 12 22:17 _1.si
-rw-r--r-- 1 elasticsearch elasticsearch  423 Jun 12 22:17 segments_6
-rw-r--r-- 1 elasticsearch elasticsearch    0 Jun 12 21:21 write.lock
```

This is easy enough to push into the `test/resources`:

```
rsync -r /var/lib/elasticsearch/nodes/0/indices/TvG2djXSQgqg4PWZSrv2wQ/0/index/ tests/resources
```

## high level todos

- `HierarchicalFacet` and `DateTime` support in the schema mapping.
- Remapping field names on export.
- java_wrapper should probably be made into a git submodule. Right now I `rsync` from another repo.