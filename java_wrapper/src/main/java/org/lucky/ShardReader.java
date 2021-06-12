package org.lucky;

import com.google.gson.JsonParser;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class ShardReader {
    public static final JsonParser JSON_PARSER = new JsonParser();

    private IndexReader reader;
    public IndexReader getReader() {
        return reader;
    }

    public ShardReader(String pathName) {
        this.reader = initReader(pathName);
    }

    // https://medium.com/@manis.eren/loading-elasticsearch-index-to-lucene-api-in-java-bf3206a152b9
    public static void main(String[] args) throws Exception {
        final String pathName = "/var/lib/elasticsearch/nodes/0/indices/9XubvRRIQjC10BJKHhG1ug/0/index/";
        final String pathName2 = "/var/lib/elasticsearch/nodes/0/indices/0aoMuSE3QK6UZFV1htSczw/0/index/";
        ShardReader sr = new ShardReader(pathName2);
        System.out.println("Docs in Reader count: " + sr.docCount());

        Document doc = sr.getReader().document(1);
        // in es, you will only see _id and _source on the doc itself
        // doc.getFields().stream().forEach(f -> System.out.println(f.name()));
        final String source = doc.getBinaryValue("_source").utf8ToString();
        System.out.println(source);
        // System.exit(0);

        // this depends on your local example schema
        Term t = new Term("name", "magnam");
        int freq = sr.getReader().docFreq(t);
        System.out.println("FREQ " + freq);

        sr.queryName("name", "magnam");
    }

    // Loading all docs into memory is a bit risky but sending them all over at once
    // might be better than having to do a full ipc per each hasNext in rust
    public List<String> allDocSource() {
        DocSourceIterator iterator = new DocSourceIterator(reader);
        List<String> actualList = new ArrayList<String>();
        while (iterator.hasNext()) {
            actualList.add(iterator.next());
        }
        return actualList;
    }

    public class DocSourceIterator implements Iterator<String> {
        IndexReader reader;
        int pos = 0;

        public DocSourceIterator(IndexReader reader) {
            this.reader = reader;
        }

        @Override
        public boolean hasNext() {
            return (pos < reader.maxDoc());
        }

        @Override
        public String next() {
            if (hasNext()) {
                try {
                    // TODO: need to check isDeleted somehow?
                    // the below call can throw
                    Document doc = reader.document(pos);
                    // NOTE: where Elasticsearch stores the underlying document
                    String source = doc.getBinaryValue("_source").utf8ToString();
                    pos++;
                    return source;
                } catch (IOException exception) {
                    // FIXME: should prob be diff error type
                    throw new NoSuchElementException();
                }
            } else {
                throw new NoSuchElementException();
            }
        }
    }

    public DocSourceIterator iterator() {
        return new DocSourceIterator(reader);
    }

    public List<String> queryName(String field, String value) {
        final Term t = new Term(field, value);
        final IndexSearcher searcher = new IndexSearcher(reader);
        final Query query = new TermQuery(t);

        List names = new ArrayList<String>();
        try {
            TopDocs tops = searcher.search(query, 10);
            ScoreDoc[] scoreDoc = tops.scoreDocs;
            for (ScoreDoc score : scoreDoc){
                Document d = reader.document(score.doc);
                System.out.println("DOC " + score.doc + " SCORE " + score.score);
                final String s = d.getBinaryValue("_source").utf8ToString();
                final String name = JSON_PARSER.parse(s).getAsJsonObject()
                        .get(field).getAsString();
                System.out.println("DOC _source " + name);
                names.add(name);
            }
            return names;
        } catch (IOException e) {
            System.err.println(e);
            throw new RuntimeException(e);
        }
    }
    public int docCount() {
        return reader.numDocs();
    }

    IndexReader initReader(String pathName) {
        try {
            File path = new File(pathName);
            System.out.println(pathName + " exists? " + path.exists());
            Directory index = FSDirectory.open(path.toPath());
            IndexReader reader = DirectoryReader.open(index);
            return reader;
        } catch (Exception e) {
            System.err.println(e.getMessage());
            throw new RuntimeException();
        }
    }
}
