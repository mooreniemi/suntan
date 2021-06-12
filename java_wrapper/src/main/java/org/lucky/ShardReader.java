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
import java.util.List;

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
