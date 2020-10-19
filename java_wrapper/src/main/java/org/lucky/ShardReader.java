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
    public static final String PATH_NAME = "/var/lib/elasticsearch/nodes/0/indices/9XubvRRIQjC10BJKHhG1ug/0/index/";
    public static final JsonParser JSON_PARSER = new JsonParser();

    // https://medium.com/@manis.eren/loading-elasticsearch-index-to-lucene-api-in-java-bf3206a152b9
    public static void main(String[] args) throws Exception {
        String pathName = PATH_NAME;
        System.out.println(pathName);
        System.out.println(docCount(pathName));
        IndexReader reader = getReader(pathName);

        Document doc = reader.document(1);
        // in es, you will only see _id and _source on the doc itself
        // doc.getFields().stream().forEach(f -> System.out.println(f.name()));
        final String source = doc.getBinaryValue("_source").utf8ToString();
        System.out.println(source);
        System.exit(0);

        Term t = new Term("customer_full_name", "mary");
        int freq = reader.docFreq(t);
        System.out.println("FREQ " + freq);

        queryName("mary");
    }

    public static List<String> queryName(String name) {
        final Term t = new Term("customer_full_name", name);
        final IndexReader reader = getReader(PATH_NAME);
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
                final String customer_full_name = JSON_PARSER.parse(s).getAsJsonObject()
                        .get("customer_full_name").getAsString();
                System.out.println("DOC _source " + customer_full_name);
                names.add(customer_full_name);
            }
            return names;
        } catch (IOException e) {
            System.err.println(e);
            throw new RuntimeException(e);
        }
    }
    public static int docCount(String pathName) {
        IndexReader reader = getReader(pathName);
        return reader.numDocs();
    }

    public static IndexReader getReader(String pathName) {
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
