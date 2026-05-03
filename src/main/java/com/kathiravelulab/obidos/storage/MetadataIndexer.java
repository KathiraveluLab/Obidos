package com.kathiravelulab.obidos.storage;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Integrated Data Repository & Metadata Index layer using Apache Lucene.
 * Provides persistent disk-backed indexing for complex metadata searches.
 */
public class MetadataIndexer {

    private final String indexPath;
    private final StandardAnalyzer analyzer;

    public MetadataIndexer(String indexPath) {
        this.indexPath = indexPath;
        this.analyzer = new StandardAnalyzer();
    }

    public MetadataIndexer() {
        this("./obidos-index");
    }

    public void indexMetadata(String sourceURI, List<String> metadataEntries) {
        try (Directory dir = FSDirectory.open(Paths.get(indexPath));
             IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(analyzer))) {
             
             Document doc = new Document();
             doc.add(new StringField("sourceURI", sourceURI, Field.Store.YES));
             
             // Combine metadata for full-text search
             String content = String.join(" ", metadataEntries);
             doc.add(new TextField("metadata", content, Field.Store.YES));
             
             // Update (delete and add) to avoid duplicate documents for the same URI
             writer.updateDocument(new org.apache.lucene.index.Term("sourceURI", sourceURI), doc);
             System.out.println("MetadataIndexer: Indexed " + sourceURI + " into persistent repository.");
        } catch (IOException e) {
             System.err.println("Error indexing metadata: " + e.getMessage());
        }
    }

    public List<String> searchMetadata(String queryString) {
        List<String> results = new ArrayList<>();
        try (Directory dir = FSDirectory.open(Paths.get(indexPath));
             DirectoryReader reader = DirectoryReader.open(dir)) {
             
             IndexSearcher searcher = new IndexSearcher(reader);
             QueryParser parser = new QueryParser("metadata", analyzer);
             Query query = parser.parse(queryString);
             
             TopDocs topDocs = searcher.search(query, 10);
             for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                 Document doc = searcher.doc(scoreDoc.doc);
                 results.add(doc.get("sourceURI"));
             }
        } catch (org.apache.lucene.index.IndexNotFoundException e) {
             System.out.println("Index not found. Please index some metadata first.");
        } catch (Exception e) {
             System.err.println("Error searching metadata: " + e.getMessage());
        }
        return results;
    }
}
