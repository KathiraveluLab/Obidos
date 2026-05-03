package com.kathiravelulab.obidos;

import com.kathiravelulab.obidos.api.NorthboundController;
import com.kathiravelulab.obidos.core.ReplicaSetHolder;
import com.kathiravelulab.obidos.services.DataDownloader;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import spark.Spark;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Scanner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ObidosIntegrationTest {

    private static ReplicaSetHolder holder;

    @BeforeClass
    public static void setUp() {
        holder = new ReplicaSetHolder();
        com.kathiravelulab.obidos.storage.MetadataIndexer indexer = new com.kathiravelulab.obidos.storage.MetadataIndexer("./target/obidos-test-index");
        DataDownloader downloader = new DataDownloader(holder, indexer);
        com.kathiravelulab.obidos.storage.QueryTransformationLayer queryLayer = new com.kathiravelulab.obidos.storage.QueryTransformationLayer(holder);
        com.kathiravelulab.obidos.services.NearDuplicateDetector duplicateDetector = new com.kathiravelulab.obidos.services.NearDuplicateDetector(holder);
        com.kathiravelulab.obidos.services.FederationService federationService = new com.kathiravelulab.obidos.services.FederationService(holder);
        new NorthboundController(holder, downloader, queryLayer, duplicateDetector, indexer, federationService);
        Spark.awaitInitialization();
    }

    @AfterClass
    public static void tearDown() {
        if (holder != null) {
            holder.stop();
        }
        Spark.stop();
        Spark.awaitStop();
    }

    @Test
    public void testHealthCheck() throws Exception {
        Thread.sleep(2000); // Wait for server to start
        URL url = new URL("http://localhost:8080/health");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        
        Scanner scanner = new Scanner(conn.getInputStream());
        String response = scanner.useDelimiter("\\A").next();
        scanner.close();
        
        assertTrue(response.contains("Óbidos is running"));
    }

    @Test
    public void testReplicaSetLifecycle() throws Exception {
        Thread.sleep(1000);
        // 1. Create a ReplicaSet
        // Use replicaSetID instead of id to match the model
        String payload = "{\"replicaSetID\":\"rs1\", \"dataSources\":[\"hdfs://data/1\", \"hdfs://data/2\"]}";
        URL url = new URL("http://localhost:8080/replicasets?userID=user1");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/json");
        conn.getOutputStream().write(payload.getBytes());
        
        assertEquals(201, conn.getResponseCode());
        conn.disconnect();

        // 2. Retrieve the ReplicaSet
        url = new URL("http://localhost:8080/replicasets/rs1");
        conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        
        assertEquals(200, conn.getResponseCode());
        
        Scanner s = new Scanner(conn.getInputStream()).useDelimiter("\\A");
        String response = s.hasNext() ? s.next() : "";
        assertTrue(response.contains("rs1"));
        s.close();
        conn.disconnect();

        // 3. List ReplicaSets for tenant
        url = new URL("http://localhost:8080/tenants/user1/replicasets");
        conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        
        assertEquals(200, conn.getResponseCode());
        s = new Scanner(conn.getInputStream()).useDelimiter("\\A");
        response = s.hasNext() ? s.next() : "";
        assertTrue(response.contains("rs1"));
        conn.disconnect();
    }

    @Test
    public void testConnectorIntegration() throws Exception {
        Thread.sleep(1000);
        // 1. Create a ReplicaSet with TCIA and S3 sources
        String payload = "{\"replicaSetID\":\"rs_connect\", \"dataSources\":[\"tcia://collection1\", \"s3://bucket1\"]}";
        URL url = new URL("http://localhost:8080/replicasets?userID=user2");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/json");
        conn.getOutputStream().write(payload.getBytes());
        assertEquals(201, conn.getResponseCode());
        conn.disconnect();

        Thread.sleep(1000); // Wait for DataDownloader to process

        // 2. Verify TCIA Metadata
        url = new URL("http://localhost:8080/sources/metadata?uri=tcia://collection1");
        conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        assertEquals(200, conn.getResponseCode());
        Scanner s = new Scanner(conn.getInputStream()).useDelimiter("\\A");
        String response = s.hasNext() ? s.next() : "";
        assertTrue(response.contains("Modality: CT"));
        s.close();
        conn.disconnect();

        // 3. Verify S3 Metadata
        url = new URL("http://localhost:8080/sources/metadata?uri=s3://bucket1");
        conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        assertEquals(200, conn.getResponseCode());
        s = new Scanner(conn.getInputStream()).useDelimiter("\\A");
        response = s.hasNext() ? s.next() : "";
        assertTrue(response.contains("ObjectKey: study_001/img_01.dcm"));
        s.close();
        conn.disconnect();
    }

    @Test
    public void testQueryEndpoint() throws Exception {
        Thread.sleep(1000);

        // 1. Ensure replicaSet rs_query exists
        String payload = "{\"replicaSetID\":\"rs_query\", \"dataSources\":[\"tcia://collection1\", \"s3://bucket1\"]}";
        URL rsUrl = new URL("http://localhost:8080/replicasets?userID=user3");
        HttpURLConnection rsConn = (HttpURLConnection) rsUrl.openConnection();
        rsConn.setRequestMethod("POST");
        rsConn.setDoOutput(true);
        rsConn.setRequestProperty("Content-Type", "application/json");
        rsConn.getOutputStream().write(payload.getBytes());
        assertEquals(201, rsConn.getResponseCode());
        rsConn.disconnect();

        // 2. Query the replica set
        String sql = "SELECT * FROM rs_query WHERE Modality='CT'";
        URL url = new URL("http://localhost:8080/query/rs_query");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.getOutputStream().write(sql.getBytes());
        
        assertEquals(200, conn.getResponseCode());
        
        Scanner s = new Scanner(conn.getInputStream()).useDelimiter("\\A");
        String response = s.hasNext() ? s.next() : "";
        
        assertTrue(response.contains("status"));
        assertTrue(response.contains("Simulated result for query"));
        assertTrue(response.contains("dfs.tcia.`collection1`"));
        assertTrue(response.contains("dfs.s3.`bucket1`"));
        s.close();
        conn.disconnect();
    }

    @Test
    public void testDuplicateDetection() throws Exception {
        Thread.sleep(1000);
        // Force add some duplicate virtual proxies
        holder.addVirtualProxy("tcia://sim1", java.util.Arrays.asList("A", "B", "C"));
        holder.addVirtualProxy("tcia://sim2", java.util.Arrays.asList("A", "B", "C"));

        URL url = new URL("http://localhost:8080/duplicates");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        assertEquals(200, conn.getResponseCode());

        Scanner s = new Scanner(conn.getInputStream()).useDelimiter("\\A");
        String response = s.hasNext() ? s.next() : "";

        assertTrue(response.contains("tcia://sim1"));
        assertTrue(response.contains("tcia://sim2"));
        assertTrue(response.contains("similarityScore"));
        
        s.close();
        conn.disconnect();
    }

    @Test
    public void testMetadataSearch() throws Exception {
        Thread.sleep(1000);
        
        // Ensure some metadata is indexed
        String payload = "{\"replicaSetID\":\"rs_search\", \"dataSources\":[\"tcia://search_col\"]}";
        URL rsUrl = new URL("http://localhost:8080/replicasets?userID=user4");
        HttpURLConnection rsConn = (HttpURLConnection) rsUrl.openConnection();
        rsConn.setRequestMethod("POST");
        rsConn.setDoOutput(true);
        rsConn.setRequestProperty("Content-Type", "application/json");
        rsConn.getOutputStream().write(payload.getBytes());
        rsConn.getResponseCode();
        rsConn.disconnect();

        Thread.sleep(2000); // Allow Lucene to write to disk

        // Search for 'Modality' which is added by TCIAConnector
        URL url = new URL("http://localhost:8080/search?q=Modality");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        assertEquals(200, conn.getResponseCode());

        Scanner s = new Scanner(conn.getInputStream()).useDelimiter("\\A");
        String response = s.hasNext() ? s.next() : "";

        assertTrue(response.contains("tcia://search_col"));
        
        s.close();
        conn.disconnect();
    }

    @Test
    public void testLazyLoading() throws Exception {
        Thread.sleep(1000);
        
        // 1. Create proxy
        holder.addVirtualProxy("s3://lazybucket", java.util.Arrays.asList("SomeMeta"));
        
        // 2. Trigger lazy load
        URL url = new URL("http://localhost:8080/sources/load?uri=s3://lazybucket");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");

        assertEquals(200, conn.getResponseCode());

        Scanner s = new Scanner(conn.getInputStream()).useDelimiter("\\A");
        String response = s.hasNext() ? s.next() : "";
        
        assertTrue(response.contains("LOADED"));
        assertTrue(response.contains("location"));
        s.close();
        conn.disconnect();

        // 3. Verify metadata updated in holder
        java.util.List<String> updatedMeta = holder.getVirtualProxy("s3://lazybucket");
        assertTrue(updatedMeta.contains("Status: LOADED"));
        boolean hasPhysical = updatedMeta.stream().anyMatch(m -> m.startsWith("PhysicalLocation: /tmp/obidos/s3_download_"));
        assertTrue(hasPhysical);
    }

    @Test
    public void testFederatedSharing() throws Exception {
        Thread.sleep(1000);
        
        // 1. Create a ReplicaSet to be shared
        String payload = "{\"replicaSetID\":\"rs_p2p\", \"dataSources\":[\"tcia://p2p_source\"]}";
        URL url = new URL("http://localhost:8080/replicasets?userID=userP2P");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/json");
        conn.getOutputStream().write(payload.getBytes());
        assertEquals(201, conn.getResponseCode());
        conn.disconnect();

        // 2. Simulate pulling it back as a different instance (using self as peer)
        URL pullUrl = new URL("http://localhost:8080/federation/pull/rs_p2p?peer=http://localhost:8080");
        HttpURLConnection pullConn = (HttpURLConnection) pullUrl.openConnection();
        pullConn.setRequestMethod("POST");

        assertEquals(200, pullConn.getResponseCode());

        Scanner s = new Scanner(pullConn.getInputStream()).useDelimiter("\\A");
        String response = s.hasNext() ? s.next() : "";
        
        assertTrue(response.contains("rs_p2p"));
        assertTrue(response.contains("tcia://p2p_source"));
        s.close();
        pullConn.disconnect();
    }
}
