package com.kathiravelulab.obidos;

import com.kathiravelulab.obidos.api.NorthboundController;
import com.kathiravelulab.obidos.core.ReplicaSetHolder;
import com.kathiravelulab.obidos.services.DataDownloader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import spark.Spark;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Scanner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ObidosIntegrationTest {

    private ReplicaSetHolder holder;

    @Before
    public void setUp() {
        holder = new ReplicaSetHolder();
        DataDownloader downloader = new DataDownloader(holder);
        com.kathiravelulab.obidos.storage.QueryTransformationLayer queryLayer = new com.kathiravelulab.obidos.storage.QueryTransformationLayer();
        new NorthboundController(holder, downloader, queryLayer);
    }

    @After
    public void tearDown() {
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
        conn.disconnect();

        // 3. Verify S3 Metadata
        url = new URL("http://localhost:8080/sources/metadata?uri=s3://bucket1");
        conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        assertEquals(200, conn.getResponseCode());
        s = new Scanner(conn.getInputStream()).useDelimiter("\\A");
        response = s.hasNext() ? s.next() : "";
        assertTrue(response.contains("ObjectKey: study_001/img_01.dcm"));
        conn.disconnect();
    }

    @Test
    public void testQueryEndpoint() throws Exception {
        Thread.sleep(1000);
        String sql = "SELECT * FROM tcia.collection1 WHERE Modality='CT'";
        URL url = new URL("http://localhost:8080/query");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.getOutputStream().write(sql.getBytes());
        
        assertEquals(200, conn.getResponseCode());
        
        Scanner s = new Scanner(conn.getInputStream()).useDelimiter("\\A");
        String response = s.hasNext() ? s.next() : "";
        assertTrue(response.contains("Simulated result for query"));
        conn.disconnect();
    }
}
