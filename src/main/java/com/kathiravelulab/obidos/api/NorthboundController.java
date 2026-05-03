package com.kathiravelulab.obidos.api;

import com.kathiravelulab.obidos.core.ReplicaSetHolder;
import com.kathiravelulab.obidos.model.ReplicaSet;
import com.kathiravelulab.obidos.services.DataDownloader;
import spark.Spark;
import static spark.Spark.*;

import com.kathiravelulab.obidos.services.NearDuplicateDetector;
import com.kathiravelulab.obidos.services.FederationService;

import com.google.gson.Gson;

/**
 * RESTful interface for Óbidos (Northbound API).
 * Implements CRUD on replica sets as described in the papers.
 */
public class NorthboundController {

    private final ReplicaSetHolder replicaSetHolder;
    private final DataDownloader dataDownloader;
    private final com.kathiravelulab.obidos.storage.QueryTransformationLayer queryLayer;
    private final NearDuplicateDetector duplicateDetector;
    private final com.kathiravelulab.obidos.storage.MetadataIndexer indexer;
    private final FederationService federationService;
    private final Gson gson = new Gson();

    public NorthboundController(ReplicaSetHolder replicaSetHolder, DataDownloader dataDownloader, 
                                com.kathiravelulab.obidos.storage.QueryTransformationLayer queryLayer,
                                NearDuplicateDetector duplicateDetector,
                                com.kathiravelulab.obidos.storage.MetadataIndexer indexer,
                                FederationService federationService) {
        this.replicaSetHolder = replicaSetHolder;
        this.dataDownloader = dataDownloader;
        this.queryLayer = queryLayer;
        this.duplicateDetector = duplicateDetector;
        this.indexer = indexer;
        this.federationService = federationService;
        setupRoutes();
    }

    private void setupRoutes() {
        port(8080);

        // CREATE: POST /replicasets?userID=...
        post("/replicasets", (req, res) -> {
            String userID = req.queryParams("userID");
            if (userID == null) {
                res.status(400);
                return "Missing userID";
            }
            
            ReplicaSet rs;
            String body = req.body();
            if (body != null && !body.isEmpty()) {
                rs = gson.fromJson(body, ReplicaSet.class);
            } else {
                rs = new ReplicaSet();
            }
            
            replicaSetHolder.addReplicaSet(userID, rs);
            dataDownloader.createReplicaSet(rs);
            res.status(201);
            return gson.toJson(rs);
        });

        // UPDATE: PUT /replicasets/:id/datasources
        Spark.put("/replicasets/:id/datasources", (req, res) -> {
            String rsID = req.params(":id");
            ReplicaSet rs = replicaSetHolder.getReplicaSet(rsID);
            if (rs == null) {
                res.status(404);
                return "ReplicaSet not found";
            }
            
            String[] sources = gson.fromJson(req.body(), String[].class);
            for (String source : sources) {
                rs.addDataSource(source);
            }
            
            replicaSetHolder.updateReplicaSet(rs);
            dataDownloader.createReplicaSet(rs);
            return gson.toJson(rs);
        });

        // RETRIEVE: GET /replicasets/:id
        get("/replicasets/:id", (req, res) -> {
            String rsID = req.params(":id");
            ReplicaSet rs = replicaSetHolder.getReplicaSet(rsID);
            if (rs == null) {
                res.status(404);
                return "ReplicaSet not found";
            }
            return gson.toJson(rs);
        });

        // LIST: GET /tenants/:userID/replicasets
        get("/tenants/:userID/replicasets", (req, res) -> {
            String userID = req.params(":userID");
            return gson.toJson(replicaSetHolder.getUserReplicaSets(userID));
        });

        // METADATA: GET /sources/metadata
        get("/sources/metadata", (req, res) -> {
            String uri = req.queryParams("uri");
            return gson.toJson(replicaSetHolder.getVirtualProxy(uri));
        });

        // QUERY: POST /query/:rsId
        post("/query/:rsId", (req, res) -> {
            String rsId = req.params(":rsId");
            String sql = req.body();
            if (sql == null || sql.isEmpty()) {
                res.status(400);
                return "Missing SQL query in body";
            }
            return gson.toJson(queryLayer.rewriteAndExecuteQuery(rsId, sql));
        });

        // LAZY LOAD: POST /sources/load
        post("/sources/load", (req, res) -> {
            String uri = req.queryParams("uri");
            if (uri == null || uri.isEmpty()) {
                res.status(400);
                return "Missing uri parameter";
            }
            String location = dataDownloader.fetchPhysicalData(uri);
            if (location == null) {
                res.status(404);
                return "Source connector not found or failed";
            }
            return "{\"status\":\"LOADED\", \"location\":\"" + location + "\"}";
        });

        // DUPLICATES: GET /duplicates
        get("/duplicates", (req, res) -> {
            return gson.toJson(duplicateDetector.detectDuplicates());
        });

        // SEARCH METADATA: GET /search
        get("/search", (req, res) -> {
            String q = req.queryParams("q");
            if (q == null || q.isEmpty()) {
                res.status(400);
                return "Missing query parameter 'q'";
            }
            return gson.toJson(indexer.searchMetadata(q));
        });

        // FEDERATION: POST /federation/share/:id?peer=...
        post("/federation/share/:id", (req, res) -> {
            String id = req.params(":id");
            String peer = req.queryParams("peer");
            return federationService.shareReplicaSet(id, peer);
        });

        // FEDERATION: POST /federation/pull/:id?peer=...
        post("/federation/pull/:id", (req, res) -> {
            String id = req.params(":id");
            String peer = req.queryParams("peer");
            return gson.toJson(federationService.pullReplicaSet(id, peer));
        });

        // Simple Health Check
        get("/health", (req, res) -> "Óbidos is running");
    }
    
    public static void main(String[] args) {
        ReplicaSetHolder holder = new ReplicaSetHolder();
        com.kathiravelulab.obidos.storage.MetadataIndexer indexer = new com.kathiravelulab.obidos.storage.MetadataIndexer();
        DataDownloader downloader = new DataDownloader(holder, indexer);
        com.kathiravelulab.obidos.storage.QueryTransformationLayer queryLayer = new com.kathiravelulab.obidos.storage.QueryTransformationLayer(holder);
        NearDuplicateDetector duplicateDetector = new NearDuplicateDetector(holder);
        FederationService federationService = new FederationService(holder);
        new NorthboundController(holder, downloader, queryLayer, duplicateDetector, indexer, federationService);
        System.out.println("Óbidos Northbound API started on port 8080");
    }
}
