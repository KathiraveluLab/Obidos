package com.kathiravelulab.obidos.api;

import com.kathiravelulab.obidos.core.ReplicaSetHolder;
import com.kathiravelulab.obidos.model.ReplicaSet;
import com.kathiravelulab.obidos.services.DataDownloader;
import spark.Spark;
import static spark.Spark.*;

import com.google.gson.Gson;

/**
 * RESTful interface for Óbidos (Northbound API).
 * Implements CRUD on replica sets as described in the papers.
 */
public class NorthboundController {

    private final ReplicaSetHolder replicaSetHolder;
    private final DataDownloader dataDownloader;
    private final com.kathiravelulab.obidos.storage.QueryTransformationLayer queryLayer;
    private final Gson gson = new Gson();

    public NorthboundController(ReplicaSetHolder replicaSetHolder, DataDownloader dataDownloader, 
                                com.kathiravelulab.obidos.storage.QueryTransformationLayer queryLayer) {
        this.replicaSetHolder = replicaSetHolder;
        this.dataDownloader = dataDownloader;
        this.queryLayer = queryLayer;
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

        // QUERY: POST /query
        post("/query", (req, res) -> {
            String sql = req.body();
            if (sql == null || sql.isEmpty()) {
                res.status(400);
                return "Missing SQL query in body";
            }
            return gson.toJson(queryLayer.executeQuery(sql));
        });

        // Simple Health Check
        get("/health", (req, res) -> "Óbidos is running");
    }
    
    public static void main(String[] args) {
        ReplicaSetHolder holder = new ReplicaSetHolder();
        DataDownloader downloader = new DataDownloader(holder);
        com.kathiravelulab.obidos.storage.QueryTransformationLayer queryLayer = new com.kathiravelulab.obidos.storage.QueryTransformationLayer();
        new NorthboundController(holder, downloader, queryLayer);
        System.out.println("Óbidos Northbound API started on port 8080");
    }
}
