package com.kathiravelulab.obidos.services;

import com.kathiravelulab.obidos.model.ReplicaSet;

/**
 * Westbound API: Source Data Downloader.
 * Responsible for selectively loading metadata and data from data sources.
 * As described in Óbidos software architecture (Figure 2, DMAH 17).
 */
public class DataDownloader {
    private final com.kathiravelulab.obidos.core.ReplicaSetHolder replicaSetHolder;

    public DataDownloader(com.kathiravelulab.obidos.core.ReplicaSetHolder replicaSetHolder) {
        this.replicaSetHolder = replicaSetHolder;
    }

    public void loadData(String dataSource, String virtualReplica, String userQuery) {
        System.out.println("Westbound API: Selective loading from " + dataSource + " for " + virtualReplica);
        
        SourceConnector connector = getConnector(dataSource);
        if (connector != null) {
            java.util.List<String> metadata = connector.extractMetadata(dataSource);
            replicaSetHolder.addVirtualProxy(dataSource, metadata);
            System.out.println("Loaded " + metadata.size() + " virtual proxy attributes into repository.");
        } else {
            System.out.println("Unknown data source protocol: " + dataSource);
        }
    }
    
    public void createReplicaSet(ReplicaSet rs) {
        System.out.println("Processing new ReplicaSet pointers: " + rs.getReplicaSetID());
        for (String source : rs.getDataSources()) {
            loadData(source, rs.getReplicaSetID(), "SELECT * FROM " + source);
        }
    }

    private SourceConnector getConnector(String sourceURI) {
        if (sourceURI.startsWith("tcia://")) {
            return new TCIAConnector();
        } else if (sourceURI.startsWith("s3://")) {
            return new S3Connector();
        }
        return null;
    }
}
