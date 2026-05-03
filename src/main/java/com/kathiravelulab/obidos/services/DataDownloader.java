package com.kathiravelulab.obidos.services;

import com.kathiravelulab.obidos.model.ReplicaSet;

/**
 * Westbound API: Source Data Downloader.
 * Responsible for selectively loading metadata and data from data sources.
 * As described in Óbidos software architecture (Figure 2, DMAH 17).
 */
public class DataDownloader {
    private final com.kathiravelulab.obidos.core.ReplicaSetHolder replicaSetHolder;
    private final com.kathiravelulab.obidos.storage.MetadataIndexer indexer;

    public DataDownloader(com.kathiravelulab.obidos.core.ReplicaSetHolder replicaSetHolder, com.kathiravelulab.obidos.storage.MetadataIndexer indexer) {
        this.replicaSetHolder = replicaSetHolder;
        this.indexer = indexer;
    }

    public void loadData(String dataSource, String virtualReplica, String userQuery) {
        System.out.println("Westbound API: Selective loading from " + dataSource + " for " + virtualReplica);
        
        SourceConnector connector = getConnector(dataSource);
        if (connector != null) {
            java.util.List<String> metadata = connector.extractMetadata(dataSource);
            replicaSetHolder.addVirtualProxy(dataSource, metadata);
            if (indexer != null) {
                indexer.indexMetadata(dataSource, metadata);
            }
            System.out.println("Loaded " + metadata.size() + " virtual proxy attributes into repository.");
        } else {
            System.out.println("Unknown data source protocol: " + dataSource);
        }
    }

    public String fetchPhysicalData(String sourceURI) {
        System.out.println("Westbound API: Fetching physical data for " + sourceURI);
        SourceConnector connector = getConnector(sourceURI);
        if (connector != null) {
            String physicalLocation = connector.downloadData(sourceURI);
            
            // Replace virtual proxy with loaded object reference
            java.util.List<String> metadata = new java.util.ArrayList<>(replicaSetHolder.getVirtualProxy(sourceURI));
            boolean updated = false;
            if (!metadata.contains("Status: LOADED")) {
                metadata.add("Status: LOADED");
                updated = true;
            }
            if (!metadata.contains("PhysicalLocation: " + physicalLocation)) {
                metadata.add("PhysicalLocation: " + physicalLocation);
                updated = true;
            }
            
            if (updated) {
                replicaSetHolder.addVirtualProxy(sourceURI, metadata);
                if (indexer != null) {
                    indexer.indexMetadata(sourceURI, metadata);
                }
                System.out.println("Virtual proxy replaced with physical data at " + physicalLocation);
            }
            return physicalLocation;
        }
        return null;
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
        } else if (sourceURI.startsWith("hdfs://")) {
            return new HDFSConnector();
        }
        return null;
    }
}
