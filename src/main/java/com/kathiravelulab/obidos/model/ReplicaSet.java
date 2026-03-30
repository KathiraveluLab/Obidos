package com.kathiravelulab.obidos.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Represents a pointer to datasets from various data sources.
 * As described in DAPD 18 and DMAH 17 papers.
 */
public class ReplicaSet implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String replicaSetID;
    private final List<String> dataSources;
    private final long timestamp;

    public ReplicaSet() {
        this.replicaSetID = UUID.randomUUID().toString();
        this.dataSources = new ArrayList<>();
        this.timestamp = System.currentTimeMillis();
    }

    public ReplicaSet(String replicaSetID, List<String> dataSources) {
        this.replicaSetID = replicaSetID;
        this.dataSources = new ArrayList<>(dataSources);
        this.timestamp = System.currentTimeMillis();
    }

    public String getReplicaSetID() {
        return replicaSetID;
    }

    public List<String> getDataSources() {
        return dataSources;
    }

    public void addDataSource(String dataSource) {
        this.dataSources.add(dataSource);
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "ReplicaSet{" +
                "replicaSetID='" + replicaSetID + '\'' +
                ", dataSources=" + dataSources +
                ", timestamp=" + timestamp +
                '}';
    }
}
