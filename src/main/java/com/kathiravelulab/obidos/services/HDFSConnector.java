package com.kathiravelulab.obidos.services;

import java.util.ArrayList;
import java.util.List;

/**
 * Connector implementation for Hadoop Distributed File System (HDFS).
 */
public class HDFSConnector implements SourceConnector {

    @Override
    public List<String> extractMetadata(String sourceURI) {
        System.out.println("HDFS: Accessing namenode for path " + sourceURI);
        List<String> metadata = new ArrayList<>();
        // Simulated file metadata from HDFS
        metadata.add("Path: " + sourceURI);
        metadata.add("Owner: hadoop");
        metadata.add("Group: supergroup");
        metadata.add("Permission: rw-r--r--");
        metadata.add("Replication: 3");
        return metadata;
    }

    @Override
    public String downloadData(String sourceURI) {
        System.out.println("HDFS: Copying file from " + sourceURI + " to local filesystem");
        return "/tmp/obidos/hdfs_download_" + System.currentTimeMillis();
    }
}
