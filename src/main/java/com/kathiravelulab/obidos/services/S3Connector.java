package com.kathiravelulab.obidos.services;

import java.util.ArrayList;
import java.util.List;

/**
 * Connector implementation for AWS S3.
 */
public class S3Connector implements SourceConnector {

    @Override
    public List<String> extractMetadata(String sourceURI) {
        System.out.println("S3: Listing objects in bucket " + sourceURI);
        List<String> metadata = new ArrayList<>();
        // Simulated object metadata from S3
        metadata.add("Bucket: " + sourceURI);
        metadata.add("ObjectKey: study_001/img_01.dcm");
        metadata.add("Size: 5242880");
        metadata.add("ETag: \"7bba12f...\"");
        return metadata;
    }

    @Override
    public String downloadData(String sourceURI) {
        System.out.println("S3: Fetching object from " + sourceURI);
        return "/tmp/obidos/s3_download_" + System.currentTimeMillis();
    }
}
