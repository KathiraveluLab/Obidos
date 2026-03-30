package com.kathiravelulab.obidos.services;

import java.util.ArrayList;
import java.util.List;

/**
 * Connector implementation for The Cancer Imaging Archive (TCIA).
 */
public class TCIAConnector implements SourceConnector {

    @Override
    public List<String> extractMetadata(String sourceURI) {
        System.out.println("TCIA: Connecting to NBIA REST API for " + sourceURI);
        List<String> metadata = new ArrayList<>();
        // Simulated metadata extraction from TCIA
        metadata.add("SOPInstanceUID: 1.3.6.1.4.1.14519.5.2.1.7009.9004.1");
        metadata.add("SeriesInstanceUID: 1.3.6.1.4.1.14519.5.2.1.7009.9004.2");
        metadata.add("Modality: CT");
        metadata.add("Manufacturer: SIEMENS");
        return metadata;
    }

    @Override
    public String downloadData(String sourceURI) {
        System.out.println("TCIA: Downloading DICOM images from " + sourceURI);
        return "/tmp/obidos/tcia_download_" + System.currentTimeMillis();
    }
}
