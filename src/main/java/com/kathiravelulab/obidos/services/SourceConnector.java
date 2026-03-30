package com.kathiravelulab.obidos.services;

import java.util.List;

/**
 * Interface for external data source connectivity.
 * Part of the Westbound API in the Óbidos architecture.
 */
public interface SourceConnector {
    /**
     * Extracts metadata from the specified source URI.
     * @param sourceURI The URI of the data source (e.g., tcia://collection/series).
     * @return A list of metadata strings (virtual proxies).
     */
    List<String> extractMetadata(String sourceURI);

    /**
     * Downloads data from the specified source URI.
     * @param sourceURI The URI of the data source.
     * @return Physical data location or confirmation.
     */
    String downloadData(String sourceURI);
}
