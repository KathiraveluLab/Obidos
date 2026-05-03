package com.kathiravelulab.obidos.services;

import com.kathiravelulab.obidos.core.ReplicaSetHolder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Near Duplicate Detection for metadata virtual proxies.
 * As described in DMAH 17 paper.
 */
public class NearDuplicateDetector {
    private final ReplicaSetHolder holder;
    private final double similarityThreshold;

    public NearDuplicateDetector(ReplicaSetHolder holder, double similarityThreshold) {
        this.holder = holder;
        this.similarityThreshold = similarityThreshold;
    }

    public NearDuplicateDetector(ReplicaSetHolder holder) {
        this(holder, 0.8); // Default 80% similarity threshold
    }

    /**
     * Executes on-demand near duplicate detection over the virtual proxies metadata.
     * @return a list of duplicate pairs with their similarity scores.
     */
    public List<Map<String, Object>> detectDuplicates() {
        Map<String, List<String>> proxies = holder.getAllVirtualProxies();
        List<String> uris = new ArrayList<>(proxies.keySet());
        List<Map<String, Object>> duplicates = new ArrayList<>();

        for (int i = 0; i < uris.size(); i++) {
            for (int j = i + 1; j < uris.size(); j++) {
                String uri1 = uris.get(i);
                String uri2 = uris.get(j);
                List<String> meta1 = proxies.get(uri1);
                List<String> meta2 = proxies.get(uri2);

                double sim = calculateJaccardSimilarity(meta1, meta2);
                if (sim >= similarityThreshold) {
                    Map<String, Object> dupInfo = new HashMap<>();
                    dupInfo.put("uri1", uri1);
                    dupInfo.put("uri2", uri2);
                    dupInfo.put("similarityScore", sim);
                    duplicates.add(dupInfo);
                }
            }
        }
        return duplicates;
    }

    private double calculateJaccardSimilarity(List<String> list1, List<String> list2) {
        if ((list1 == null || list1.isEmpty()) && (list2 == null || list2.isEmpty())) {
            return 1.0;
        }
        if (list1 == null || list1.isEmpty() || list2 == null || list2.isEmpty()) {
            return 0.0;
        }

        Set<String> set1 = new HashSet<>(list1);
        Set<String> set2 = new HashSet<>(list2);

        Set<String> intersection = new HashSet<>(set1);
        intersection.retainAll(set2);

        Set<String> union = new HashSet<>(set1);
        union.addAll(set2);

        return (double) intersection.size() / union.size();
    }
}
