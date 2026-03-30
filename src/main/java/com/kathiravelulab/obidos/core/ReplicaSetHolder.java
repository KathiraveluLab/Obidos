package com.kathiravelulab.obidos.core;

import com.kathiravelulab.obidos.model.ReplicaSet;
import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Core module for managing ReplicaSets using Infinispan In-Memory Data Grid.
 * As described in the Óbidos architecture.
 */
public class ReplicaSetHolder {

    private final EmbeddedCacheManager cacheManager;
    private final Cache<String, List<String>> userMap; // UserID -> List of ReplicaSetIDs
    private final Cache<String, ReplicaSet> replicaSetMap; // ReplicaSetID -> ReplicaSet Object
    private final Cache<String, List<String>> virtualProxyMap; // Source URI -> Metadata List
    
    // Maps for hierarchical granularity (DICOM example: Collections -> Patients -> Studies -> Series)
    private final Map<Integer, Cache<String, List<String>>> granularityMaps;

    public ReplicaSetHolder() {
        this.cacheManager = new DefaultCacheManager();
        this.userMap = cacheManager.getCache("userMap");
        this.replicaSetMap = cacheManager.getCache("replicaSetMap");
        this.virtualProxyMap = cacheManager.getCache("virtualProxyMap");
        this.granularityMaps = new ConcurrentHashMap<>();
        
        // Initialize 4 granularities for DICOM research parity
        for (int i = 0; i < 4; i++) {
            granularityMaps.put(i, cacheManager.getCache("granularity_" + i));
        }
    }

    public void addReplicaSet(String userID, ReplicaSet replicaSet) {
        replicaSetMap.put(replicaSet.getReplicaSetID(), replicaSet);
        
        List<String> userReplicaSets = userMap.getOrDefault(userID, new ArrayList<>());
        userReplicaSets.add(replicaSet.getReplicaSetID());
        userMap.put(userID, userReplicaSets);
    }

    public ReplicaSet getReplicaSet(String replicaSetID) {
        return replicaSetMap.get(replicaSetID);
    }

    public List<String> getUserReplicaSets(String userID) {
        return userMap.getOrDefault(userID, new ArrayList<>());
    }

    public void updateReplicaSet(ReplicaSet replicaSet) {
        replicaSetMap.put(replicaSet.getReplicaSetID(), replicaSet);
    }

    public void addVirtualProxy(String sourceURI, List<String> metadata) {
        virtualProxyMap.put(sourceURI, metadata);
    }

    public List<String> getVirtualProxy(String sourceURI) {
        return virtualProxyMap.getOrDefault(sourceURI, new ArrayList<>());
    }

    public void stop() {
        cacheManager.stop();
    }

    public Cache<String, List<String>> getGranularityMap(int level) {
        return granularityMaps.get(level);
    }
}
