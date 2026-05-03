package com.kathiravelulab.obidos.services;

import com.google.gson.Gson;
import com.kathiravelulab.obidos.core.ReplicaSetHolder;
import com.kathiravelulab.obidos.model.ReplicaSet;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Scanner;

/**
 * Federation Service for Cross-Organization P2P Sharing of ReplicaSets.
 * As described in DAPD 18.
 */
public class FederationService {
    private final ReplicaSetHolder holder;
    private final Gson gson = new Gson();

    public FederationService(ReplicaSetHolder holder) {
        this.holder = holder;
    }

    /**
     * Pulls a ReplicaSet from a remote peer.
     * @param replicaSetID ID of the replica set to pull.
     * @param remotePeerUrl Base URL of the remote Óbidos instance.
     * @return The imported ReplicaSet.
     */
    public ReplicaSet pullReplicaSet(String replicaSetID, String remotePeerUrl) throws Exception {
        System.out.println("Federation: Pulling ReplicaSet " + replicaSetID + " from " + remotePeerUrl);
        URL url = new URL(remotePeerUrl + "/replicasets/" + replicaSetID);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        if (conn.getResponseCode() == 200) {
            try (Scanner s = new Scanner(conn.getInputStream()).useDelimiter("\\A")) {
                String json = s.hasNext() ? s.next() : "";
                ReplicaSet rs = gson.fromJson(json, ReplicaSet.class);
                if (rs != null) {
                    holder.addReplicaSet("federated_user", rs);
                    System.out.println("Federation: Successfully imported " + replicaSetID + " with " + rs.getDataSources().size() + " sources.");
                    return rs;
                }
            }
        }
        throw new Exception("Failed to pull ReplicaSet: " + conn.getResponseCode());
    }

    /**
     * Shares (pushes) a ReplicaSet to a remote peer.
     * @param replicaSetID ID of the local replica set to share.
     * @param remotePeerUrl Base URL of the remote Óbidos instance.
     * @return True if sharing was successful.
     */
    public boolean shareReplicaSet(String replicaSetID, String remotePeerUrl) throws Exception {
        System.out.println("Federation: Sharing ReplicaSet " + replicaSetID + " with " + remotePeerUrl);
        ReplicaSet rs = holder.getReplicaSet(replicaSetID);
        if (rs == null) {
            System.err.println("Federation Error: Local ReplicaSet " + replicaSetID + " not found.");
            return false;
        }

        URL url = new URL(remotePeerUrl + "/replicasets?userID=shared_from_peer");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/json");

        try (OutputStream os = conn.getOutputStream()) {
            os.write(gson.toJson(rs).getBytes());
        }

        int responseCode = conn.getResponseCode();
        System.out.println("Federation: Share status for " + replicaSetID + ": " + responseCode);
        return responseCode == 201;
    }
}
