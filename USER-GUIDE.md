# Óbidos User Guide

Welcome to the Óbidos User Guide. This document provides instructions on how to interact with the Óbidos Northbound API and utilize its Hybrid ETL and data sharing features.

## Query Rewriting

Óbidos allows users to group multiple distributed data sources (like TCIA, S3, or HDFS) into a single virtual entity called a **ReplicaSet**. Instead of writing complex queries targeting specific storage backend paths, users can issue simple, unified SQL queries against the abstract `ReplicaSetID`. The Óbidos Southbound API dynamically rewrites these queries into execution-ready SQL tailored for **Apache Drill**.

### 1. Creating a ReplicaSet
First, create a ReplicaSet grouping the diverse data sources you want to query.

**Endpoint:** `POST /replicasets?userID={userID}`

**Payload:**
```json
{
  "replicaSetID": "rs_query",
  "dataSources": [
    "tcia://collection1",
    "s3://bucket1"
  ]
}
```

### 2. Issuing a Query
You can now issue a query against your created ReplicaSet by sending a POST request to the `/query/{replicaSetID}` endpoint. 

Use the `ReplicaSetID` as the target table name in your SQL `FROM` clause.

**Endpoint:** `POST /query/rs_query`

**Payload (Raw Body):**
```sql
SELECT * FROM rs_query WHERE Modality='CT'
```

### 3. How the Rewriter Works (Under the Hood)
When the request is received, the `QueryTransformationLayer` performs the following steps:

1. **Resolution:** It looks up `rs_query` and retrieves the underlying data source pointers (`tcia://collection1` and `s3://bucket1`).
2. **Translation:** It translates the URI schemes into Drill-compatible workspace tables (e.g., `dfs.tcia.collection1` and `dfs.s3.bucket1`).
3. **Rewriting:** It seamlessly substitutes the abstract `rs_query` reference in your SQL string with a combined `UNION ALL` subquery structure representing the physical data sources.

**Resulting Apache Drill Query:**
```sql
SELECT * FROM (
    SELECT * FROM dfs.tcia.`collection1` 
    UNION ALL 
    SELECT * FROM dfs.s3.`bucket1`
) AS rs_query WHERE Modality='CT'
```

This rewritten query is then executed natively on Apache Drill, allowing for federated SQL querying across your unstructured and semi-structured datasets without the need to manually unify the ETL process!

---

## Metadata Search

Óbidos features a disk-backed persistent repository and a sophisticated indexing engine powered by **Apache Lucene**. Metadata extracted from virtual proxies is automatically indexed for high-performance full-text search.

**Endpoint:** `GET /search?q={query}`

**Example:**
```bash
curl "http://localhost:8080/search?q=Modality:CT"
```
*Returns a list of source URIs matching the criteria.*

---

## Hybrid ETL & Lazy Loading

To optimize storage and network usage, Óbidos utilizes a **Lazy ETL** approach. Data sources are initially registered as lightweight virtual proxies (metadata only). The heavy binary data (e.g., DICOM files) is only pulled into the local repository on-demand.

**Endpoint:** `POST /sources/load?uri={sourceURI}`

**Example:**
```bash
curl -X POST "http://localhost:8080/sources/load?uri=s3://bucket1"
```
*Triggers the physical download and updates the proxy status to `LOADED`.*

---

## Cross-Organization P2P Sharing

Óbidos supports federated data sharing between different deployments. Researchers can "pull" abstract ReplicaSets from other organizations to reuse their curated dataset pointers without needing to repeat the ETL process.

### Pulling from a Peer
**Endpoint:** `POST /federation/pull/:rsId?peer={peerURL}`

**Example:**
```bash
curl -X POST "http://localhost:8080/federation/pull/rs_cancer_study?peer=http://peer-obidos:8080"
```

### Sharing with a Peer
**Endpoint:** `POST /federation/share/:rsId?peer={peerURL}`

**Example:**
```bash
curl -X POST "http://localhost:8080/federation/share/rs_local_01?peer=http://remote-obidos:8080"
```
