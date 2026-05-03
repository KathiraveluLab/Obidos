# Óbidos
On-Demand Big Data Integration: A Hybrid ETL Approach for Reproducible Scientific Research

## System Architecture

Óbidos is a research-grade big data integration platform designed for heterogeneous medical imaging data. It consists of four primary layers:

1. **Northbound API**: RESTful interface (SparkJava) for managing ReplicaSets and executing queries.
2. **Proxy Layer**: In-memory data repository (Infinispan) that stores virtual proxies of datasets.
3. **Southbound API**: Query transformation and data retrieval layer using Apache Drill JDBC.
4. **Westbound API**: Pluggable source connectors (e.g., TCIA, S3, HDFS) for selective metadata and data loading.

## Prerequisites

- Java Development Kit (JDK) 1.8 or higher
- Apache Maven 3.6+
- Docker and Docker Compose (optional, for Apache Drill integration)

## Quick Start

### 1. Build the Project
```bash
mvn clean install
```

### 2. Run Apache Drill (Optional)
To use a live Apache Drill instance for query execution:
```bash
docker-compose up -d
```

### 3. Run the Northbound API
```bash
mvn exec:java -Dexec.mainClass="com.kathiravelulab.obidos.api.NorthboundController"
```
The API will be available at `http://localhost:8080`.

## API Endpoints

- **GET /**: Welcome message and landing page.
- **GET /health**: System health check.
- **POST /replicasets**: Create a new ReplicaSet.
- **GET /replicasets/:id**: Retrieve a specific ReplicaSet.
- **GET /tenants/:userID/replicasets**: List ReplicaSets for a specific user.
- **POST /query/:rsId**: Execute a SQL query rewritten for unified access across multiple sources.
- **GET /duplicates**: Detect near-duplicate metadata using Jaccard Similarity.
- **GET /search?q={query}**: Full-text search across persistent metadata index (Lucene).
- **POST /sources/load?uri={uri}**: Trigger lazy loading of physical data from a source (Hybrid ETL).
- **POST /federation/pull/:rsId?peer={url}**: Pull a ReplicaSet from a remote P2P peer.
- **POST /federation/share/:rsId?peer={url}**: Share a local ReplicaSet with a remote peer.

## Citing Óbidos

If you use Óbidos in your research, please cite the following papers:

* Kathiravelu, P., Sharma, A., Galhardas, H., Van Roy, P. and Veiga, L., 2019. **On-demand big data integration: A hybrid ETL approach for reproducible scientific research.** Distributed and Parallel Databases, 37(2), pp.273-295.

* Kathiravelu, P., Chen, Y., Sharma, A., Galhardas, H., Van Roy, P. and Veiga, L., 2017, August. **On-demand service-based big data integration: optimized for research collaboration.** In VLDB Workshop on Data Management and Analytics for Medicine and Healthcare (pp. 9-28). Cham: Springer International Publishing.
