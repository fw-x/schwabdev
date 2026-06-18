# Real-Time Data Pipeline Architecture

This repository contains a high-throughput data pipeline built on a modular **Producer/Consumer framework**. It utilizes **Redis Streams** as a low-latency event manager/message broker to ingest, transform, and persist streaming data into an optimized analytical data lake.

---

## 🏗️ Architecture Overview

The pipeline implements a decoupled **Source ➔ Transformer ➔ Sink** pattern to ensure horizontal scalability, modular maintenance, and reliable backpressure management.


* **Producer (Source):** Ingests raw, high-frequency streaming events and publishes them directly to a Redis Stream FIFO queue.
* **Event Manager (Broker):** **Redis** acts as the high-performance caching and streaming layer, holding messages sequentially until explicitly managed.
* **Consumer (Transformer/Sink):** A modular engine that consumes deltas from the stream, executes transformations, and commits records to columnar persistence.

```Plain Text
               ┌────────────────┐
               │  Data Source   │
               └───────┬────────┘
                       │ (Streaming)
                       ▼
               ┌────────────────┐
               │    Producer    │
               └───────┬────────┘
                       │
                       ▼ [ Redis Stream ]
                       │
               ┌────────────────┐
               │    Consumer    │
               └───────┬────────┘
                       │ (Compute / Transform)
                       ▼
               ┌────────────────┐
               │ DuckDB/Parquet │
               └────────────────┘
```
---

## 🛠️ Tech Stack

* **Language:** Python 3.11+
* **Stream Orchestration:** Redis (Streams)
* **Analytical Engine:** DuckDB
* **Storage Format:** Hive-Partitioned Parquet (ZSTD Compressed)
* **Containerization:** Docker (Multi-stage, rootless production builds)

---

## Configure Environment Variables:
Create a .env file in the root directory:

Code snippet
```.env
   REDIS_HOST=localhost
   REDIS_PORT=6379
   APP_NAME=your_app_module
   SCHWAB_KEY=your_schwab_api_key
   SCHWAB_SECRETE=your_schwab_api_secrete
   TOKEN_DB=location_of_token_db
```


## 📂 Data Lake Layout
Data is written to disk using a strict Hive-partitioning strategy optimized for single-root historical analysis and rapid partition pruning:

```Plaintext
data/storage/main/
└── service=SERVICE_NAME/
    ├── part_root=ROOT_A/
    │   ├── part_date=1970_01_01/
    │   └── part_date=1970_01_02/
    └── part_root=ROOT_B/
        ├── part_date=1970_01_01/
        └── part_date=1970_01_02/
```
