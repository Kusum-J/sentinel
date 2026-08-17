# Sentinel — Real-Time Uptime & Health Monitoring Service

Sentinel is a lightweight, concurrent backend service written in Go that monitors a list of URLs at configurable intervals, tracks their up/down status, measures response times, and records uptime history. It exposes a REST API to manage monitors and fetch real-time logs.

---

## Architecture Diagram

```
                     +---------------------------------------+
                     |         Client (Curl / Postman)       |
                     +-------------------+-----------+-------+
                                         |           ^
                                    REST |           | JSON
                                     API |           | Response
                                         v           |
                     +-------------------+-----------+-------+
                     |                 API Server            | <----------+
                     +-------------------+-----------+-------+            |
                                         |           |                    |
                         Cache-Aside Read|           | MongoDB Write      |
                                         v           v                    | Check Cache
                                     +---+---+   +---+---+                | & Fallback
                                     | Redis |   | Mongo |                |
                                     | Cache |   |  DB   |                |
                                     +-------+   +---+---+                |
                                                     ^                    |
                                                     | MongoDB            |
                                                     | Write History      |
  +----------------------+                           | & State Changes    |
  |      Scheduler       |                           |                    |
  +----------+-----------+                           |                    |
             |                                       |                    |
             | 1. Check Interval                     |                    |
             | 2. Try In-Flight Lock                 |                    |
             | 3. LPUSH Job                          |                    |
             v                                       |                    |
       +-----+-----+                                 |                    |
       |   Redis   | <===============================+                    |
       |   Queue   |                                                      |
       +-----+-----+                                                      |
             |                                                            |
             | BRPOP Job                                                  |
             v                                                            |
   +---------+---------+                                                  |
   |   Worker Pool     | -------------------------------------------------+
   | (Goroutines 1..N) | ====> HTTP GET ====> [ Target URL ]
   +-------------------+
```

---

## How It Works: Preventing Duplicate & Overlapping Jobs

To prevent a slow or hanging URL check from causing multiple queued checks for the same monitor (which would hammer the destination server and clog our worker pool), Sentinel uses a dual-protection mechanism:

1. **Scheduler Interval Check**:
   The scheduler ticks every second. It compares the current time against the `last_queued_at` timestamp stored in MongoDB for each monitor. A job is only eligible if `now - last_queued_at >= interval_seconds`.
2. **Redis In-Flight Lock (`SETNX`)**:
   Before a job is pushed to the Redis queue, the scheduler attempts to acquire a lock in Redis using a key format: `inflight:{monitor_id}` via `SETNX`.
   * **If lock is acquired (value is set)**: The scheduler pushes the monitor ID to the Redis queue (`LPUSH sentinel:queue`) and updates the `last_queued_at` timestamp in MongoDB.
   * **If lock acquisition fails**: It means a worker is currently checking that URL, or the check job is already queued and waiting to be picked up. The scheduler **skips** queueing the monitor for this tick.
   * **Release and Timeout**: When the worker completes the HTTP check, it releases the lock (`DEL inflight:{monitor_id}`). The lock has a TTL of **30 seconds** (longer than the 5s check timeout) to ensure that if a worker crashes mid-check, the lock will automatically expire, preventing the monitor from being starved forever.

---

## Design Decisions (Interview Talking Points)

* **Why Go for Concurrency?**
  Go's goroutines are extremely lightweight (starting at ~2KB stack size compared to ~1MB for traditional OS threads). By implementing a **Worker Pool pattern** (using a fixed number of worker goroutines blocking on a Redis queue via `BRPOP`), we achieve high concurrency with minimal CPU and memory overhead. This is far more efficient than spawning OS threads or running asynchronous event loops in single-threaded environments.
* **Why Redis for Queue + Caching?**
  * **Queue**: Redis lists provide a fast, memory-backed queue. Using `LPUSH` to enqueue and `BRPOP` (blocking pop) in workers allows workers to sleep efficiently without CPU polling, immediately waking up when a job arrives.
  * **Cache**: We cache the latest check result for each monitor under `status:{monitor_id}` with a short TTL (30 seconds). The API reads from this cache first (cache-aside pattern), drastically reducing the read load on MongoDB for frequently loaded dashboards.
* **Why MongoDB for History?**
  Monitors, checks, and status change events map naturally to document structures. Uptime checks are write-heavy, time-series data. MongoDB offers high-performance writes, document structure flexibility (allowing us to easily log network errors when they occur), and excellent indexing on compound keys like `{monitor_id: 1, timestamp: -1}` for fast history retrieval.

---

## Setup & Running Locally (Under 2 Minutes)

You can run everything containerized via Docker Compose.

### Step 1: Clone and Start Containers
From the root directory (`sentinel/`), run:
```bash
docker compose up --build
```
This builds the API server and worker binaries inside Docker and starts all four containers:
* `sentinel-mongodb` (Port 27017)
* `sentinel-redis` (Port 6379)
* `sentinel-api` (Port 8080)
* `sentinel-worker`

### Step 2: Seed Test Monitors
Once the containers are up, you can seed them with test URLs (a working URL and a broken URL) using one of our scripts:
* **Windows (PowerShell)**:
  ```powershell
  ./seed.ps1
  ```
* **macOS / Linux / Git Bash**:
  ```bash
  chmod +x seed.sh
  ./seed.sh
  ```

---

## Example REST API Usage

### 1. Add a New Monitor
* **Endpoint**: `POST /urls`
* **Request**:
  ```bash
  curl -X POST http://localhost:8080/urls \
    -H "Content-Type: application/json" \
    -d '{"url": "https://httpbin.org/status/200", "interval_seconds": 10}'
  ```
* **Response (201 Created)**:
  ```json
  {
    "id": "64b18dfa838be70a318cb9c0",
    "url": "https://httpbin.org/status/200",
    "interval_seconds": 10,
    "created_at": "2026-07-16T22:20:00Z"
  }
  ```

### 2. List All Monitored URLs (with Current Status)
* **Endpoint**: `GET /urls`
* **Request**:
  ```bash
  curl http://localhost:8080/urls
  ```
* **Response (200 OK)**:
  ```json
  [
    {
      "id": "64b18dfa838be70a318cb9c0",
      "url": "https://httpbin.org/status/200",
      "interval_seconds": 10,
      "created_at": "2026-07-16T22:20:00Z",
      "latest_check": {
        "monitor_id": "64b18dfa838be70a318cb9c0",
        "timestamp": "2026-07-16T22:20:10Z",
        "status_code": 200,
        "response_time_ms": 142,
        "is_up": true
      }
    }
  ]
  ```

### 3. Get Details for a Single URL
* **Endpoint**: `GET /urls/:id`
* **Request**:
  ```bash
  curl http://localhost:8080/urls/64b18dfa838be70a318cb9c0
  ```

### 4. Get Status Change History Log
* **Endpoint**: `GET /urls/:id/history`
* **Request**:
  ```bash
  curl http://localhost:8080/urls/64b18dfa838be70a318cb9c0/history
  ```
* **Response (200 OK)**:
  ```json
  [
    {
      "id": "64b18e00838be70a318cb9c1",
      "monitor_id": "64b18dfa838be70a318cb9c0",
      "timestamp": "2026-07-16T22:20:10Z",
      "changed_to": "up"
    }
  ]
  ```

### 5. Stop Monitoring & Delete History
* **Endpoint**: `DELETE /urls/:id`
* **Request**:
  ```bash
  curl -X DELETE http://localhost:8080/urls/64b18dfa838be70a318cb9c0
  ```
* **Response**: `204 No Content`

---

## AWS EC2 Production Deployment Plan

In a production scenario, you would deploy Sentinel using a highly available and secure architecture:

1. **Deployment Architecture**:
   * Deploy the Go app (API + Worker) inside Docker containers on AWS EC2.
   * Run the EC2 instances inside an Auto Scaling Group behind an **Application Load Balancer (ALB)** to distribute API traffic.
   * Offload the data layer from EC2:
     * Use **Amazon DocumentDB** (MongoDB compatible) for persistent database history.
     * Use **Amazon ElastiCache for Redis** for the queuing and cache layer.
2. **Security & Networking**:
   * Place MongoDB and Redis in private subnets, allowing connections only from the security groups of the EC2 instances.
   * Run the API server EC2 instances in private subnets, with the ALB in public subnets routing traffic to them.
3. **CI/CD Pipeline**:
   * Automate builds using GitHub Actions to compile binaries, build Docker images, push them to **Amazon ECR**, and deploy to ECS or EC2 via Docker Compose/Ansible.

---

## Limitations / Future Scope

If I had more time, I would address the following constraints:
1. **Multi-Region Monitoring**: Currently, checks are run from the worker's hosting region. For a production health service, we should execute checks from multiple AWS regions (e.g. us-east-1, eu-west-1, ap-southeast-1) to detect geo-specific routing/network failures.
2. **Distributed Scheduler Coordination**: With multiple worker/scheduler instances running, we should use a distributed scheduler (like Redis Redlock or a database-backed leader election) to ensure only one scheduler process executes the ticking check, avoiding redundant queries.
3. **Metrics Rollups (Aggregation)**: The `checks` collection grows lineary with every check. For long-term storage, we would implement a background aggregation worker to roll up raw check data into hourly/daily uptime percentages, then prune raw data older than 30 days.
4. **Alerting & Notifications**: Integrate channels like Slack webhooks, PagerDuty, or SMS/Email alerts (AWS SNS/SES) triggered on status change events.
