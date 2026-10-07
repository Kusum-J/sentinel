# Sentinel

A lightweight URL monitoring service built with Go. It periodically checks registered URLs, tracks their status and response time, and stores the results for later viewing.

## What it does

- Add URLs with a custom monitoring interval
- Check URLs in the background
- Track HTTP status and response time
- Store check history and status changes in MongoDB
- Use Redis for job queuing, caching, and preventing duplicate in-flight checks
- Run checks concurrently using a Go worker pool
- Simple web dashboard for viewing monitored URLs

## Stack

- **Go** — API, scheduler and workers
- **MongoDB** — monitor configuration and check history
- **Redis** — queue, cache and in-flight locks
- **HTML/CSS/JavaScript** — dashboard
- **Docker / Docker Compose** — local setup

## Architecture

```text
Browser
   ↓
Go API
   ↓
MongoDB

Scheduler
   ↓
Redis Queue
   ↓
Worker Pool
   ↓
HTTP Request
   ↓
Monitored URL
   ↓
MongoDB + Redis
```

The scheduler finds monitors that are due for a check and puts jobs into Redis. Workers consume the jobs concurrently, make the HTTP requests, and save the results.

## Running

```bash
docker compose up --build
```

The project also includes in-memory implementations for local development without MongoDB and Redis.

## Notes

This is a backend-focused project built to explore Go concurrency, background jobs, Redis queueing/caching, and MongoDB persistence. It is not intended to be a production-ready monitoring platform.