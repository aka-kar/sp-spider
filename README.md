# siliconpin_spider

A Go-based web crawler with per-domain SQLite storage, robots.txt compliance,
randomised polite delays, and Server-Sent Events (SSE) for real-time progress.

---

## Requirements

| Tool | Notes |
|------|-------|
| Go 1.21+ | |
| GCC | Required by `go-sqlite3` (CGO) |

```bash
# Ubuntu / Debian
apt install gcc

# macOS (Xcode CLI tools)
xcode-select --install
```

---

## Run

```bash
go mod tidy
go run main.go
# Server → http://localhost:8080
```

---

## On startup

- Creates **`siliconpin_spider.sqlite`** (domains registry)
- Serves `./static/` at `/`
- **Resumes** any crawls that were previously registered

---

## Domain Statuses

The `siliconpin_spider.sqlite` registry tracks each domain with one of these statuses:

| Status | Description |
|--------|-------------|
| `pending` | Domain is queued and waiting to start crawling |
| `running` | Actively crawling the domain (fetching URLs, following links) |
| `paused` | Crawling is temporarily paused (can be resumed) |
| `done` | Crawling completed (no more URLs to process) |

**Status Transitions:**
- `pending` → `running` (when crawler starts)
- `running` → `paused` (manual pause or shutdown)
- `paused` → `running` (manual resume)
- `running` → `done` (crawl completes naturally)

---

## API

### `POST /api/add_domain`

Register a domain and immediately start crawling it.

```bash
curl -X POST http://localhost:8080/api/add_domain \
  -H "Content-Type: application/json" \
  -d '{"domain":"siliconpin.com","Crawl-delay":"20","internal_domain":"1","external_domain":"1"}'
```

**Example: Crawl only internal links**
```bash
curl -X POST http://localhost:8080/api/add_domain \
  -H "Content-Type: application/json" \
  -d '{"domain":"siliconpin.com","internal_domain":"1"}'
```

**Example: Discover external domains only**
```bash
curl -X POST http://localhost:8080/api/add_domain \
  -H "Content-Type: application/json" \
  -d '{"domain":"siliconpin.com","external_domain":"1"}'
```

**Body fields**

| Field | Required | Default | Notes |
|-------|----------|---------|-------|
| `domain` | ✅ | — | bare domain, scheme/www stripped automatically |
| `Crawl-delay` | ❌ | `60` | seconds; actual delay is random in `[N, N*2]` |
| `internal_domain` | ❌ | `0` | `1` to crawl internal links, `0` to skip |
| `external_domain` | ❌ | `0` | `1` to discover external domains, `0` to skip |

**Response `201`**

```json
{
  "message":  "domain added, crawler started",
  "domain":   "siliconpin.com",
  "interval": 20,
  "db_file":  "siliconpin.com.sqlite",
  "sse":      "/api/sse/siliconpin.com"
}
```

Creates **`siliconpin.com.sqlite`** with table:

```
urls(id, url UNIQUE, created_at, updated_at)
```

---

### `GET /api/sse/{domain}`

Stream crawl events for any registered domain as **Server-Sent Events**.

```bash
curl -N http://localhost:8080/api/sse/siliconpin.com
curl -N http://localhost:8080/api/sse/cicdhosting.com
```

Each `data:` line is a JSON object:

```
data: {"event":"connected",  "data":{"domain":"siliconpin.com"}}
data: {"event":"status",     "data":{"msg":"fetching robots.txt"}}
data: {"event":"robots",     "data":{"disallowed":["/admin/"],"robots_delay":10,"effective_delay":20}}
data: {"event":"waiting",    "data":{"url":"https://siliconpin.com/about","delay_s":27,"queue":4}}
data: {"event":"fetching",   "data":{"url":"https://siliconpin.com/about"}}
data: {"event":"saved",      "data":{"url":"…","status":200,"content_type":"text/html"}}
data: {"event":"links_found","data":{"url":"…","found":12,"new":8,"queue_len":12}}
data: {"event":"skipped",    "data":{"url":"…","reason":"robots.txt"}}
data: {"event":"error",      "data":{"url":"…","msg":"…"}}
data: {"event":"done",       "data":{"domain":"siliconpin.com","msg":"crawl complete"}}
: keepalive
```

Multiple browser tabs / curl processes can listen to the **same** domain stream simultaneously.

---

## Crawl behaviour

1. Fetches `robots.txt`; respects `Disallow` paths and `Crawl-delay`
   - If `robots.txt` specifies a higher delay than you set, the higher value wins
2. BFS queue – same-host HTML links only
3. Random delay between requests: **`interval` → `interval × 2`** seconds
4. Skips already-visited URLs (checked against the domain's SQLite)
5. On restart, existing domains resume from where they left off (unvisited URLs are re-queued from the start URL; already saved URLs are skipped)