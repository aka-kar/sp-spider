package main

import (
	"context"
	"database/sql"
	"sync"
)

// ─────────────────────────────────────────────────────────────────
//  File paths
// ─────────────────────────────────────────────────────────────────

const appDataDir = "./data/app"
const mainDBFile = appDataDir + "/siliconpin_spider.sqlite"
const skipDBFile = appDataDir + "/skip_domain_list.sqlite"

// ─────────────────────────────────────────────────────────────────
//  Database handles
// ─────────────────────────────────────────────────────────────────

var mainDB *sql.DB  // siliconpin_spider.sqlite – domain registry
var skipDB *sql.DB  // skip_domain_list.sqlite  – skip list
var mariaDB *sql.DB // MariaDB                  – crawled urls / queue / ext_links

// MariaDB table names (from env)
var mariaTable string
var mariaTableDomains string

// ─────────────────────────────────────────────────────────────────
//  SSE brokers – one per domain
// ─────────────────────────────────────────────────────────────────

var (
	brokersMu sync.RWMutex
	brokers   = map[string]*Broker{}
)

// ─────────────────────────────────────────────────────────────────
//  Crawler coordination
// ─────────────────────────────────────────────────────────────────

// crawlersMu guards the crawlers map and semaphore acquisition atomically.
var (
	crawlersMu sync.Mutex
	crawlers   = map[string]bool{}
)

// crawlerSem is a buffered channel used as a semaphore to cap concurrent crawlers.
// Initialised in main() from MAX_CRAWLERS env var (default 10).
var crawlerSem chan struct{}

// shutdown coordination
var (
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
	crawlerWg      sync.WaitGroup
)

// ─────────────────────────────────────────────────────────────────
//  Pause / resume channels – one per domain
// ─────────────────────────────────────────────────────────────────

var (
	pauseChsMu sync.RWMutex
	pauseChs   = map[string]chan struct{}{} // pause signal
	resumeChs  = map[string]chan struct{}{} // resume signal
)

// ─────────────────────────────────────────────────────────────────
//  Domain status constants
// ─────────────────────────────────────────────────────────────────

const (
	statusRunning = "running"
	statusPaused  = "paused"
	statusDone    = "done"
	statusPending = "pending"
)
