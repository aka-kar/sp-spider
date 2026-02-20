package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// ─────────────────────────────────────────────────────────────────
//  Global state
// ─────────────────────────────────────────────────────────────────

const mainDBFile = "siliconpin_spider.sqlite"

var mainDB *sql.DB

// per-domain SSE brokers
var (
	brokersMu sync.RWMutex
	brokers   = map[string]*Broker{}
)

// per-domain DB connections (kept open)
var (
	domainDBsMu sync.RWMutex
	domainDBs   = map[string]*sql.DB{}
)

// guard against duplicate crawlers
var (
	crawlersMu sync.Mutex
	crawlers   = map[string]bool{}
)

// ─────────────────────────────────────────────────────────────────
//  SSE Broker  – fan-out to multiple subscribers per domain
// ─────────────────────────────────────────────────────────────────

type Broker struct {
	mu      sync.Mutex
	clients map[chan string]struct{}
}

func newBroker() *Broker {
	return &Broker{clients: make(map[chan string]struct{})}
}

func (b *Broker) subscribe() chan string {
	ch := make(chan string, 64)
	b.mu.Lock()
	b.clients[ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

func (b *Broker) unsubscribe(ch chan string) {
	b.mu.Lock()
	delete(b.clients, ch)
	b.mu.Unlock()
}

func (b *Broker) publish(msg string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.clients {
		select {
		case ch <- msg:
		default: // slow client – drop message
		}
	}
}

func getBroker(domain string) *Broker {
	brokersMu.RLock()
	br, ok := brokers[domain]
	brokersMu.RUnlock()
	if ok {
		return br
	}
	brokersMu.Lock()
	defer brokersMu.Unlock()
	if br, ok = brokers[domain]; ok {
		return br
	}
	br = newBroker()
	brokers[domain] = br
	return br
}

// ─────────────────────────────────────────────────────────────────
//  SSE event helper
// ─────────────────────────────────────────────────────────────────

type sseEvent struct {
	Event string      `json:"event"`
	Data  interface{} `json:"data"`
}

func emit(br *Broker, event string, data interface{}) {
	payload, _ := json.Marshal(sseEvent{Event: event, Data: data})
	br.publish(string(payload))
}

// ─────────────────────────────────────────────────────────────────
//  Database helpers
// ─────────────────────────────────────────────────────────────────

func initMainDB() {
	var err error
	mainDB, err = sql.Open("sqlite3", mainDBFile+"?_journal=WAL&_busy_timeout=5000")
	if err != nil {
		log.Fatalf("open main DB: %v", err)
	}
	_, err = mainDB.Exec(`
		CREATE TABLE IF NOT EXISTS domains (
			id         INTEGER  PRIMARY KEY AUTOINCREMENT,
			domain     TEXT     NOT NULL UNIQUE,
			interval   INTEGER  NOT NULL DEFAULT 60,
			created_at DATETIME NOT NULL,
			updated_at DATETIME NOT NULL
		)`)
	if err != nil {
		log.Fatalf("create domains table: %v", err)
	}
	log.Printf("Main DB ready: %s", mainDBFile)
}

func openDomainDB(domain string) (*sql.DB, error) {
	domainDBsMu.RLock()
	db, ok := domainDBs[domain]
	domainDBsMu.RUnlock()
	if ok {
		return db, nil
	}

	db, err := sql.Open("sqlite3", domain+".sqlite?_journal=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS urls (
			id         INTEGER  PRIMARY KEY AUTOINCREMENT,
			url        TEXT     NOT NULL UNIQUE,
			created_at DATETIME NOT NULL,
			updated_at DATETIME NOT NULL
		)`)
	if err != nil {
		db.Close()
		return nil, err
	}

	domainDBsMu.Lock()
	domainDBs[domain] = db
	domainDBsMu.Unlock()
	return db, nil
}

func insertURL(db *sql.DB, rawURL string) (bool, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	res, err := db.Exec(
		`INSERT OR IGNORE INTO urls (url, created_at, updated_at) VALUES (?, ?, ?)`,
		rawURL, now, now,
	)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func isURLKnown(db *sql.DB, rawURL string) bool {
	var c int
	db.QueryRow(`SELECT COUNT(1) FROM urls WHERE url = ?`, rawURL).Scan(&c)
	return c > 0
}

// ─────────────────────────────────────────────────────────────────
//  robots.txt  (minimal, single-pass parser)
// ─────────────────────────────────────────────────────────────────

type robotsRules struct {
	disallowed []string
	crawlDelay int // 0 = not set
}

func fetchRobots(domain string) *robotsRules {
	rules := &robotsRules{}
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Get("https://" + domain + "/robots.txt")
	if err != nil || resp.StatusCode != 200 {
		return rules
	}
	defer resp.Body.Close()

	inSection := false
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		lower := strings.ToLower(line)

		if strings.HasPrefix(lower, "user-agent:") {
			agent := strings.TrimSpace(line[len("user-agent:"):])
			inSection = agent == "*" ||
				strings.EqualFold(agent, "siliconpin_spider")
			continue
		}
		if !inSection {
			continue
		}
		switch {
		case strings.HasPrefix(lower, "disallow:"):
			p := strings.TrimSpace(line[len("disallow:"):])
			if p != "" {
				rules.disallowed = append(rules.disallowed, p)
			}
		case strings.HasPrefix(lower, "crawl-delay:"):
			fmt.Sscanf(strings.TrimSpace(line[len("crawl-delay:"):]), "%d", &rules.crawlDelay)
		}
	}
	return rules
}

func (r *robotsRules) allowed(path string) bool {
	for _, d := range r.disallowed {
		if strings.HasPrefix(path, d) {
			return false
		}
	}
	return true
}

// ─────────────────────────────────────────────────────────────────
//  Link extractor  – same-host HTML links only
// ─────────────────────────────────────────────────────────────────

var hrefRe = regexp.MustCompile(`(?i)href=["']([^"'#][^"']*)["']`)

func extractLinks(base *url.URL, body string) []string {
	seen := map[string]bool{}
	var links []string
	for _, m := range hrefRe.FindAllStringSubmatch(body, -1) {
		href := strings.TrimSpace(m[1])
		parsed, err := url.Parse(href)
		if err != nil {
			continue
		}
		resolved := base.ResolveReference(parsed)
		resolved.Fragment = ""
		resolved.RawQuery = ""
		if resolved.Scheme != "http" && resolved.Scheme != "https" {
			continue
		}
		if !strings.EqualFold(resolved.Hostname(), base.Hostname()) {
			continue
		}
		s := resolved.String()
		if !seen[s] {
			seen[s] = true
			links = append(links, s)
		}
	}
	return links
}

// ─────────────────────────────────────────────────────────────────
//  Crawler goroutine
// ─────────────────────────────────────────────────────────────────

func crawlDomain(domain string, intervalSec int) {
	log.Printf("[%s] crawler started (base interval %ds)", domain, intervalSec)
	br := getBroker(domain)

	db, err := openDomainDB(domain)
	if err != nil {
		emit(br, "error", map[string]string{"msg": "DB error: " + err.Error()})
		return
	}

	// ── robots.txt ──────────────────────────────────────────────
	emit(br, "status", map[string]string{"msg": "fetching robots.txt"})
	robots := fetchRobots(domain)

	// robots.txt crawl-delay overrides our setting if higher
	if robots.crawlDelay > intervalSec {
		intervalSec = robots.crawlDelay
		now := time.Now().UTC().Format(time.RFC3339)
		mainDB.Exec(`UPDATE domains SET interval=?, updated_at=? WHERE domain=?`,
			intervalSec, now, domain)
	}
	emit(br, "robots", map[string]interface{}{
		"disallowed":      robots.disallowed,
		"robots_delay":    robots.crawlDelay,
		"effective_delay": intervalSec,
	})

	// ── BFS queue ───────────────────────────────────────────────
	startURL := "https://" + domain + "/"
	queue := []string{startURL}

	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return fmt.Errorf("too many redirects")
			}
			return nil
		},
	}

	for len(queue) > 0 {
		// Re-read interval in case it was updated via API
		var cur int
		if err := mainDB.QueryRow(`SELECT interval FROM domains WHERE domain=?`, domain).Scan(&cur); err == nil && cur > 0 {
			intervalSec = cur
		}

		target := queue[0]
		queue = queue[1:]

		if isURLKnown(db, target) {
			continue
		}

		// robots check
		parsed, err := url.Parse(target)
		if err != nil {
			continue
		}
		if !robots.allowed(parsed.Path) {
			emit(br, "skipped", map[string]string{"url": target, "reason": "robots.txt"})
			continue
		}

		// random delay: [interval, interval*2] seconds
		delaySec := intervalSec + rand.Intn(intervalSec+1)
		delay := time.Duration(delaySec) * time.Second
		emit(br, "waiting", map[string]interface{}{
			"url":     target,
			"delay_s": delaySec,
			"queue":   len(queue),
		})
		time.Sleep(delay)

		// fetch
		emit(br, "fetching", map[string]string{"url": target})
		resp, err := httpClient.Get(target)
		if err != nil {
			emit(br, "error", map[string]string{"url": target, "msg": err.Error()})
			log.Printf("[%s] fetch error %s: %v", domain, target, err)
			continue
		}

		ct := resp.Header.Get("Content-Type")
		isHTML := strings.Contains(ct, "text/html")

		var bodyStr string
		if isHTML {
			raw, _ := io.ReadAll(io.LimitReader(resp.Body, 5<<20)) // 5 MB cap
			bodyStr = string(raw)
		}
		resp.Body.Close()

		inserted, _ := insertURL(db, target)
		if inserted {
			emit(br, "saved", map[string]interface{}{
				"url":          target,
				"status":       resp.StatusCode,
				"content_type": ct,
			})
			log.Printf("[%s] saved: %s", domain, target)
		}

		// discover links
		if isHTML && resp.StatusCode == 200 {
			links := extractLinks(parsed, bodyStr)
			newCount := 0
			for _, link := range links {
				if !isURLKnown(db, link) {
					queue = append(queue, link)
					newCount++
				}
			}
			emit(br, "links_found", map[string]interface{}{
				"url":       target,
				"found":     len(links),
				"new":       newCount,
				"queue_len": len(queue),
			})
		}
	}

	emit(br, "done", map[string]string{"domain": domain, "msg": "crawl complete"})
	log.Printf("[%s] crawl complete", domain)

	crawlersMu.Lock()
	delete(crawlers, domain)
	crawlersMu.Unlock()
}

// ─────────────────────────────────────────────────────────────────
//  HTTP handlers
// ─────────────────────────────────────────────────────────────────

func sanitizeDomain(raw string) string {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "https://")
	raw = strings.TrimPrefix(raw, "http://")
	raw = strings.TrimPrefix(raw, "www.")
	raw = strings.TrimRight(raw, "/")
	return raw
}

var domainRe = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z]{2,})+$`)

func isValidDomain(d string) bool { return domainRe.MatchString(d) }

// POST /api/add_domain
func addDomainHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var body struct {
		Domain     string `json:"domain"`
		CrawlDelay string `json:"Crawl-delay"`
	}
	w.Header().Set("Content-Type", "application/json")

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid JSON"})
		return
	}
	if body.Domain == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "domain is required"})
		return
	}

	domain := sanitizeDomain(body.Domain)
	if !isValidDomain(domain) {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid domain"})
		return
	}

	interval := 60
	if body.CrawlDelay != "" {
		fmt.Sscanf(body.CrawlDelay, "%d", &interval)
		if interval <= 0 {
			interval = 60
		}
	}

	now := time.Now().UTC().Format(time.RFC3339)
	_, err := mainDB.Exec(
		`INSERT INTO domains (domain,interval,created_at,updated_at) VALUES (?,?,?,?)
		 ON CONFLICT(domain) DO UPDATE SET interval=excluded.interval, updated_at=excluded.updated_at`,
		domain, interval, now, now,
	)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "db error"})
		return
	}

	if _, err := openDomainDB(domain); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "domain DB init failed"})
		return
	}

	// start crawler if not already running
	crawlersMu.Lock()
	if !crawlers[domain] {
		crawlers[domain] = true
		go crawlDomain(domain, interval)
	}
	crawlersMu.Unlock()

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"message":  "domain added, crawler started",
		"domain":   domain,
		"interval": interval,
		"db_file":  domain + ".sqlite",
		"sse":      "/api/sse/" + domain,
	})
}

// GET /api/sse/{domain}
func sseHandler(w http.ResponseWriter, r *http.Request) {
	rawDomain := strings.TrimPrefix(r.URL.Path, "/api/sse/")
	domain := sanitizeDomain(rawDomain)
	if !isValidDomain(domain) {
		http.Error(w, "invalid domain", http.StatusBadRequest)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // nginx: disable proxy buffering
	w.Header().Set("Access-Control-Allow-Origin", "*")

	br := getBroker(domain)
	ch := br.subscribe()
	defer br.unsubscribe(ch)

	log.Printf("[SSE] client connected → %s", domain)

	// send immediate connected event
	fmt.Fprintf(w, "data: {\"event\":\"connected\",\"data\":{\"domain\":%q}}\n\n", domain)
	flusher.Flush()

	ticker := time.NewTicker(25 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			log.Printf("[SSE] client disconnected → %s", domain)
			return
		case msg := <-ch:
			fmt.Fprintf(w, "data: %s\n\n", msg)
			flusher.Flush()
		case <-ticker.C:
			fmt.Fprintf(w, ": keepalive\n\n")
			flusher.Flush()
		}
	}
}

// ─────────────────────────────────────────────────────────────────
//  main
// ─────────────────────────────────────────────────────────────────

func main() {
	rand.Seed(time.Now().UnixNano()) //nolint:staticcheck

	if err := os.MkdirAll("static", 0o755); err != nil {
		log.Fatalf("mkdir static: %v", err)
	}

	initMainDB()

	// Resume any domains already in the DB from a previous run
	rows, err := mainDB.Query(`SELECT domain, interval FROM domains`)
	if err == nil {
		for rows.Next() {
			var d string
			var iv int
			if rows.Scan(&d, &iv) == nil {
				crawlersMu.Lock()
				if !crawlers[d] {
					crawlers[d] = true
					go crawlDomain(d, iv)
				}
				crawlersMu.Unlock()
			}
		}
		rows.Close()
	}

	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.Dir("./static")))
	mux.HandleFunc("/api/add_domain", addDomainHandler)
	mux.HandleFunc("/api/sse/", sseHandler)

	srv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	// ── Graceful shutdown ────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Printf("siliconpin_spider listening on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	sig := <-quit
	log.Printf("received %s — shutting down gracefully…", sig)

	// 1. Stop accepting new HTTP requests; give in-flight ones 10s to finish
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("HTTP shutdown error: %v", err)
	}

	// 2. Notify all SSE clients
	brokersMu.RLock()
	for domain, br := range brokers {
		emit(br, "shutdown", map[string]string{"domain": domain, "msg": "server stopping"})
	}
	brokersMu.RUnlock()

	// Brief pause so SSE messages flush to clients
	time.Sleep(500 * time.Millisecond)

	// 3. Checkpoint WAL → merge pending writes into the .sqlite file
	//    After this the .sqlite is fully self-contained (no WAL needed).
	domainDBsMu.RLock()
	for domain, db := range domainDBs {
		if _, err := db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
			log.Printf("checkpoint %s: %v", domain, err)
		} else {
			log.Printf("checkpointed %s.sqlite", domain)
		}
		db.Close()
	}
	domainDBsMu.RUnlock()

	if _, err := mainDB.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		log.Printf("checkpoint main DB: %v", err)
	}
	mainDB.Close()

	log.Println("shutdown complete — all WAL data flushed, goodbye.")
}
