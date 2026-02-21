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
//  Constants & globals
// ─────────────────────────────────────────────────────────────────

const appDataDir = "./data/app"
const domainDataDir = "./data/domain"
const mainDBFile = appDataDir + "/siliconpin_spider.sqlite"
const skipDBFile = appDataDir + "/skip_domain_list.sqlite"

var mainDB *sql.DB
var skipDB *sql.DB

// SSE brokers  – one per domain
var (
	brokersMu sync.RWMutex
	brokers   = map[string]*Broker{}
)

// open domain DB handles
var (
	domainDBsMu sync.RWMutex
	domainDBs   = map[string]*sql.DB{}
)

// crawler goroutine guard
var (
	crawlersMu sync.Mutex
	crawlers   = map[string]bool{}
)

// pause/resume channels – one per domain
// sending to pauseCh pauses; sending to resumeCh resumes
var (
	pauseChsMu sync.RWMutex
	pauseChs   = map[string]chan struct{}{} // pause signal
	resumeChs  = map[string]chan struct{}{} // resume signal
)

// domain status values stored in main DB
const (
	statusRunning = "running"
	statusPaused  = "paused"
	statusDone    = "done"
	statusPending = "pending"
)

// ─────────────────────────────────────────────────────────────────
//  SSE Broker
// ─────────────────────────────────────────────────────────────────

type Broker struct {
	mu      sync.Mutex
	clients map[chan string]struct{}
}

func newBroker() *Broker { return &Broker{clients: make(map[chan string]struct{})} }

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
		default:
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

type ssePayload struct {
	Event string      `json:"event"`
	Data  interface{} `json:"data"`
}

func emit(br *Broker, event string, data interface{}) {
	b, _ := json.Marshal(ssePayload{Event: event, Data: data})
	br.publish(string(b))
}

// broadcast emits to ALL domain brokers (e.g. for a new_domain event)
func broadcast(event string, data interface{}) {
	brokersMu.RLock()
	defer brokersMu.RUnlock()
	b, _ := json.Marshal(ssePayload{Event: event, Data: data})
	msg := string(b)
	for _, br := range brokers {
		br.publish(msg)
	}
}

// ─────────────────────────────────────────────────────────────────
//  Main DB helpers
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
			status     TEXT     NOT NULL DEFAULT 'pending',
			parent     TEXT     NOT NULL DEFAULT '',
			created_at DATETIME NOT NULL,
			updated_at DATETIME NOT NULL
		)`)
	if err != nil {
		log.Fatalf("create domains table: %v", err)
	}
	log.Printf("Main DB ready: %s", mainDBFile)
}

func initSkipDB() {
	var err error
	skipDB, err = sql.Open("sqlite3", skipDBFile+"?_journal=WAL&_busy_timeout=5000")
	if err != nil {
		log.Fatalf("open skip DB: %v", err)
	}
	_, err = skipDB.Exec(`
		CREATE TABLE IF NOT EXISTS skip_domains (
			id         INTEGER  PRIMARY KEY AUTOINCREMENT,
			domain     TEXT     NOT NULL UNIQUE,
			reason     TEXT     NOT NULL DEFAULT '',
			created_at DATETIME NOT NULL
		)`)
	if err != nil {
		log.Fatalf("create skip_domains table: %v", err)
	}

	// Seed default skip list — INSERT OR IGNORE so re-runs are safe
	defaults := []struct{ domain, reason string }{
		{"google.com", "analytics / search engine"},
		{"facebook.com", "social media tracker"},
		{"linkedin.com", "social media tracker"},
		{"googletagmanager.com", "tag manager / analytics"},
	}
	now := time.Now().UTC().Format(time.RFC3339)
	for _, e := range defaults {
		skipDB.Exec(
			`INSERT OR IGNORE INTO skip_domains (domain, reason, created_at) VALUES (?, ?, ?)`,
			e.domain, e.reason, now)
	}
	log.Printf("Skip DB ready: %s", skipDBFile)
}

// isDomainSkipped returns true if the domain (or any parent domain suffix) is
// in the skip list.  e.g. "cdn.google.com" matches the "google.com" entry.
func isDomainSkipped(domain string) bool {
	// exact match
	var c int
	skipDB.QueryRow(`SELECT COUNT(1) FROM skip_domains WHERE domain = ?`, domain).Scan(&c)
	if c > 0 {
		return true
	}
	// suffix match: check if domain ends with "."+skipEntry
	rows, err := skipDB.Query(`SELECT domain FROM skip_domains`)
	if err != nil {
		return false
	}
	defer rows.Close()
	for rows.Next() {
		var entry string
		if rows.Scan(&entry) != nil {
			continue
		}
		if strings.HasSuffix(domain, "."+entry) {
			return true
		}
	}
	return false
}

func setDomainStatus(domain, status string) {
	now := time.Now().UTC().Format(time.RFC3339)
	mainDB.Exec(`UPDATE domains SET status=?, updated_at=? WHERE domain=?`, status, now, domain)
}

type DomainRow struct {
	ID        int    `json:"id"`
	Domain    string `json:"domain"`
	Interval  int    `json:"interval"`
	Status    string `json:"status"`
	Parent    string `json:"parent,omitempty"`
	URLCount  int    `json:"url_count"`
	QueueLen  int    `json:"queue_len"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

func listDomains() ([]DomainRow, error) {
	rows, err := mainDB.Query(
		`SELECT id, domain, interval, status, parent, created_at, updated_at
		 FROM domains ORDER BY id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []DomainRow
	for rows.Next() {
		var d DomainRow
		if err := rows.Scan(&d.ID, &d.Domain, &d.Interval, &d.Status,
			&d.Parent, &d.CreatedAt, &d.UpdatedAt); err != nil {
			continue
		}
		// get live counts from domain DB
		if db, err2 := openDomainDB(d.Domain); err2 == nil {
			db.QueryRow(`SELECT COUNT(1) FROM urls`).Scan(&d.URLCount)
			db.QueryRow(`SELECT COUNT(1) FROM queue`).Scan(&d.QueueLen)
		}
		out = append(out, d)
	}
	return out, nil
}

// registerDomain upserts a domain in the main DB.
// parentDomain is "" for user-added domains, otherwise the domain that found it.
func registerDomain(domain string, interval int, parentDomain string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := mainDB.Exec(`
		INSERT INTO domains (domain, interval, status, parent, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(domain) DO UPDATE SET
			interval=excluded.interval,
			updated_at=excluded.updated_at`,
		domain, interval, statusPending, parentDomain, now, now)
	return err
}

// ─────────────────────────────────────────────────────────────────
//  Domain DB helpers
// ─────────────────────────────────────────────────────────────────

func openDomainDB(domain string) (*sql.DB, error) {
	domainDBsMu.RLock()
	db, ok := domainDBs[domain]
	domainDBsMu.RUnlock()
	if ok {
		return db, nil
	}

	db, err := sql.Open("sqlite3", domainDataDir+"/"+domain+".sqlite?_journal=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, err
	}
	if _, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS urls (
			id         INTEGER  PRIMARY KEY AUTOINCREMENT,
			url        TEXT     NOT NULL UNIQUE,
			created_at DATETIME NOT NULL,
			updated_at DATETIME NOT NULL
		)`); err != nil {
		db.Close()
		return nil, err
	}
	if _, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS queue (
			id       INTEGER  PRIMARY KEY AUTOINCREMENT,
			url      TEXT     NOT NULL UNIQUE,
			added_at DATETIME NOT NULL
		)`); err != nil {
		db.Close()
		return nil, err
	}
	// cross-domain links discovered during crawl
	if _, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS ext_links (
			id         INTEGER  PRIMARY KEY AUTOINCREMENT,
			ext_domain TEXT     NOT NULL UNIQUE,
			found_at   DATETIME NOT NULL
		)`); err != nil {
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
		rawURL, now, now)
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

func enqueueURL(db *sql.DB, rawURL string) {
	now := time.Now().UTC().Format(time.RFC3339)
	db.Exec(`INSERT OR IGNORE INTO queue (url, added_at) VALUES (?, ?)`, rawURL, now)
}

func dequeueURL(db *sql.DB) (string, bool) {
	tx, err := db.Begin()
	if err != nil {
		return "", false
	}
	defer tx.Rollback() //nolint:errcheck
	var id int64
	var rawURL string
	if err = tx.QueryRow(`SELECT id, url FROM queue ORDER BY id ASC LIMIT 1`).
		Scan(&id, &rawURL); err != nil {
		return "", false
	}
	if _, err = tx.Exec(`DELETE FROM queue WHERE id = ?`, id); err != nil {
		return "", false
	}
	if err = tx.Commit(); err != nil {
		return "", false
	}
	return rawURL, true
}

func queueLen(db *sql.DB) int {
	var n int
	db.QueryRow(`SELECT COUNT(1) FROM queue`).Scan(&n)
	return n
}

func seedQueue(db *sql.DB, startURL string) {
	var qc, uc int
	db.QueryRow(`SELECT COUNT(1) FROM queue`).Scan(&qc)
	db.QueryRow(`SELECT COUNT(1) FROM urls`).Scan(&uc)
	if qc == 0 && uc == 0 {
		enqueueURL(db, startURL)
	}
}

// recordExtLink saves a discovered external domain and auto-registers it.
func recordExtLink(srcDomain, extDomain string, parentInterval int) {
	// Skip domains on the block list (and their subdomains)
	if isDomainSkipped(extDomain) {
		log.Printf("[%s] skip-listed external domain ignored: %s", srcDomain, extDomain)
		return
	}

	db, err := openDomainDB(srcDomain)
	if err != nil {
		return
	}
	now := time.Now().UTC().Format(time.RFC3339)
	res, _ := db.Exec(
		`INSERT OR IGNORE INTO ext_links (ext_domain, found_at) VALUES (?, ?)`,
		extDomain, now)
	n, _ := res.RowsAffected()
	if n == 0 {
		return // already recorded
	}

	// Register in main DB (inherit parent's interval)
	if err := registerDomain(extDomain, parentInterval, srcDomain); err != nil {
		log.Printf("registerDomain %s (from %s): %v", extDomain, srcDomain, err)
		return
	}

	log.Printf("[%s] discovered external domain: %s", srcDomain, extDomain)

	// Notify UI
	broadcast("new_domain", map[string]string{
		"domain": extDomain,
		"parent": srcDomain,
	})

	// Init the new domain's DB and start its crawler
	if _, err := openDomainDB(extDomain); err != nil {
		log.Printf("openDomainDB %s: %v", extDomain, err)
		return
	}

	crawlersMu.Lock()
	if !crawlers[extDomain] {
		crawlers[extDomain] = true
		go crawlDomain(extDomain, parentInterval)
	}
	crawlersMu.Unlock()
}

// ─────────────────────────────────────────────────────────────────
//  Pause / resume machinery
// ─────────────────────────────────────────────────────────────────

func ensurePauseChannels(domain string) {
	pauseChsMu.Lock()
	defer pauseChsMu.Unlock()
	if _, ok := pauseChs[domain]; !ok {
		pauseChs[domain] = make(chan struct{}, 1)
		resumeChs[domain] = make(chan struct{}, 1)
	}
}

// pauseCrawler signals the crawler to pause. Non-blocking.
func pauseCrawler(domain string) {
	pauseChsMu.RLock()
	ch, ok := pauseChs[domain]
	pauseChsMu.RUnlock()
	if !ok {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}

// resumeCrawler signals a paused crawler to continue. Non-blocking.
func resumeCrawler(domain string) {
	pauseChsMu.RLock()
	ch, ok := resumeChs[domain]
	pauseChsMu.RUnlock()
	if !ok {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}

// checkPause is called inside the crawl loop between requests.
// If a pause signal is pending it blocks until resume arrives.
func checkPause(domain string, br *Broker) {
	pauseChsMu.RLock()
	pCh := pauseChs[domain]
	rCh := resumeChs[domain]
	pauseChsMu.RUnlock()

	select {
	case <-pCh:
		setDomainStatus(domain, statusPaused)
		emit(br, "paused", map[string]string{"domain": domain})
		log.Printf("[%s] paused", domain)
		// drain any duplicate pause signals
		for len(pCh) > 0 {
			<-pCh
		}
		// block until resume
		<-rCh
		setDomainStatus(domain, statusRunning)
		emit(br, "resumed", map[string]string{"domain": domain})
		log.Printf("[%s] resumed", domain)
	default:
	}
}

// ─────────────────────────────────────────────────────────────────
//  robots.txt
// ─────────────────────────────────────────────────────────────────

type robotsRules struct {
	disallowed []string
	crawlDelay int
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
			inSection = agent == "*" || strings.EqualFold(agent, "siliconpin_spider")
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
//  Link extractor
// ─────────────────────────────────────────────────────────────────

var hrefRe = regexp.MustCompile(`(?i)href=["']([^"'#][^"']*)["']`)

type extractedLinks struct {
	sameHost []string
	external []string // distinct external hostnames (not full URLs)
}

func extractLinks(base *url.URL, body string) extractedLinks {
	seenSame := map[string]bool{}
	seenExt := map[string]bool{}
	var result extractedLinks

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
		host := strings.ToLower(resolved.Hostname())
		baseHost := strings.ToLower(base.Hostname())

		if host == baseHost {
			s := resolved.String()
			if !seenSame[s] {
				seenSame[s] = true
				result.sameHost = append(result.sameHost, s)
			}
		} else {
			// strip www. for normalisation
			extDomain := strings.TrimPrefix(host, "www.")
			if extDomain != "" && !seenExt[extDomain] && isValidDomain(extDomain) {
				seenExt[extDomain] = true
				result.external = append(result.external, extDomain)
			}
		}
	}
	return result
}

// ─────────────────────────────────────────────────────────────────
//  Crawler goroutine
// ─────────────────────────────────────────────────────────────────

func crawlDomain(domain string, intervalSec int) {
	log.Printf("[%s] crawler started (interval %ds)", domain, intervalSec)
	br := getBroker(domain)
	ensurePauseChannels(domain)

	db, err := openDomainDB(domain)
	if err != nil {
		emit(br, "error", map[string]string{"msg": "DB open failed: " + err.Error()})
		return
	}

	setDomainStatus(domain, statusRunning)

	// robots.txt
	emit(br, "status", map[string]string{"msg": "fetching robots.txt"})
	robots := fetchRobots(domain)
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

	startURL := "https://" + domain + "/"
	seedQueue(db, startURL)

	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return fmt.Errorf("too many redirects")
			}
			// If the redirect crosses to a different host, register it as an
			// external domain so it gets discovered and crawled.
			redirectHost := strings.TrimPrefix(strings.ToLower(req.URL.Hostname()), "www.")
			originHost := strings.TrimPrefix(strings.ToLower(via[0].URL.Hostname()), "www.")
			if redirectHost != "" && redirectHost != originHost && isValidDomain(redirectHost) {
				go recordExtLink(domain, redirectHost, intervalSec)
			}
			return nil
		},
	}

	for {
		// ── pause check ─────────────────────────────────────────
		checkPause(domain, br)

		// ── re-read interval ────────────────────────────────────
		var cur int
		if mainDB.QueryRow(`SELECT interval FROM domains WHERE domain=?`, domain).
			Scan(&cur) == nil && cur > 0 {
			intervalSec = cur
		}

		target, ok := dequeueURL(db)
		if !ok {
			break
		}

		if isURLKnown(db, target) {
			continue
		}

		parsed, err := url.Parse(target)
		if err != nil {
			continue
		}
		if !robots.allowed(parsed.Path) {
			emit(br, "skipped", map[string]string{"url": target, "reason": "robots.txt"})
			continue
		}

		// random delay [interval, interval*2]
		delaySec := intervalSec + rand.Intn(intervalSec+1)
		emit(br, "waiting", map[string]interface{}{
			"url":     target,
			"delay_s": delaySec,
			"queue":   queueLen(db),
		})
		time.Sleep(time.Duration(delaySec) * time.Second)

		// ── pause check after sleep (could have been paused during wait) ──
		checkPause(domain, br)

		emit(br, "fetching", map[string]string{"url": target})
		resp, err := httpClient.Get(target)
		if err != nil {
			emit(br, "error", map[string]string{"url": target, "msg": err.Error()})
			log.Printf("[%s] fetch error %s: %v", domain, target, err)
			enqueueURL(db, target) // retry next run
			continue
		}

		ct := resp.Header.Get("Content-Type")
		isHTML := strings.Contains(ct, "text/html")

		var bodyStr string
		if isHTML {
			raw, _ := io.ReadAll(io.LimitReader(resp.Body, 5<<20))
			bodyStr = string(raw)
		}
		resp.Body.Close()

		if ins, _ := insertURL(db, target); ins {
			emit(br, "saved", map[string]interface{}{
				"url":          target,
				"status":       resp.StatusCode,
				"content_type": ct,
			})
			log.Printf("[%s] saved: %s", domain, target)
		}

		if isHTML && resp.StatusCode == 200 {
			links := extractLinks(parsed, bodyStr)

			// same-host links → queue
			newCount := 0
			for _, link := range links.sameHost {
				if !isURLKnown(db, link) {
					enqueueURL(db, link)
					newCount++
				}
			}
			emit(br, "links_found", map[string]interface{}{
				"url":       target,
				"found":     len(links.sameHost),
				"new":       newCount,
				"queue_len": queueLen(db),
				"external":  len(links.external),
			})

			// external domains → auto-register & crawl
			for _, extDomain := range links.external {
				recordExtLink(domain, extDomain, intervalSec)
			}
		}
	}

	setDomainStatus(domain, statusDone)
	emit(br, "done", map[string]string{"domain": domain, "msg": "crawl complete"})
	log.Printf("[%s] crawl complete", domain)

	crawlersMu.Lock()
	delete(crawlers, domain)
	crawlersMu.Unlock()
}

// ─────────────────────────────────────────────────────────────────
//  HTTP helpers
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

func jsonOK(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

// ─────────────────────────────────────────────────────────────────
//  Handlers
// ─────────────────────────────────────────────────────────────────

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
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		jsonOK(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
		return
	}
	if body.Domain == "" {
		jsonOK(w, http.StatusBadRequest, map[string]string{"error": "domain is required"})
		return
	}
	domain := sanitizeDomain(body.Domain)
	if !isValidDomain(domain) {
		jsonOK(w, http.StatusBadRequest, map[string]string{"error": "invalid domain"})
		return
	}
	interval := 60
	if body.CrawlDelay != "" {
		fmt.Sscanf(body.CrawlDelay, "%d", &interval)
		if interval <= 0 {
			interval = 60
		}
	}

	if err := registerDomain(domain, interval, ""); err != nil {
		jsonOK(w, http.StatusInternalServerError, map[string]string{"error": "db error"})
		return
	}
	if _, err := openDomainDB(domain); err != nil {
		jsonOK(w, http.StatusInternalServerError, map[string]string{"error": "domain DB init failed"})
		return
	}

	crawlersMu.Lock()
	if !crawlers[domain] {
		crawlers[domain] = true
		go crawlDomain(domain, interval)
	}
	crawlersMu.Unlock()

	broadcast("new_domain", map[string]string{"domain": domain, "parent": ""})

	jsonOK(w, http.StatusCreated, map[string]interface{}{
		"message":  "domain added, crawler started",
		"domain":   domain,
		"interval": interval,
		"db_file":  domainDataDir + "/" + domain + ".sqlite",
		"sse":      "/api/sse/" + domain,
	})
}

// GET /api/domains
func domainsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	list, err := listDomains()
	if err != nil {
		jsonOK(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if list == nil {
		list = []DomainRow{}
	}
	jsonOK(w, http.StatusOK, list)
}

// POST /api/pause/{domain}
func pauseHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	domain := sanitizeDomain(strings.TrimPrefix(r.URL.Path, "/api/pause/"))
	if !isValidDomain(domain) {
		jsonOK(w, http.StatusBadRequest, map[string]string{"error": "invalid domain"})
		return
	}

	crawlersMu.Lock()
	running := crawlers[domain]
	crawlersMu.Unlock()

	if !running {
		jsonOK(w, http.StatusConflict, map[string]string{"error": "crawler not running for this domain"})
		return
	}

	pauseCrawler(domain)
	jsonOK(w, http.StatusOK, map[string]string{"message": "pause signal sent", "domain": domain})
}

// POST /api/resume/{domain}
func resumeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	domain := sanitizeDomain(strings.TrimPrefix(r.URL.Path, "/api/resume/"))
	if !isValidDomain(domain) {
		jsonOK(w, http.StatusBadRequest, map[string]string{"error": "invalid domain"})
		return
	}
	resumeCrawler(domain)
	jsonOK(w, http.StatusOK, map[string]string{"message": "resume signal sent", "domain": domain})
}

// GET /api/sse/{domain}   — or /api/sse/  (global stream for all domains)
func sseHandler(w http.ResponseWriter, r *http.Request) {
	rawDomain := strings.TrimPrefix(r.URL.Path, "/api/sse/")
	rawDomain = strings.TrimRight(rawDomain, "/")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// If no domain specified, subscribe to the global "__all__" broker
	// which receives broadcasts (new_domain, shutdown, etc.)
	domainKey := rawDomain
	if domainKey == "" {
		domainKey = "__all__"
	} else {
		domainKey = sanitizeDomain(domainKey)
		if !isValidDomain(domainKey) && domainKey != "__all__" {
			http.Error(w, "invalid domain", http.StatusBadRequest)
			return
		}
	}

	br := getBroker(domainKey)
	ch := br.subscribe()
	defer br.unsubscribe(ch)

	log.Printf("[SSE] client connected → %s", domainKey)
	fmt.Fprintf(w, "data: {\"event\":\"connected\",\"data\":{\"domain\":%q}}\n\n", domainKey)
	flusher.Flush()

	ticker := time.NewTicker(25 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			log.Printf("[SSE] client disconnected → %s", domainKey)
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
//  broadcast helper – publish to __all__ broker
// ─────────────────────────────────────────────────────────────────

func init() {
	// ensure the global broadcast broker always exists
	getBroker("__all__")
}

// override broadcast to also send to __all__
func broadcastAll(event string, data interface{}) {
	b, _ := json.Marshal(ssePayload{Event: event, Data: data})
	msg := string(b)

	brokersMu.RLock()
	defer brokersMu.RUnlock()
	for _, br := range brokers {
		br.publish(msg)
	}
}

// ─────────────────────────────────────────────────────────────────
//  main
// ─────────────────────────────────────────────────────────────────

func main() {
	rand.Seed(time.Now().UnixNano()) //nolint:staticcheck

	if err := os.MkdirAll(appDataDir, 0o755); err != nil {
		log.Fatalf("mkdir %s: %v", appDataDir, err)
	}
	if err := os.MkdirAll(domainDataDir, 0o755); err != nil {
		log.Fatalf("mkdir %s: %v", domainDataDir, err)
	}
	if err := os.MkdirAll("static", 0o755); err != nil {
		log.Fatalf("mkdir static: %v", err)
	}

	initSkipDB()
	initMainDB()

	// Resume domains from previous run
	rows, err := mainDB.Query(`SELECT domain, interval, status FROM domains`)
	if err == nil {
		for rows.Next() {
			var d, status string
			var iv int
			if rows.Scan(&d, &iv, &status) != nil {
				continue
			}
			// don't restart completed or paused crawls automatically;
			// only restart those that were mid-flight (running/pending)
			if status == statusDone {
				continue
			}
			crawlersMu.Lock()
			if !crawlers[d] {
				crawlers[d] = true
				// reset status so it shows running
				setDomainStatus(d, statusPending)
				go crawlDomain(d, iv)
			}
			crawlersMu.Unlock()
		}
		rows.Close()
	}

	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.Dir("./static")))
	mux.HandleFunc("/api/add_domain", addDomainHandler)
	mux.HandleFunc("/api/domains", domainsHandler)
	mux.HandleFunc("/api/pause/", pauseHandler)
	mux.HandleFunc("/api/resume/", resumeHandler)
	mux.HandleFunc("/api/sse/", sseHandler)

	srv := &http.Server{Addr: ":8080", Handler: mux}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Printf("siliconpin_spider listening on :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server: %v", err)
		}
	}()

	<-quit
	log.Println("shutting down…")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv.Shutdown(ctx) //nolint:errcheck

	brokersMu.RLock()
	for d, br := range brokers {
		emit(br, "shutdown", map[string]string{"domain": d, "msg": "server stopping"})
	}
	brokersMu.RUnlock()
	time.Sleep(500 * time.Millisecond)

	domainDBsMu.RLock()
	for d, db := range domainDBs {
		db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`) //nolint:errcheck
		db.Close()
		log.Printf("checkpointed %s/%s.sqlite", domainDataDir, d)
	}
	domainDBsMu.RUnlock()

	mainDB.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`) //nolint:errcheck
	mainDB.Close()
	skipDB.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`) //nolint:errcheck
	skipDB.Close()
	log.Println("goodbye.")
}
