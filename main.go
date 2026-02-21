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

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
)

// ─────────────────────────────────────────────────────────────────
//  Constants & globals
// ─────────────────────────────────────────────────────────────────

const appDataDir = "./data/app"
const mainDBFile = appDataDir + "/siliconpin_spider.sqlite"
const skipDBFile = appDataDir + "/skip_domain_list.sqlite"

var mainDB *sql.DB  // siliconpin_spider.sqlite  – domain registry
var skipDB *sql.DB  // skip_domain_list.sqlite    – skip list
var mariaDB *sql.DB // MariaDB                  – crawled urls / queue / ext_links

// MariaDB table names (from env)
var mariaTable string
var mariaTableDomains string

// SSE brokers – one per domain
var (
	brokersMu sync.RWMutex
	brokers   = map[string]*Broker{}
)

// crawler goroutine guard
var (
	crawlersMu sync.Mutex
	crawlers   = map[string]bool{}
)

// shutdown coordination
var (
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
	crawlerWg      sync.WaitGroup
)

// pause/resume channels – one per domain
var (
	pauseChsMu sync.RWMutex
	pauseChs   = map[string]chan struct{}{}
	resumeChs  = map[string]chan struct{}{}
)

// domain status values stored in main DB
const (
	statusRunning = "running"
	statusPaused  = "paused"
	statusDone    = "done"
	statusPending = "pending"
)

// ─────────────────────────────────────────────────────────────────
//  .env loader (minimal, no external dependency)
// ─────────────────────────────────────────────────────────────────

func loadEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		return // .env is optional
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.Trim(strings.TrimSpace(parts[1]), `"'`)
		if os.Getenv(key) == "" { // don't override real env vars
			os.Setenv(key, val)
		}
	}
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("required env var %s is not set (check .env)", key)
	}
	return v
}

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
//  MariaDB init
// ─────────────────────────────────────────────────────────────────

func initMariaDB() {
	host := mustEnv("MARIA_DB_HOST")
	port := mustEnv("MARIA_DB_PORT")
	user := mustEnv("MARIA_DB_USER")
	pass := mustEnv("MARIA_DB_PASS")
	dbname := mustEnv("MARIA_DB_DATABASE")
	mariaTable = mustEnv("MARIA_DB_TABLE")
	mariaTableDomains = mustEnv("MARIA_DB_TABLE_DOMAINS")

	// DSN format: user:pass@tcp(host:port)/dbname?params
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, pass, host, port, dbname)
	var err error
	mariaDB, err = sql.Open("mysql", dsn+"?parseTime=true&timeout=10s&readTimeout=30s&writeTimeout=30s")
	if err != nil {
		log.Fatalf("open MariaDB: %v", err)
	}
	mariaDB.SetMaxOpenConns(25)
	mariaDB.SetMaxIdleConns(10)
	mariaDB.SetConnMaxLifetime(5 * time.Minute)

	if err = mariaDB.Ping(); err != nil {
		log.Fatalf("ping MariaDB: %v", err)
	}

	// Create the single unified table if it doesn't exist.
	// Three logical sections: urls, queue, ext_links – distinguished by `kind`.
	// Layout:
	//   kind: 'url' | 'queue' | 'ext_link'
	//   domain: the crawled domain
	//   value: the url or ext_domain value
	//   status / content_type only filled for kind='url'
	//   added_at always set
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id           BIGINT       NOT NULL AUTO_INCREMENT,
			kind         ENUM('url','queue','ext_link') NOT NULL,
			domain       VARCHAR(253) NOT NULL,
			value        TEXT         NOT NULL,
			content_type VARCHAR(255) NOT NULL DEFAULT '',
			http_status  SMALLINT     NOT NULL DEFAULT 0,
			created_at   DATETIME     NOT NULL,
			updated_at   DATETIME     NOT NULL,
			PRIMARY KEY (id),
			UNIQUE KEY uq_kind_domain_value (kind, domain, value(512)),
			KEY idx_domain (domain),
			KEY idx_kind_domain (kind, domain)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, mariaTable)

	if _, err = mariaDB.Exec(createSQL); err != nil {
		log.Fatalf("create MariaDB table %s: %v", mariaTable, err)
	}

	// Discovered-domains table
	createDomainsSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id           BIGINT       NOT NULL AUTO_INCREMENT,
			domain       VARCHAR(253) NOT NULL,
			parent       VARCHAR(253) NOT NULL DEFAULT '',
			interval_sec INT          NOT NULL DEFAULT 60,
			discovered_at DATETIME    NOT NULL,
			PRIMARY KEY (id),
			UNIQUE KEY uq_domain (domain),
			KEY idx_parent (parent)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, mariaTableDomains)

	if _, err = mariaDB.Exec(createDomainsSQL); err != nil {
		log.Fatalf("create MariaDB table %s: %v", mariaTableDomains, err)
	}
	log.Printf("MariaDB ready: table=%s, domains_table=%s", mariaTable, mariaTableDomains)
}

// ─────────────────────────────────────────────────────────────────
//  Main DB (siliconpin_spider.sqlite) helpers
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

func isDomainSkipped(domain string) bool {
	var c int
	skipDB.QueryRow(`SELECT COUNT(1) FROM skip_domains WHERE domain = ?`, domain).Scan(&c)
	if c > 0 {
		return true
	}
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
		d.URLCount = mariaCount("url", d.Domain)
		d.QueueLen = mariaCount("queue", d.Domain)
		out = append(out, d)
	}
	return out, nil
}

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
//  MariaDB domain-data helpers (replaces per-domain SQLite files)
// ─────────────────────────────────────────────────────────────────

func nowStr() string { return time.Now().UTC().Format("2006-01-02 15:04:05") }

func mariaCount(kind, domain string) int {
	var n int
	q := fmt.Sprintf(`SELECT COUNT(1) FROM %s WHERE kind=? AND domain=?`, mariaTable)
	mariaDB.QueryRow(q, kind, domain).Scan(&n)
	return n
}

// insertURL inserts a crawled URL. Returns true if it was new.
func insertURL(domain, rawURL, contentType string, httpStatus int) (bool, error) {
	now := nowStr()
	q := fmt.Sprintf(`
		INSERT INTO %s (kind, domain, value, content_type, http_status, created_at, updated_at)
		VALUES ('url', ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE updated_at=VALUES(updated_at)`, mariaTable)
	res, err := mariaDB.Exec(q, domain, rawURL, contentType, httpStatus, now, now)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	// MySQL: INSERT … ON DUPLICATE KEY UPDATE returns 1 for insert, 2 for update, 0 for no-op
	return n == 1, nil
}

func isURLKnown(domain, rawURL string) bool {
	var c int
	q := fmt.Sprintf(`SELECT COUNT(1) FROM %s WHERE kind='url' AND domain=? AND value=?`, mariaTable)
	mariaDB.QueryRow(q, domain, rawURL).Scan(&c)
	return c > 0
}

func enqueueURL(domain, rawURL string) {
	now := nowStr()
	q := fmt.Sprintf(`
		INSERT IGNORE INTO %s (kind, domain, value, created_at, updated_at)
		VALUES ('queue', ?, ?, ?, ?)`, mariaTable)
	mariaDB.Exec(q, domain, rawURL, now, now)
}

func dequeueURL(domain string) (string, bool) {
	tx, err := mariaDB.Begin()
	if err != nil {
		return "", false
	}
	defer tx.Rollback() //nolint:errcheck

	var id int64
	var rawURL string
	q := fmt.Sprintf(`SELECT id, value FROM %s WHERE kind='queue' AND domain=? ORDER BY id ASC LIMIT 1 FOR UPDATE`, mariaTable)
	if err = tx.QueryRow(q, domain).Scan(&id, &rawURL); err != nil {
		return "", false
	}
	dq := fmt.Sprintf(`DELETE FROM %s WHERE id=?`, mariaTable)
	if _, err = tx.Exec(dq, id); err != nil {
		return "", false
	}
	if err = tx.Commit(); err != nil {
		return "", false
	}
	return rawURL, true
}

func queueLen(domain string) int {
	return mariaCount("queue", domain)
}

func seedQueue(domain, startURL string) {
	if mariaCount("queue", domain) == 0 && mariaCount("url", domain) == 0 {
		enqueueURL(domain, startURL)
	}
}

// insertExtLink records a discovered external domain. Returns true if new.
func insertExtLink(srcDomain, extDomain string) bool {
	now := nowStr()
	q := fmt.Sprintf(`
		INSERT IGNORE INTO %s (kind, domain, value, created_at, updated_at)
		VALUES ('ext_link', ?, ?, ?, ?)`, mariaTable)
	res, _ := mariaDB.Exec(q, srcDomain, extDomain, now, now)
	n, _ := res.RowsAffected()
	return n > 0
}

// insertDiscoveredDomain records an auto-discovered domain in MariaDB.
func insertDiscoveredDomain(domain, parent string, intervalSec int) {
	now := nowStr()
	q := fmt.Sprintf(`
		INSERT IGNORE INTO %s (domain, parent, interval_sec, discovered_at)
		VALUES (?, ?, ?, ?)`, mariaTableDomains)
	if _, err := mariaDB.Exec(q, domain, parent, intervalSec, now); err != nil {
		log.Printf("insertDiscoveredDomain %s: %v", domain, err)
	}
}

// ─────────────────────────────────────────────────────────────────
//  recordExtLink – discover & register external domains
// ─────────────────────────────────────────────────────────────────

func recordExtLink(srcDomain, extDomain string, parentInterval int) {
	if isDomainSkipped(extDomain) {
		log.Printf("[%s] skip-listed external domain ignored: %s", srcDomain, extDomain)
		return
	}
	if !insertExtLink(srcDomain, extDomain) {
		return // already recorded
	}

	if err := registerDomain(extDomain, parentInterval, srcDomain); err != nil {
		log.Printf("registerDomain %s (from %s): %v", extDomain, srcDomain, err)
		return
	}

	log.Printf("[%s] discovered external domain: %s", srcDomain, extDomain)
	insertDiscoveredDomain(extDomain, srcDomain, parentInterval)
	broadcast("new_domain", map[string]string{
		"domain": extDomain,
		"parent": srcDomain,
	})

	crawlersMu.Lock()
	if !crawlers[extDomain] {
		crawlers[extDomain] = true
		crawlerWg.Add(1)
		go crawlDomain(shutdownCtx, extDomain, parentInterval)
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

// checkPause checks for a pending pause signal and blocks until resumed.
// Returns true if a pause-then-resume cycle happened.
func checkPause(domain string, br *Broker) bool {
	pauseChsMu.RLock()
	pCh := pauseChs[domain]
	rCh := resumeChs[domain]
	pauseChsMu.RUnlock()

	select {
	case <-pCh:
		for len(pCh) > 0 {
			<-pCh
		}
		setDomainStatus(domain, statusPaused)
		emit(br, "paused", map[string]string{"domain": domain})
		log.Printf("[%s] paused", domain)
		<-rCh
		setDomainStatus(domain, statusRunning)
		emit(br, "resumed", map[string]string{"domain": domain})
		log.Printf("[%s] resumed", domain)
		return true
	default:
		return false
	}
}

// interruptibleSleep sleeps for d but wakes immediately on a pause or shutdown signal.
// Returns true if interrupted (caller should re-queue URL and continue).
func interruptibleSleep(ctx context.Context, domain string, br *Broker, d time.Duration) bool {
	pauseChsMu.RLock()
	pCh := pauseChs[domain]
	pauseChsMu.RUnlock()

	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return true // treat shutdown as interrupt; caller will re-queue and exit on next loop check
	case <-timer.C:
		return false
	case <-pCh:
		for len(pCh) > 0 {
			<-pCh
		}
		pauseChsMu.RLock()
		rCh := resumeChs[domain]
		pauseChsMu.RUnlock()
		setDomainStatus(domain, statusPaused)
		emit(br, "paused", map[string]string{"domain": domain})
		log.Printf("[%s] paused (during wait)", domain)
		<-rCh
		setDomainStatus(domain, statusRunning)
		emit(br, "resumed", map[string]string{"domain": domain})
		log.Printf("[%s] resumed", domain)
		return true
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
	external []string
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

func crawlDomain(ctx context.Context, domain string, intervalSec int) {
	defer crawlerWg.Done()
	log.Printf("[%s] crawler started (interval %ds)", domain, intervalSec)
	br := getBroker(domain)
	ensurePauseChannels(domain)

	// checkPause immediately – handles the pre-loaded pause signal on restart
	checkPause(domain, br)

	setDomainStatus(domain, statusRunning)

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
	seedQueue(domain, startURL)

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
		// ── shutdown check ─────────────────────────────────────
		select {
		case <-ctx.Done():
			log.Printf("[%s] crawler stopping (shutdown)", domain)
			setDomainStatus(domain, statusPaused)
			return
		default:
		}
		// ── pause check ─────────────────────────────────────────
		checkPause(domain, br)

		// ── re-read interval ────────────────────────────────────
		var cur int
		if mainDB.QueryRow(`SELECT interval FROM domains WHERE domain=?`, domain).
			Scan(&cur) == nil && cur > 0 {
			intervalSec = cur
		}

		target, ok := dequeueURL(domain)
		if !ok {
			break
		}

		if isURLKnown(domain, target) {
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

		// random delay [interval, interval*2] – interruptible by pause
		delaySec := intervalSec + rand.Intn(intervalSec+1)
		emit(br, "waiting", map[string]interface{}{
			"url":     target,
			"delay_s": delaySec,
			"queue":   queueLen(domain),
		})
		if paused := interruptibleSleep(ctx, domain, br, time.Duration(delaySec)*time.Second); paused {
			// put the URL back so it is retried after resume
			enqueueURL(domain, target)
			continue
		}

		emit(br, "fetching", map[string]string{"url": target})
		resp, err := httpClient.Get(target)
		if err != nil {
			emit(br, "error", map[string]string{"url": target, "msg": err.Error()})
			log.Printf("[%s] fetch error %s: %v", domain, target, err)
			enqueueURL(domain, target) // retry next run
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

		if ins, _ := insertURL(domain, target, ct, resp.StatusCode); ins {
			emit(br, "saved", map[string]interface{}{
				"url":          target,
				"status":       resp.StatusCode,
				"content_type": ct,
			})
			log.Printf("[%s] saved: %s", domain, target)
		}

		if isHTML && resp.StatusCode == 200 {
			links := extractLinks(parsed, bodyStr)

			newCount := 0
			for _, link := range links.sameHost {
				if !isURLKnown(domain, link) {
					enqueueURL(domain, link)
					newCount++
				}
			}
			emit(br, "links_found", map[string]interface{}{
				"url":       target,
				"found":     len(links.sameHost),
				"new":       newCount,
				"queue_len": queueLen(domain),
				"external":  len(links.external),
			})

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

	crawlersMu.Lock()
	if !crawlers[domain] {
		crawlers[domain] = true
		crawlerWg.Add(1)
		go crawlDomain(shutdownCtx, domain, interval)
	}
	crawlersMu.Unlock()

	broadcast("new_domain", map[string]string{"domain": domain, "parent": ""})

	jsonOK(w, http.StatusCreated, map[string]interface{}{
		"message":  "domain added, crawler started",
		"domain":   domain,
		"interval": interval,
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

// GET /api/sse/{domain}  – or /api/sse/ (global)
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
//  init – ensure global SSE broker exists
// ─────────────────────────────────────────────────────────────────

func init() {
	getBroker("__all__")
}

// ─────────────────────────────────────────────────────────────────
//  main
// ─────────────────────────────────────────────────────────────────

func main() {
	rand.Seed(time.Now().UnixNano()) //nolint:staticcheck

	loadEnv(".env")

	shutdownCtx, shutdownCancel = context.WithCancel(context.Background())

	if err := os.MkdirAll(appDataDir, 0o755); err != nil {
		log.Fatalf("mkdir %s: %v", appDataDir, err)
	}
	if err := os.MkdirAll("static", 0o755); err != nil {
		log.Fatalf("mkdir static: %v", err)
	}

	initSkipDB()
	initMainDB()
	initMariaDB()

	// Resume domains from previous run
	rows, err := mainDB.Query(`SELECT domain, interval, status FROM domains`)
	if err == nil {
		for rows.Next() {
			var d, status string
			var iv int
			if rows.Scan(&d, &iv, &status) != nil {
				continue
			}
			// don't restart completed crawls; paused crawls restart but
			// immediately re-enter pause so the user's choice is preserved.
			if status == statusDone {
				continue
			}
			crawlersMu.Lock()
			if !crawlers[d] {
				crawlers[d] = true
				if status == statusPaused {
					// Pre-load a pause signal before the goroutine starts so
					// the crawler immediately blocks in its pause state.
					ensurePauseChannels(d)
					pauseCrawler(d)
				} else {
					setDomainStatus(d, statusPending)
				}
				crawlerWg.Add(1)
				go crawlDomain(shutdownCtx, d, iv)
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

	// 1. Signal all crawler goroutines to stop
	shutdownCancel()

	// 2. Stop accepting new HTTP requests
	httpCtx, httpCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer httpCancel()
	srv.Shutdown(httpCtx) //nolint:errcheck

	// 3. Notify SSE clients
	brokersMu.RLock()
	for d, br := range brokers {
		emit(br, "shutdown", map[string]string{"domain": d, "msg": "server stopping"})
	}
	brokersMu.RUnlock()

	// 4. Wait for all crawler goroutines to exit (max 30s)
	log.Println("waiting for crawlers to stop…")
	done := make(chan struct{})
	go func() { crawlerWg.Wait(); close(done) }()
	select {
	case <-done:
		log.Println("all crawlers stopped")
	case <-time.After(30 * time.Second):
		log.Println("timed out waiting for crawlers; forcing shutdown")
	}

	// 5. Now safe to close DBs
	mariaDB.Close()
	mainDB.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`) //nolint:errcheck
	mainDB.Close()
	skipDB.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`) //nolint:errcheck
	skipDB.Close()
	log.Println("goodbye.")
}
