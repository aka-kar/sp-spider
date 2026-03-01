package main

import (
	"database/sql"
	"log"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// ─────────────────────────────────────────────────────────────────
//  Main DB – siliconpin_spider.sqlite (domain registry)
// ─────────────────────────────────────────────────────────────────

var mainDBMu sync.Mutex

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
			internal   INTEGER  NOT NULL DEFAULT 0,
			external   INTEGER  NOT NULL DEFAULT 0,
			created_at DATETIME NOT NULL,
			updated_at DATETIME NOT NULL
		)`)
	if err != nil {
		log.Fatalf("create domains table: %v", err)
	}

	// Add new columns to existing tables if they don't exist
	mainDB.Exec(`ALTER TABLE domains ADD COLUMN internal INTEGER NOT NULL DEFAULT 0`) //nolint:errcheck
	mainDB.Exec(`ALTER TABLE domains ADD COLUMN external INTEGER NOT NULL DEFAULT 0`) //nolint:errcheck

	log.Printf("Main DB ready: %s", mainDBFile)
}

func setDomainStatus(domain, status string) {
	mainDBMu.Lock()
	defer mainDBMu.Unlock()

	now := time.Now().UTC().Format(time.RFC3339)
	mainDB.Exec(`UPDATE domains SET status=?, updated_at=? WHERE domain=?`, status, now, domain)
}

type DomainRow struct {
	ID        int    `json:"id"`
	Domain    string `json:"domain"`
	Interval  int    `json:"interval"`
	Status    string `json:"status"`
	Parent    string `json:"parent,omitempty"`
	Internal  int    `json:"internal"`
	External  int    `json:"external"`
	URLCount  int    `json:"url_count"`
	QueueLen  int    `json:"queue_len"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

func listDomains() ([]DomainRow, error) {
	rows, err := mainDB.Query(
		`SELECT id, domain, interval, status, parent, internal, external, created_at, updated_at
		 FROM domains ORDER BY id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []DomainRow
	for rows.Next() {
		var d DomainRow
		if err := rows.Scan(&d.ID, &d.Domain, &d.Interval, &d.Status,
			&d.Parent, &d.Internal, &d.External, &d.CreatedAt, &d.UpdatedAt); err != nil {
			continue
		}
		d.URLCount = mariaCount("url", d.Domain)
		d.QueueLen = mariaCount("queue", d.Domain)
		out = append(out, d)
	}
	return out, nil
}

func getDomainConfig(domain string) (interval int, internal int, external int, err error) {
	var iv, internalVal, externalVal int
	err = mainDB.QueryRow(`SELECT interval, internal, external FROM domains WHERE domain=?`, domain).Scan(&iv, &internalVal, &externalVal)
	return iv, internalVal, externalVal, err
}

func registerDomain(domain string, interval int, parentDomain string, internal int, external int) error {
	mainDBMu.Lock()
	defer mainDBMu.Unlock()

	now := time.Now().UTC().Format(time.RFC3339)
	_, err := mainDB.Exec(`
		INSERT INTO domains (domain, interval, status, parent, internal, external, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(domain) DO UPDATE SET
			interval=excluded.interval,
			internal=excluded.internal,
			external=excluded.external,
			updated_at=excluded.updated_at`,
		domain, interval, statusPending, parentDomain, internal, external, now, now)
	return err
}

// ─────────────────────────────────────────────────────────────────
//  Skip DB – skip_domain_list.sqlite
// ─────────────────────────────────────────────────────────────────

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
		{"tumblr.com", "blogging platform"},
		{"twitter.com", "social media platform"},
		{"instagram.com", "social media platform"},
		{"youtube.com", "video platform"},
		{"tiktok.com", "video platform"},
		{"reddit.com", "social media platform"},
		{"pinterest.com", "social media platform"},
	}
	now := time.Now().UTC().Format(time.RFC3339)
	for _, e := range defaults {
		skipDB.Exec(
			`INSERT OR IGNORE INTO skip_domains (domain, reason, created_at) VALUES (?, ?, ?)`,
			e.domain, e.reason, now)
	}
	log.Printf("Skip DB ready: %s", skipDBFile)
}

// isDomainSkipped returns true if domain or any parent suffix is in the skip list.
func isDomainSkipped(domain string) bool {
	// Normalize domain: remove trailing dots and convert to lowercase
	domain = strings.ToLower(strings.TrimSuffix(domain, "."))

	// Check exact match first
	var c int
	skipDB.QueryRow(`SELECT COUNT(1) FROM skip_domains WHERE domain = ?`, domain).Scan(&c)
	if c > 0 {
		return true
	}

	// Check if any skip entry matches as a suffix (subdomain check)
	// Using SQL LIKE for better performance: '%.tumblr.com' matches any subdomain of tumblr.com
	skipDB.QueryRow(`SELECT COUNT(1) FROM skip_domains WHERE ? LIKE '%' || domain || '.'`, domain).Scan(&c)
	if c > 0 {
		return true
	}

	// Also check if domain itself ends with any skip entry (for cases like 'sub.tumblr.com')
	skipDB.QueryRow(`SELECT COUNT(1) FROM skip_domains WHERE ? LIKE '%.' || domain`, domain).Scan(&c)
	return c > 0
}
