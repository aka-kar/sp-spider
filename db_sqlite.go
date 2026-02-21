package main

import (
	"database/sql"
	"log"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// ─────────────────────────────────────────────────────────────────
//  Main DB – siliconpin_spider.sqlite (domain registry)
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
