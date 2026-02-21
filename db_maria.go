package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

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
	if err = createCrawlTable(); err != nil {
		log.Fatalf("create MariaDB table %s: %v", mariaTable, err)
	}
	if err = createDomainsTable(); err != nil {
		log.Fatalf("create MariaDB table %s: %v", mariaTableDomains, err)
	}
	log.Printf("MariaDB ready: table=%s, domains_table=%s", mariaTable, mariaTableDomains)
}

func createCrawlTable() error {
	_, err := mariaDB.Exec(fmt.Sprintf(`
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
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, mariaTable))
	return err
}

func createDomainsTable() error {
	_, err := mariaDB.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id            BIGINT       NOT NULL AUTO_INCREMENT,
			domain        VARCHAR(253) NOT NULL,
			parent        VARCHAR(253) NOT NULL DEFAULT '',
			interval_sec  INT          NOT NULL DEFAULT 60,
			discovered_at DATETIME     NOT NULL,
			PRIMARY KEY (id),
			UNIQUE KEY uq_domain (domain),
			KEY idx_parent (parent)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, mariaTableDomains))
	return err
}

// ─────────────────────────────────────────────────────────────────
//  Query helpers
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
	// MySQL: 1 = inserted, 2 = updated, 0 = no-op
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

// insertExtLink records a discovered external domain link. Returns true if new.
func insertExtLink(srcDomain, extDomain string) bool {
	now := nowStr()
	q := fmt.Sprintf(`
		INSERT IGNORE INTO %s (kind, domain, value, created_at, updated_at)
		VALUES ('ext_link', ?, ?, ?, ?)`, mariaTable)
	res, _ := mariaDB.Exec(q, srcDomain, extDomain, now, now)
	n, _ := res.RowsAffected()
	return n > 0
}

// insertDiscoveredDomain records an auto-discovered domain in the domains table.
func insertDiscoveredDomain(domain, parent string, intervalSec int) {
	now := nowStr()
	q := fmt.Sprintf(`
		INSERT IGNORE INTO %s (domain, parent, interval_sec, discovered_at)
		VALUES (?, ?, ?, ?)`, mariaTableDomains)
	if _, err := mariaDB.Exec(q, domain, parent, intervalSec, now); err != nil {
		log.Printf("insertDiscoveredDomain %s: %v", domain, err)
	}
}
