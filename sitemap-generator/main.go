package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"encoding/xml"
)

// Sitemap XML structures
type SitemapIndex struct {
	XMLName  xml.Name  `xml:"sitemapindex"`
	Xmlns    string    `xml:"xmlns,attr"`
	Sitemaps []Sitemap `xml:"sitemap"`
}

type Sitemap struct {
	XMLName xml.Name `xml:"sitemap"`
	Loc     string   `xml:"loc"`
	LastMod string   `xml:"lastmod"`
}

type URLSet struct {
	XMLName xml.Name `xml:"urlset"`
	Xmlns   string   `xml:"xmlns,attr"`
	URLs    []URL    `xml:"url"`
}

type URL struct {
	Loc        string `xml:"loc"`
	LastMod    string `xml:"lastmod,omitempty"`
	ChangeFreq string `xml:"changefreq,omitempty"`
	Priority   string `xml:"priority,omitempty"`
}

type SitemapRobotsRules struct {
	Disallow   []string
	Allow      []string
	CrawlDelay int
}

// Path segments that indicate action/utility endpoints — not indexable content pages.
var skipPathSegments = []string{
	"subscribe", "unsubscribe", "login", "logout", "register", "signup",
	"sign-up", "sign-in", "cart", "checkout", "payment", "account",
	"password", "reset", "confirm", "verify", "oauth", "auth",
	"wp-admin", "wp-login", "wp-json", "xmlrpc",
	"feed", "rss", "sitemap",
	"cdn-cgi", "wp-content", "wp-includes",
}

// File extensions to skip (not HTML content pages).
var skipExtensions = []string{
	".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico",
	".css", ".js", ".woff", ".woff2", ".ttf", ".eot",
	".pdf", ".zip", ".tar", ".gz", ".mp4", ".mp3", ".avi",
	".xml", ".json", ".txt", ".csv",
}

var (
	sitemapUserAgent = "SitemapGenerator/1.0"
	sitemapClient    = &http.Client{
		Timeout: 30 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return fmt.Errorf("too many redirects")
			}
			return nil
		},
		Transport: &http.Transport{
			MaxIdleConns:          10,
			IdleConnTimeout:       30 * time.Second,
			DisableCompression:    true,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
)

// Database structure for sitemap pages
type SitemapPage struct {
	ID          int       `json:"id"`
	URL         string    `json:"url"`
	Domain      string    `json:"domain"`
	Status      string    `json:"status"`       // 'found', '404', 'error', 'skipped'
	StatusCode  int       `json:"status_code"`  // HTTP status code
	ContentType string    `json:"content_type"` // 'html', 'css', 'svg', etc.
	HTML        int       `json:"html"`         // 1 if HTML page, 0 if not
	QStatus     string    `json:"q_status"`     // 'pending', 'processing', 'complete'
	Attempts    int       `json:"attempts"`     // Retry attempts
	FoundAt     time.Time `json:"found_at"`
	LastChecked time.Time `json:"last_checked"`
}

// Initialize database for sitemap
func initDB(domain string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", fmt.Sprintf("%s.sqlite", domain))
	if err != nil {
		return nil, err
	}

	// Create unified table with queue and content tracking
	createTable := `
	CREATE TABLE IF NOT EXISTS sitemap_pages (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		url TEXT UNIQUE NOT NULL,
		domain TEXT NOT NULL,
		status TEXT NOT NULL DEFAULT 'found',
		status_code INTEGER DEFAULT 200,
		content_type TEXT DEFAULT 'unknown',
		html INTEGER DEFAULT 0,
		q_status TEXT DEFAULT 'pending',
		priority INTEGER DEFAULT 1,
		attempts INTEGER DEFAULT 0,
		found_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		last_checked DATETIME DEFAULT CURRENT_TIMESTAMP,
		processed_at DATETIME
	);
	CREATE INDEX IF NOT EXISTS idx_domain ON sitemap_pages(domain);
	CREATE INDEX IF NOT EXISTS idx_status ON sitemap_pages(status);
	CREATE INDEX IF NOT EXISTS idx_html ON sitemap_pages(html);
	CREATE INDEX IF NOT EXISTS idx_q_status ON sitemap_pages(q_status);
	CREATE INDEX IF NOT EXISTS idx_priority ON sitemap_pages(priority DESC, id);
	`

	_, err = db.Exec(createTable)
	if err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

// Save page to database
func savePage(db *sql.DB, url, domain, status string, statusCode int, contentType string, html int) error {
	query := `
	INSERT OR REPLACE INTO sitemap_pages (url, domain, status, status_code, content_type, html, last_checked)
	VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
	`
	_, err := db.Exec(query, url, domain, status, statusCode, contentType, html)
	return err
}

// Get all pages from database
func getPages(db *sql.DB) ([]SitemapPage, error) {
	rows, err := db.Query(`
		SELECT id, url, domain, status, status_code, content_type, html, q_status, attempts, found_at, last_checked 
		FROM sitemap_pages 
		ORDER BY url
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pages []SitemapPage
	for rows.Next() {
		var page SitemapPage
		err := rows.Scan(&page.ID, &page.URL, &page.Domain, &page.Status,
			&page.StatusCode, &page.ContentType, &page.HTML, &page.QStatus,
			&page.Attempts, &page.FoundAt, &page.LastChecked)
		if err != nil {
			return nil, err
		}
		pages = append(pages, page)
	}
	return pages, nil
}

// Add URL to queue
func addToQueue(db *sql.DB, url, domain string, priority int) error {
	query := `
	INSERT OR IGNORE INTO sitemap_pages (url, domain, q_status, priority, html)
	VALUES (?, ?, 'pending', ?, 0)
	`
	_, err := db.Exec(query, url, domain, priority)
	return err
}

// Get next URL from queue
func getNextFromQueue(db *sql.DB) (string, error) {
	query := `
	UPDATE sitemap_pages 
	SET q_status = 'processing', processed_at = CURRENT_TIMESTAMP
	WHERE id = (
		SELECT id FROM sitemap_pages 
		WHERE q_status = 'pending' 
		ORDER BY priority DESC, id 
		LIMIT 1
	)
	RETURNING url
	`

	var url string
	err := db.QueryRow(query).Scan(&url)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil // No more URLs in queue
		}
		return "", err
	}
	return url, nil
}

// Mark queue item as complete
func markQueueComplete(db *sql.DB, url string) error {
	query := `
	UPDATE sitemap_pages 
	SET q_status = 'complete' 
	WHERE url = ?
	`
	_, err := db.Exec(query, url)
	return err
}

// Mark queue item as failed (no retry - just save status and continue)
func markQueueFailed(db *sql.DB, url string) error {
	query := `
	UPDATE sitemap_pages 
	SET q_status = 'complete'
	WHERE url = ?
	`
	_, err := db.Exec(query, url)
	return err
}

// Get queue statistics
func getQueueStats(db *sql.DB) (pending, processing, complete int) {
	db.QueryRow("SELECT COUNT(*) FROM sitemap_pages WHERE q_status = 'pending'").Scan(&pending)
	db.QueryRow("SELECT COUNT(*) FROM sitemap_pages WHERE q_status = 'processing'").Scan(&processing)
	db.QueryRow("SELECT COUNT(*) FROM sitemap_pages WHERE q_status = 'complete'").Scan(&complete)
	return
}

func main() {
	flag.Parse()
	if len(flag.Args()) < 1 || len(flag.Args()) > 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s <domain> [format] [delay]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Formats: xml (default), txt, json\n")
		fmt.Fprintf(os.Stderr, "Delay: seconds between requests (0-99, default: 2)\n")
		os.Exit(1)
	}

	domain := flag.Arg(0)
	format := "xml"
	delay := 2

	if len(flag.Args()) >= 2 {
		format = strings.ToLower(flag.Arg(1))
	}
	if len(flag.Args()) == 3 {
		fmt.Sscanf(flag.Arg(2), "%d", &delay)
		if delay < 0 {
			delay = 0
		}
		if delay > 99 {
			delay = 99
		}
	}

	if format != "xml" && format != "txt" && format != "json" {
		log.Fatalf("Invalid format: %s. Supported: xml, txt, json", format)
	}

	if !isValidSitemapDomain(domain) {
		log.Fatalf("Invalid domain: %s", domain)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		<-sigCh
		log.Println("Interrupted, shutting down...")
		cancel()
	}()

	log.Printf("Generating sitemap for %s in %s format (delay: %ds)", domain, format, delay)

	// Initialize database
	db, dbErr := initDB(domain)
	if dbErr != nil {
		log.Fatalf("Failed to initialize database: %v", dbErr)
	}
	defer db.Close()

	robots := fetchSitemapRobots(domain)
	log.Printf("Robots.txt: %d disallowed, %d allowed paths", len(robots.Disallow), len(robots.Allow))

	// Crawl and save to database
	urls := crawlSitemapDomain(ctx, domain, robots, delay, db)
	log.Printf("Crawled %d URLs, saved to %s.sqlite", len(urls), domain)

	// Generate sitemap from database
	pages, pageErr := getPages(db)
	if pageErr != nil {
		log.Fatalf("Failed to get pages from database: %v", pageErr)
	}

	// Extract URLs for sitemap generation (only HTML pages with html=1)
	var sitemapURLs []string
	for _, page := range pages {
		if page.Status == "found" && page.StatusCode == 200 && page.HTML == 1 {
			sitemapURLs = append(sitemapURLs, page.URL)
		}
	}

	log.Printf("Generating sitemap with %d valid URLs", len(sitemapURLs))

	var genErr error
	switch format {
	case "xml":
		genErr = generateSitemapXML(domain, sitemapURLs)
	case "txt":
		genErr = generateSitemapTXT(domain, sitemapURLs)
	case "json":
		genErr = generateSitemapJSON(domain, sitemapURLs)
	}

	if genErr != nil {
		log.Fatalf("Failed to generate sitemap: %v", genErr)
	}

	log.Printf("Done: %s.%s — %d URLs", domain, format, len(sitemapURLs))
}

func fetchSitemapRobots(domain string) *SitemapRobotsRules {
	robotsURL := fmt.Sprintf("https://%s/robots.txt", domain)
	req, err := http.NewRequest("GET", robotsURL, nil)
	if err != nil {
		return &SitemapRobotsRules{}
	}
	req.Header.Set("User-Agent", sitemapUserAgent)

	resp, err := sitemapClient.Do(req)
	if err != nil {
		log.Printf("robots.txt fetch failed: %v — continuing with no restrictions", err)
		return &SitemapRobotsRules{}
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		log.Printf("robots.txt status %d — continuing with no restrictions", resp.StatusCode)
		return &SitemapRobotsRules{}
	}

	rules := &SitemapRobotsRules{}
	content := make([]byte, 8192)
	n, _ := resp.Body.Read(content)
	lines := strings.Split(string(content[:n]), "\n")

	applyToUs := false
	for _, line := range lines {
		line = strings.TrimSpace(line)
		lower := strings.ToLower(line)

		if strings.HasPrefix(lower, "user-agent:") {
			agent := strings.TrimSpace(line[len("user-agent:"):])
			applyToUs = agent == "*" || strings.EqualFold(agent, "sitemapgenerator")
		} else if applyToUs {
			if strings.HasPrefix(lower, "disallow:") {
				path := strings.TrimSpace(line[len("disallow:"):])
				if path != "" {
					rules.Disallow = append(rules.Disallow, path)
				}
			} else if strings.HasPrefix(lower, "allow:") {
				path := strings.TrimSpace(line[len("allow:"):])
				if path != "" {
					rules.Allow = append(rules.Allow, path)
				}
			} else if strings.HasPrefix(lower, "crawl-delay:") {
				fmt.Sscanf(strings.TrimSpace(line[len("crawl-delay:"):]), "%d", &rules.CrawlDelay)
			}
		}
	}

	log.Printf("Robots.txt parsed OK")
	return rules
}

// crawlSitemapDomain uses database queue for persistent, resumable crawling
func crawlSitemapDomain(ctx context.Context, domain string, robots *SitemapRobotsRules, delay int, db *sql.DB) []string {
	baseURL := fmt.Sprintf("https://%s/", domain)
	var result []string

	// Add root URL to queue to start
	addToQueue(db, baseURL, domain, 2) // High priority for root

	log.Printf("Starting database-driven crawl for %s", domain)

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			log.Println("Crawl cancelled")
			return result
		default:
		}

		// Get next URL from queue
		pageURL, err := getNextFromQueue(db)
		if err != nil {
			log.Printf("Error getting URL from queue: %v", err)
			continue
		}
		if pageURL == "" {
			// No more URLs in queue
			log.Println("Queue empty, crawl complete")
			break
		}

		// Skip non-content URLs before even fetching them
		if !isContentURL(pageURL) {
			log.Printf("SKIP (non-content): %s", pageURL)
			markQueueComplete(db, pageURL)
			continue
		}

		// Skip disallowed paths
		if !isSitemapAllowed(pageURL, robots) {
			log.Printf("SKIP (robots.txt): %s", pageURL)
			markQueueComplete(db, pageURL)
			continue
		}

		// Show queue statistics
		pending, processing, complete := getQueueStats(db)
		log.Printf("Crawling [%d total, queue:%d pending, %d processing]: %s",
			complete+1, pending, processing, pageURL)

		// Rate limit: wait before each request
		if delay > 0 {
			select {
			case <-time.After(time.Duration(delay) * time.Second):
			case <-ctx.Done():
				return result
			}
		}

		// Fetch the page
		newURLs := fetchSitemapPage(ctx, pageURL, domain, db)
		if newURLs == nil {
			// Fetch failed - mark as complete and continue (no retry)
			markQueueFailed(db, pageURL)
			continue
		}

		// Success - add to results and mark queue complete
		result = append(result, pageURL)
		markQueueComplete(db, pageURL)

		// Add new URLs to queue
		for _, u := range newURLs {
			addToQueue(db, u, domain, 1) // Normal priority for discovered URLs
		}
	}

	return result
}

// isContentURL returns false for URLs that are action endpoints or static assets.
func isContentURL(rawURL string) bool {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return false
	}

	path := strings.ToLower(parsed.Path)

	// Skip by file extension
	for _, ext := range skipExtensions {
		if strings.HasSuffix(path, ext) {
			return false
		}
	}

	// Skip by path segment
	segments := strings.Split(path, "/")
	for _, seg := range segments {
		seg = strings.TrimSpace(seg)
		for _, skip := range skipPathSegments {
			if seg == skip {
				return false
			}
		}
	}

	return true
}

// fetchSitemapPage fetches a page and returns links found on it.
// Returns nil if the page couldn't be fetched or isn't HTML.
func fetchSitemapPage(ctx context.Context, pageURL, domain string, db *sql.DB) []string {
	req, err := http.NewRequestWithContext(ctx, "GET", pageURL, nil)
	if err != nil {
		return nil
	}
	req.Header.Set("User-Agent", sitemapUserAgent)

	resp, err := sitemapClient.Do(req)
	if err != nil {
		log.Printf("  fetch error: %v", err)
		// Save error status to database (html=0, not for sitemap)
		if db != nil {
			savePage(db, pageURL, domain, "error", 0, "error", 0)
		}
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		log.Printf("  HTTP %d — skipping", resp.StatusCode)
		// Save status to database (html=0, not for sitemap)
		if db != nil {
			status := "error"
			if resp.StatusCode == 404 {
				status = "404"
			}
			savePage(db, pageURL, domain, status, resp.StatusCode, "error", 0)
		}
		return nil
	}

	contentType := resp.Header.Get("Content-Type")
	if !strings.Contains(contentType, "text/html") {
		// Save non-HTML content to database (html=0, not for sitemap)
		if db != nil {
			// Extract content type from header
			ct := "unknown"
			if strings.Contains(contentType, "text/css") {
				ct = "css"
			} else if strings.Contains(contentType, "image/") {
				ct = "image"
			} else if strings.Contains(contentType, "application/") {
				ct = "application"
			} else if strings.Contains(contentType, "text/") {
				ct = "text"
			} else if strings.Contains(contentType, "font/") {
				ct = "font"
			} else if strings.Contains(contentType, "javascript") {
				ct = "js"
			}
			savePage(db, pageURL, domain, "found", resp.StatusCode, ct, 0)
		}
		return nil
	}

	body := make([]byte, 1024*1024*8) // 8MB cap
	n, _ := resp.Body.Read(body)

	// Save successful HTML page to database (html=1, for sitemap)
	if db != nil {
		savePage(db, pageURL, domain, "found", resp.StatusCode, "html", 1)
	}

	return extractSitemapLinks(string(body[:n]), domain)
}

func isSitemapAllowed(urlStr string, robots *SitemapRobotsRules) bool {
	parsed, err := url.Parse(urlStr)
	if err != nil {
		return false
	}
	path := parsed.Path

	for _, disallow := range robots.Disallow {
		if disallow == "" {
			continue
		}
		if strings.HasPrefix(path, disallow) {
			// A more-specific Allow overrides
			for _, allow := range robots.Allow {
				if strings.HasPrefix(path, allow) && len(allow) > len(disallow) {
					return true
				}
			}
			return false
		}
	}
	return true
}

func extractSitemapLinks(content, domain string) []string {
	hrefRegex := regexp.MustCompile(`href=["']([^"']+)["']`)
	matches := hrefRegex.FindAllStringSubmatch(content, -1)

	seen := make(map[string]bool)
	var urls []string

	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		href := strings.TrimSpace(match[1])

		// Skip non-navigable schemes and fragments
		if strings.HasPrefix(href, "javascript:") ||
			strings.HasPrefix(href, "mailto:") ||
			strings.HasPrefix(href, "tel:") ||
			strings.HasPrefix(href, "#") ||
			strings.HasPrefix(href, "data:") {
			continue
		}

		// Resolve relative URLs
		if strings.HasPrefix(href, "/") {
			href = fmt.Sprintf("https://%s%s", domain, href)
		} else if !strings.HasPrefix(href, "http://") && !strings.HasPrefix(href, "https://") {
			href = fmt.Sprintf("https://%s/%s", domain, href)
		}

		parsed, err := url.Parse(href)
		if err != nil {
			continue
		}

		// Same domain only (strip www. for comparison)
		host := strings.TrimPrefix(parsed.Hostname(), "www.")
		if host != strings.TrimPrefix(domain, "www.") {
			continue
		}

		// Normalize: strip fragment and query string
		parsed.Fragment = ""
		parsed.RawQuery = ""
		normalized := parsed.String()

		if !seen[normalized] {
			seen[normalized] = true
			urls = append(urls, normalized)
		}
	}

	return urls
}

func isValidSitemapDomain(domain string) bool {
	if len(domain) == 0 {
		return false
	}
	r := regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$`)
	return r.MatchString(domain)
}

func generateSitemapTXT(domain string, urls []string) error {
	file, err := os.Create(fmt.Sprintf("%s.txt", domain))
	if err != nil {
		return err
	}
	defer file.Close()
	for _, u := range urls {
		file.WriteString(u + "\n")
	}
	return nil
}

func generateSitemapJSON(domain string, urls []string) error {
	file, err := os.Create(fmt.Sprintf("%s.json", domain))
	if err != nil {
		return err
	}
	defer file.Close()

	sitemap := struct {
		Domain string   `json:"domain"`
		URLs   []string `json:"urls"`
		Count  int      `json:"count"`
	}{domain, urls, len(urls)}

	data, err := json.MarshalIndent(sitemap, "", "  ")
	if err != nil {
		return err
	}
	file.Write(data)
	return nil
}

func generateSitemapXML(domain string, urls []string) error {
	file, err := os.Create(fmt.Sprintf("%s.xml", domain))
	if err != nil {
		return err
	}
	defer file.Close()

	urlSet := URLSet{Xmlns: "http://www.sitemaps.org/schemas/sitemap/0.9"}
	now := time.Now().Format("2006-01-02")
	for _, u := range urls {
		urlSet.URLs = append(urlSet.URLs, URL{
			Loc:        u,
			LastMod:    now,
			ChangeFreq: "weekly",
			Priority:   "0.8",
		})
	}

	file.WriteString(xml.Header)
	enc := xml.NewEncoder(file)
	enc.Indent("", "  ")
	return enc.Encode(urlSet)
}
