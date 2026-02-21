package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"
)

// ─────────────────────────────────────────────────────────────────
//  Helpers
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
//  POST /api/add_domain
// ─────────────────────────────────────────────────────────────────

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

	tryStartCrawler(domain, interval)
	broadcast("new_domain", map[string]string{"domain": domain, "parent": ""})

	jsonOK(w, http.StatusCreated, map[string]interface{}{
		"message":  "domain added, crawler started",
		"domain":   domain,
		"interval": interval,
		"sse":      "/api/sse/" + domain,
	})
}

// ─────────────────────────────────────────────────────────────────
//  GET /api/domains
// ─────────────────────────────────────────────────────────────────

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

// ─────────────────────────────────────────────────────────────────
//  POST /api/pause/{domain}
// ─────────────────────────────────────────────────────────────────

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

// ─────────────────────────────────────────────────────────────────
//  POST /api/resume/{domain}
// ─────────────────────────────────────────────────────────────────

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

// ─────────────────────────────────────────────────────────────────
//  GET /api/sse/{domain}  – or /api/sse/ (global)
// ─────────────────────────────────────────────────────────────────

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

	fmt.Fprintf(w, "data: {\"event\":\"connected\",\"data\":{\"domain\":%q}}\n\n", domainKey)
	flusher.Flush()

	ticker := time.NewTicker(25 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
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
