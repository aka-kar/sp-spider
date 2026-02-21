package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// ─────────────────────────────────────────────────────────────────
//  Crawler slot management
// ─────────────────────────────────────────────────────────────────

// tryStartCrawler launches crawlDomain if no goroutine is already running for
// the domain AND a semaphore slot is available. Returns true if launched.
func tryStartCrawler(domain string, intervalSec int) bool {
	crawlersMu.Lock()
	defer crawlersMu.Unlock()
	if crawlers[domain] {
		return false // already running
	}
	select {
	case crawlerSem <- struct{}{}: // slot acquired
	default:
		log.Printf("[%s] max crawlers reached, queued as pending", domain)
		return false // no slot available; stays pending in DB
	}
	crawlers[domain] = true
	crawlerWg.Add(1)
	go func() {
		defer func() { <-crawlerSem }() // release slot when done
		crawlDomain(shutdownCtx, domain, intervalSec)
	}()
	return true
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

	tryStartCrawler(extDomain, parentInterval)
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
		return true
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
	defer func() {
		crawlersMu.Lock()
		delete(crawlers, domain)
		crawlersMu.Unlock()
	}()

	log.Printf("[%s] crawler started (interval %ds)", domain, intervalSec)
	br := getBroker(domain)
	ensurePauseChannels(domain)

	// checkPause immediately – handles the pre-loaded pause signal on restart
	checkPause(domain, br)

	// Check for shutdown before doing any real work
	select {
	case <-ctx.Done():
		log.Printf("[%s] crawler stopping (shutdown)", domain)
		setDomainStatus(domain, statusPaused)
		return
	default:
	}

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
			redirectHost := strings.TrimPrefix(strings.ToLower(req.URL.Hostname()), "www.")
			originHost := strings.TrimPrefix(strings.ToLower(via[0].URL.Hostname()), "www.")
			if redirectHost != "" && redirectHost != originHost && isValidDomain(redirectHost) {
				go recordExtLink(domain, redirectHost, intervalSec)
			}
			return nil
		},
	}

	for {
		// ── shutdown check ───────────────────────────────────────
		select {
		case <-ctx.Done():
			log.Printf("[%s] crawler stopping (shutdown)", domain)
			setDomainStatus(domain, statusPaused)
			return
		default:
		}

		// ── pause check ──────────────────────────────────────────
		checkPause(domain, br)

		// ── re-read interval ─────────────────────────────────────
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

		// random delay [interval, interval*2] – interruptible by pause/shutdown
		delaySec := intervalSec + rand.Intn(intervalSec+1)
		emit(br, "waiting", map[string]interface{}{
			"url":     target,
			"delay_s": delaySec,
			"queue":   queueLen(domain),
		})
		if interrupted := interruptibleSleep(ctx, domain, br, time.Duration(delaySec)*time.Second); interrupted {
			enqueueURL(domain, target) // put back so it's retried after resume/restart
			continue
		}

		emit(br, "fetching", map[string]string{"url": target})
		resp, err := httpClient.Get(target)
		if err != nil {
			emit(br, "error", map[string]string{"url": target, "msg": err.Error()})
			log.Printf("[%s] fetch error %s: %v", domain, target, err)
			enqueueURL(domain, target)
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
}
