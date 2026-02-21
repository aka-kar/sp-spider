package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func init() {
	// Ensure the global "__all__" SSE broker always exists.
	getBroker("__all__")
}

func main() {
	rand.Seed(time.Now().UnixNano()) //nolint:staticcheck

	loadEnv(".env")

	shutdownCtx, shutdownCancel = context.WithCancel(context.Background())

	// Semaphore: cap concurrent crawlers via MAX_CRAWLERS env (default 10)
	maxCrawlers := 10
	if v := os.Getenv("MAX_CRAWLERS"); v != "" {
		fmt.Sscanf(v, "%d", &maxCrawlers)
		if maxCrawlers < 1 {
			maxCrawlers = 1
		}
	}
	crawlerSem = make(chan struct{}, maxCrawlers)
	log.Printf("max concurrent crawlers: %d", maxCrawlers)

	if err := os.MkdirAll(appDataDir, 0o755); err != nil {
		log.Fatalf("mkdir %s: %v", appDataDir, err)
	}
	if err := os.MkdirAll("static", 0o755); err != nil {
		log.Fatalf("mkdir static: %v", err)
	}

	initSkipDB()
	initMainDB()
	initMariaDB()

	// Dispatcher: periodically picks up pending domains and starts crawlers
	// when semaphore slots become free (handles overflow from auto-discovery).
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-shutdownCtx.Done():
				return
			case <-ticker.C:
				rows, err := mainDB.Query(
					`SELECT domain, interval FROM domains WHERE status=? ORDER BY id ASC`,
					statusPending)
				if err != nil {
					continue
				}
				for rows.Next() {
					var d string
					var iv int
					if rows.Scan(&d, &iv) != nil {
						continue
					}
					tryStartCrawler(d, iv)
				}
				rows.Close()
			}
		}
	}()

	// Resume domains from previous run
	rows, err := mainDB.Query(`SELECT domain, interval, status FROM domains`)
	if err == nil {
		for rows.Next() {
			var d, status string
			var iv int
			if rows.Scan(&d, &iv, &status) != nil {
				continue
			}
			if status == statusDone {
				continue
			}
			if status == statusPaused {
				ensurePauseChannels(d)
				pauseCrawler(d)
				tryStartCrawler(d, iv)
			} else {
				setDomainStatus(d, statusPending)
				tryStartCrawler(d, iv)
			}
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
