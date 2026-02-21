package main

import (
	"encoding/json"
	"sync"
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

// broadcast emits to ALL domain brokers.
func broadcast(event string, data interface{}) {
	brokersMu.RLock()
	defer brokersMu.RUnlock()
	b, _ := json.Marshal(ssePayload{Event: event, Data: data})
	msg := string(b)
	for _, br := range brokers {
		br.publish(msg)
	}
}
