package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type Backend struct {
	URL        *url.URL
	Weight     int
	Deficit    float64   // for DRR
	FinishTime float64   // for WFQ
	ActiveConn int       // for LC
	Mutex      sync.Mutex
}

type LoadBalancer struct {
	Groups    map[string][]*Backend
	Algorithm string
	Quantum   float64
	mu        sync.Mutex
	lastUsed  map[string]int // for DRR rotation
}

func NewLoadBalancer(algorithm string, quantum float64) *LoadBalancer {
	return &LoadBalancer{
		Algorithm: algorithm,
		Quantum:   quantum,
		Groups:    make(map[string][]*Backend),
		lastUsed:  make(map[string]int),
	}
}

func (lb *LoadBalancer) AddBackend(path string, rawurl string, weight int) {
	u, err := url.Parse(rawurl)
	if err != nil {
		log.Fatal(err)
	}
	lb.Groups[path] = append(lb.Groups[path], &Backend{URL: u, Weight: weight})
}

func estimateRequestCost(r *http.Request) float64 {
	cost := float64(len(r.URL.Path))
	if r.ContentLength > 0 {
		cost += float64(r.ContentLength)
	}
	return math.Max(cost/100.0, 1.0) // Normalize cost to a base scale
}

func (lb *LoadBalancer) getNextBackendDRR(backends []*Backend, path string, cost float64) *Backend {
	n := len(backends)
	start := lb.lastUsed[path]

	for i := 0; i < n; i++ {
		index := (start + i) % n
		b := backends[index]
		b.Mutex.Lock()
		b.Deficit += lb.Quantum
		if b.Deficit >= cost {
			b.Deficit -= cost
			lb.lastUsed[path] = (index + 1) % n
			b.Mutex.Unlock()
			return b
		}
		b.Mutex.Unlock()
	}

	return backends[start]
}

func (lb *LoadBalancer) getNextBackendWFQ(backends []*Backend, cost float64) *Backend {
	now := float64(time.Now().UnixNano()) / 1e9
	minFinish := math.MaxFloat64
	var selected *Backend

	for _, b := range backends {
		b.Mutex.Lock()
		arrival := now
		finish := math.Max(arrival, b.FinishTime) + cost/float64(b.Weight)
		if finish < minFinish {
			selected = b
			minFinish = finish
		}
		b.Mutex.Unlock()
	}

	if selected != nil {
		selected.Mutex.Lock()
		selected.FinishTime = minFinish
		selected.Mutex.Unlock()
	}
	return selected
}

func (lb *LoadBalancer) getNextBackendLC(backends []*Backend) *Backend {
	min := math.MaxInt32
	var selected *Backend
	for _, b := range backends {
		b.Mutex.Lock()
		if b.ActiveConn < min {
			min = b.ActiveConn
			selected = b
		}
		b.Mutex.Unlock()
	}
	selected.Mutex.Lock()
	selected.ActiveConn++
	selected.Mutex.Unlock()
	return selected
}

func (lb *LoadBalancer) forward(path string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		backends, ok := lb.Groups[path]
		if !ok {
			http.Error(w, "Unknown path", http.StatusNotFound)
			return
		}

		cost := estimateRequestCost(r)
		var backend *Backend
		switch lb.Algorithm {
		case "DRR":
			backend = lb.getNextBackendDRR(backends, path, cost)
		case "WFQ":
			backend = lb.getNextBackendWFQ(backends, cost)
		case "LC":
			backend = lb.getNextBackendLC(backends)
		default:
			http.Error(w, "Unknown algorithm", http.StatusBadRequest)
			return
		}

		proxyURL := *backend.URL
		trimmedPath := strings.TrimPrefix(r.URL.Path, path)
		if !strings.HasPrefix(trimmedPath, "/") {
			trimmedPath = "/" + trimmedPath
		}
		proxyURL.Path = trimmedPath

		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		r = r.WithContext(ctx)
		defer cancel()

		proxyReq, err := http.NewRequestWithContext(ctx, r.Method, proxyURL.String(), r.Body)
		if err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}

		proxyReq.Header = make(http.Header)
		for k, v := range r.Header {
			proxyReq.Header[k] = v
		}

		client := &http.Client{}
		resp, err := client.Do(proxyReq)
		if err != nil {
			http.Error(w, "Bad gateway", http.StatusBadGateway)
			if lb.Algorithm == "LC" {
				backend.Mutex.Lock()
				backend.ActiveConn--
				backend.Mutex.Unlock()
			}
			return
		}
		defer resp.Body.Close()

		for k, v := range resp.Header {
			w.Header()[k] = v
		}
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)

		if lb.Algorithm == "LC" {
			backend.Mutex.Lock()
			backend.ActiveConn--
			backend.Mutex.Unlock()
		}
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
  port := "9010"
  algorithm := "WFQ" // Change to "DRR", "WFQ", or "LC"

	lb := NewLoadBalancer(algorithm, 1)

  lb.AddBackend("/api", "https://singidunum.ac.rs", 1)
	lb.AddBackend("/test", "https://mage.singidunum.ac.rs", 1)

	http.HandleFunc("/api", lb.forward("/api"))
	http.HandleFunc("/api/", lb.forward("/api"))
	http.HandleFunc("/test", lb.forward("/test"))
	http.HandleFunc("/test/", lb.forward("/test"))

  fmt.Printf("Load balancer listening on 0.0.0.0:%s\n", port)
	http.ListenAndServe("0.0.0.0:"+port, nil)
}
