package main

import (
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
	Current    int
	FinishTime float64
	ActiveConn int
	Mutex      sync.Mutex
}

type BackendGroup struct {
	Path     string
	Backends []*Backend
}

type LoadBalancer struct {
	Groups    map[string][]*Backend
	Algorithm string
	Quantum   int
	mu        sync.Mutex
}

func NewLoadBalancer(algorithm string, quantum int) *LoadBalancer {
	return &LoadBalancer{
		Algorithm: algorithm,
		Quantum:   quantum,
		Groups:    make(map[string][]*Backend),
	}
}

func (lb *LoadBalancer) AddBackend(path string, rawurl string, weight int) {
	u, err := url.Parse(rawurl)
	if err != nil {
		log.Fatal(err)
	}
	lb.Groups[path] = append(lb.Groups[path], &Backend{URL: u, Weight: weight})
}

func (lb *LoadBalancer) getNextBackendDRR(backends []*Backend) *Backend {
	for {
		for _, b := range backends {
			b.Mutex.Lock()
			b.Current += lb.Quantum
			if b.Current >= 1 {
				b.Current--
				b.Mutex.Unlock()
				return b
			}
			b.Mutex.Unlock()
		}
	}
}

func (lb *LoadBalancer) getNextBackendWFQ(backends []*Backend) *Backend {
	now := float64(time.Now().UnixNano()) / 1e9
	minFinish := math.MaxFloat64
	var selected *Backend

	for _, b := range backends {
		b.Mutex.Lock()
		arrival := now
		finish := math.Max(arrival, b.FinishTime) + 1.0/float64(b.Weight)
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

		var backend *Backend
		switch lb.Algorithm {
		case "DRR":
			backend = lb.getNextBackendDRR(backends)
		case "WFQ":
			backend = lb.getNextBackendWFQ(backends)
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
		proxyReq, err := http.NewRequest(r.Method, proxyURL.String(), r.Body)
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
  algorithm := "LC" // Change to "DRR", "WFQ", or "LC"

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
