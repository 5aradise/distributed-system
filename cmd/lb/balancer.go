package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/5aradise/distributed-system/httptools"
	"github.com/5aradise/distributed-system/signal"
)

var (
	errAllServersUnhealthy = errors.New("all servers are unhealthy")
)

var (
	port       = flag.Int("port", 8090, "load balancer port")
	timeoutSec = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https      = flag.Bool("https", false, "whether backends support HTTPs")

	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

type server struct {
	url     string
	healthy atomic.Bool
}

var (
	timeout     = time.Duration(*timeoutSec) * time.Second
	serversPool = []*server{
		{url: "server1:8080"},
		{url: "server2:8080"},
		{url: "server3:8080"},
	}
)

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

func health(dst string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", scheme(), dst), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	if resp.StatusCode != http.StatusOK {
		return false
	}
	return true
}

func forward(dst string, rw http.ResponseWriter, r *http.Request) error {
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = dst
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = dst

	resp, err := http.DefaultClient.Do(fwdRequest)
	if err == nil {
		for k, values := range resp.Header {
			for _, value := range values {
				rw.Header().Add(k, value)
			}
		}
		if *traceEnabled {
			rw.Header().Set("lb-from", dst)
		}
		log.Println("fwd", resp.StatusCode, resp.Request.URL)
		rw.WriteHeader(resp.StatusCode)
		defer resp.Body.Close()
		_, err := io.Copy(rw, resp.Body)
		if err != nil {
			log.Printf("Failed to write response: %s", err)
		}
		return nil
	} else {
		log.Printf("Failed to get response from %s: %s", dst, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
}

func chooseHealthy(i int) (string, error) {
	for _, srv := range serversPool[i:] {
		if srv.healthy.Load() {
			return srv.url, nil
		}
	}
	for _, srv := range serversPool[:i] {
		if srv.healthy.Load() {
			return srv.url, nil
		}
	}
	return "", errAllServersUnhealthy
}

func main() {
	flag.Parse()

	for _, srv := range serversPool {
		go func() {
			srv.healthy.Store(health(srv.url))
			for range time.Tick(10 * time.Second) {
				srv.healthy.Store(health(srv.url))
			}
		}()
	}

	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		serverIndex := int(hash(r.URL.Path)) % len(serversPool)
		if serverIndex < 0 {
			serverIndex = -serverIndex
		}
		dst, err := chooseHealthy(serverIndex)
		if err != nil {
			rw.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		forward(dst, rw, r)
	}))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	go frontend.Start()
	signal.WaitForTerminationSignal()
}
