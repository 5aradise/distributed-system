package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"time"
)

var target = flag.String("target", "http://localhost:8090", "request target")

func main() {
	flag.Parse()
	client := new(http.Client)
	client.Timeout = 10 * time.Second

	for range time.Tick(time.Second / 10) {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data/%s", *target, generateString()))
		if err == nil {
			data, err := io.ReadAll(resp.Body)
			if err == nil {
				log.Printf("response %d: %s", resp.StatusCode, string(data))
			} else {
				log.Printf("response %d: read body error (%s)", resp.StatusCode, err)
			}
		} else {
			log.Printf("error %s", err)
		}
	}
}

const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789$-_.+!*'()"

func generateString() string {
	generated := make([]byte, 0, 8)
	for range 8 {
		generated = append(generated, chars[rand.Intn(len(chars))])
	}
	return string(generated)
}
