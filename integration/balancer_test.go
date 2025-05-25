package integration

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	baseAddress   = "http://balancer:8090"
	apiPath       = "/api/v1/some-data/"
	numIterations = 15
)

var client = http.Client{
	Timeout: 3 * time.Second,
}

func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	t.Run("Distribution", checkDistribution)
	t.Run("Consistency", checkConsistency)
}

func checkDistribution(t *testing.T) {
	servers := make(map[string]bool)

	for i := 1; i <= numIterations; i++ {
		url := buildURL(i)
		server := getServerForRequest(t, url)
		servers[server] = true
	}

	assert.Greater(t, len(servers), 1, "Expected >1 server, got %d", len(servers))
}

func checkConsistency(t *testing.T) {
	url := buildURL(1)
	firstServer := getServerForRequest(t, url)

	for i := 0; i < numIterations; i++ {
		currentServer := getServerForRequest(t, url)
		assert.Equal(t, firstServer, currentServer, "Expected the same server, got different servers")
	}
}

func getServerForRequest(t *testing.T, url string) string {
	resp, err := client.Get(url)
	require.NoError(t, err, "Failed to make request")
	defer resp.Body.Close()

	server := resp.Header.Get("lb-from")
	require.NotEmpty(t, server, "Expected 'lb-from' header to be set")

	return server
}

func buildURL(id int) string {
	return fmt.Sprintf("%s%s%d", baseAddress, apiPath, id)
}

func BenchmarkBalancer(b *testing.B) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		b.Skip("Integration test is not enabled")
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		url := buildURL(i)
		resp, err := client.Get(url)
		require.NoError(b, err)
		resp.Body.Close()
	}
}
