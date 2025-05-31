package integration

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	baseAddress   = "http://balancer:8090"
	apiBasePath   = "/api/v1/some-data"
	teamName      = "faang"
	numIterations = 15
)

var client = http.Client{
	Timeout: 10 * time.Second,
}

type ApiDataResponse struct {
	Data string `json:"data"`
}

func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	t.Run("GetDataWithTeamKey", func(t *testing.T) {
		url := fmt.Sprintf("%s%s?key=%s", baseAddress, apiBasePath, teamName)
		log.Printf("Testing URL: %s", url)

		resp, err := client.Get(url)
		require.NoError(t, err, "Failed to make request for team key")
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode, "Expected status OK for team key")

		bodyBytes, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "Failed to read response body for team key")
		require.NotEmpty(t, bodyBytes, "Expected non-empty response body for team key")

		var apiResp ApiDataResponse
		err = json.Unmarshal(bodyBytes, &apiResp)
		require.NoError(t, err, "Failed to unmarshal JSON response for team key")

		assert.Regexp(t, `^\d{4}-\d{2}-\d{2}$`, apiResp.Data, "Data field should be a date string YYYY-MM-DD")
		log.Printf("Received data for team key '%s': %s", teamName, apiResp.Data)
	})

	t.Run("DistributionForSinglePath", func(t *testing.T) {
		servers := make(map[string]bool)
		url := fmt.Sprintf("%s%s?key=%s", baseAddress, apiBasePath, teamName)

		for i := 1; i <= numIterations; i++ {
			serverHeader := getServerForRequest(t, url)
			servers[serverHeader] = true
		}

		assert.Equal(t, 1, len(servers),
			"For a balancer hashing r.URL.Path, and a single path, expected 1 server. Got %d. Servers: %v",
			len(servers), servers)
		log.Printf("DistributionForSinglePath (key '%s'): %d unique server(s) hit. Servers: %v", teamName, len(servers), servers)
	})

	t.Run("ConsistencyForSinglePath", func(t *testing.T) {
		url := fmt.Sprintf("%s%s?key=%s", baseAddress, apiBasePath, teamName)
		firstServerHeader := getServerForRequest(t, url)

		for i := 0; i < numIterations; i++ {
			currentServerHeader := getServerForRequest(t, url)
			assert.Equal(t, firstServerHeader, currentServerHeader,
				"For a balancer hashing r.URL.Path, and a single path, expected the same server. Iteration %d", i)
		}
		log.Printf("ConsistencyForSinglePath (key '%s'): All requests hit server '%s'", teamName, firstServerHeader)
	})
}

func getServerForRequest(t *testing.T, url string) string {
	resp, err := client.Get(url)
	require.NoError(t, err, "Failed to make request to URL: %s", url)
	if resp.Body != nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	require.Equal(t, http.StatusOK, resp.StatusCode, "Expected status OK for URL: %s", url)

	server := resp.Header.Get("lb-from")
	require.NotEmpty(t, server, "Expected 'lb-from' header to be set for URL: %s", url)
	return server
}

// BenchmarkBalancer measures request performance to the load balancer under repeated access.
func BenchmarkBalancer(b *testing.B) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		b.Skip("Integration test is not enabled")
	}

	url := fmt.Sprintf("%s%s?key=%s", baseAddress, apiBasePath, teamName)
	respSetup, errSetup := client.Get(url)
	if errSetup != nil || respSetup.StatusCode != http.StatusOK {
		b.Fatalf("Failed to setup data for benchmark: %v, status: %d", errSetup, respSetup.StatusCode)
	}
	if respSetup.Body != nil {
		io.Copy(io.Discard, respSetup.Body)
		respSetup.Body.Close()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := client.Get(url)
		if err != nil {
			b.Error(err)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			b.Errorf("Expected status 200, got %d for URL %s", resp.StatusCode, url)
		}
		if resp.Body != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	}
}
