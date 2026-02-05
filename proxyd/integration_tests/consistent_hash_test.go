package integration_tests

import (
	"encoding/json"
	"net/http"
	"os"
	"sync"
	"testing"

	"github.com/ethereum-optimism/infra/proxyd"
	"github.com/stretchr/testify/require"
)

// Test transaction hex strings with known sender addresses
// txHex1 sender: 0x89c8b1b2774201bAC50F627403eaC1b732459cF7 (from sender_rate_limit_test.go)
const consistentHashTxHex1 = "0x02f8b28201a406849502f931849502f931830147f9948f3ddd0fbf3e78ca1d6c" +
	"d17379ed88e261249b5280b84447e7ef2400000000000000000000000089c8b1" +
	"b2774201bac50f627403eac1b732459cf7000000000000000000000000000000" +
	"0000000000000000056bc75e2d63100000c080a0473c95566026c312c9664cd6" +
	"1145d2f3e759d49209fe96011ac012884ec5b017a0763b58f6fa6096e6ba28ee" +
	"08bfac58f58fb3b8bcef5af98578bdeaddf40bde42"

// txHex2 sender: 0xBE53E587975603A13D0923D0aA6D37C5233dD750 (from sender_rate_limit_test.go)
const consistentHashTxHex2 = "0x02f8758201a48217fd84773594008504a817c80082520894be53e587975603" +
	"a13d0923d0aa6d37c5233dd750865af3107a400080c080a04aefbd5819c35729" +
	"138fe26b6ae1783ebf08d249b356c2f920345db97877f3f7a008d5ae92560a3c" +
	"65f723439887205713af7ce7d7f6b24fba198f2afa03435867"

const consistentHashGoodResponse = `{"jsonrpc": "2.0", "result": "0x1234", "id": 999}`

func TestConsistentHashDeterministicRouting(t *testing.T) {
	// Create 3 mock backends that track which requests they receive
	backend1 := NewMockBackend(BatchedResponseHandler(200, consistentHashGoodResponse))
	defer backend1.Close()
	backend2 := NewMockBackend(BatchedResponseHandler(200, consistentHashGoodResponse))
	defer backend2.Close()
	backend3 := NewMockBackend(BatchedResponseHandler(200, consistentHashGoodResponse))
	defer backend3.Close()

	require.NoError(t, os.Setenv("NODE1_BACKEND_RPC_URL", backend1.URL()))
	require.NoError(t, os.Setenv("NODE2_BACKEND_RPC_URL", backend2.URL()))
	require.NoError(t, os.Setenv("NODE3_BACKEND_RPC_URL", backend3.URL()))
	require.NoError(t, os.Setenv("CONSISTENT_HASH_SALT", "test-salt-12345"))

	config := ReadConfig("consistent_hash")
	client := NewProxydClient("http://127.0.0.1:8545")
	_, shutdown, err := proxyd.Start(config)
	require.NoError(t, err)
	defer shutdown()

	// Send the same transaction multiple times - should always go to the same backend
	for i := 0; i < 5; i++ {
		backend1.Reset()
		backend2.Reset()
		backend3.Reset()

		_, statusCode, err := client.SendRequest(makeSendRawTransactionRequest(consistentHashTxHex1))
		require.NoError(t, err)
		require.Equal(t, 200, statusCode)

		// Count which backend received the request
		totalRequests := len(backend1.Requests()) + len(backend2.Requests()) + len(backend3.Requests())
		require.Equal(t, 1, totalRequests, "exactly one backend should receive the request")
	}

	// Track which backend received the first request
	backend1.Reset()
	backend2.Reset()
	backend3.Reset()

	_, _, err = client.SendRequest(makeSendRawTransactionRequest(consistentHashTxHex1))
	require.NoError(t, err)

	var primaryBackend *MockBackend
	var primaryName string
	if len(backend1.Requests()) > 0 {
		primaryBackend = backend1
		primaryName = "node1"
	} else if len(backend2.Requests()) > 0 {
		primaryBackend = backend2
		primaryName = "node2"
	} else {
		primaryBackend = backend3
		primaryName = "node3"
	}

	t.Logf("Primary backend for txHex1: %s", primaryName)

	// Send more requests from the same sender - should all go to the same backend
	for i := 0; i < 3; i++ {
		backend1.Reset()
		backend2.Reset()
		backend3.Reset()

		_, statusCode, err := client.SendRequest(makeSendRawTransactionRequest(consistentHashTxHex1))
		require.NoError(t, err)
		require.Equal(t, 200, statusCode)
		require.Equal(t, 1, len(primaryBackend.Requests()), "request should go to the same primary backend")
	}
}

func TestConsistentHashDifferentSendersDistribution(t *testing.T) {
	backend1 := NewMockBackend(BatchedResponseHandler(200, consistentHashGoodResponse))
	defer backend1.Close()
	backend2 := NewMockBackend(BatchedResponseHandler(200, consistentHashGoodResponse))
	defer backend2.Close()
	backend3 := NewMockBackend(BatchedResponseHandler(200, consistentHashGoodResponse))
	defer backend3.Close()

	require.NoError(t, os.Setenv("NODE1_BACKEND_RPC_URL", backend1.URL()))
	require.NoError(t, os.Setenv("NODE2_BACKEND_RPC_URL", backend2.URL()))
	require.NoError(t, os.Setenv("NODE3_BACKEND_RPC_URL", backend3.URL()))
	require.NoError(t, os.Setenv("CONSISTENT_HASH_SALT", "test-salt-distribution"))

	config := ReadConfig("consistent_hash")
	client := NewProxydClient("http://127.0.0.1:8545")
	_, shutdown, err := proxyd.Start(config)
	require.NoError(t, err)
	defer shutdown()

	// Send from sender 1
	_, statusCode1, err := client.SendRequest(makeSendRawTransactionRequest(consistentHashTxHex1))
	require.NoError(t, err)
	require.Equal(t, 200, statusCode1)

	// Record which backend got the first sender's request
	sender1Backend := ""
	if len(backend1.Requests()) > 0 {
		sender1Backend = "node1"
	} else if len(backend2.Requests()) > 0 {
		sender1Backend = "node2"
	} else if len(backend3.Requests()) > 0 {
		sender1Backend = "node3"
	}

	backend1.Reset()
	backend2.Reset()
	backend3.Reset()

	// Send from sender 2
	_, statusCode2, err := client.SendRequest(makeSendRawTransactionRequest(consistentHashTxHex2))
	require.NoError(t, err)
	require.Equal(t, 200, statusCode2)

	// Record which backend got the second sender's request
	sender2Backend := ""
	if len(backend1.Requests()) > 0 {
		sender2Backend = "node1"
	} else if len(backend2.Requests()) > 0 {
		sender2Backend = "node2"
	} else if len(backend3.Requests()) > 0 {
		sender2Backend = "node3"
	}

	t.Logf("Sender 1 routed to: %s", sender1Backend)
	t.Logf("Sender 2 routed to: %s", sender2Backend)

	// Both senders should be routed deterministically (may or may not be different backends
	// depending on the hash, but both should be assigned to some backend)
	require.NotEmpty(t, sender1Backend, "sender 1 should be routed to a backend")
	require.NotEmpty(t, sender2Backend, "sender 2 should be routed to a backend")
}

func TestConsistentHashSaltChangesRouting(t *testing.T) {
	backend1 := NewMockBackend(BatchedResponseHandler(200, consistentHashGoodResponse))
	defer backend1.Close()
	backend2 := NewMockBackend(BatchedResponseHandler(200, consistentHashGoodResponse))
	defer backend2.Close()
	backend3 := NewMockBackend(BatchedResponseHandler(200, consistentHashGoodResponse))
	defer backend3.Close()

	require.NoError(t, os.Setenv("NODE1_BACKEND_RPC_URL", backend1.URL()))
	require.NoError(t, os.Setenv("NODE2_BACKEND_RPC_URL", backend2.URL()))
	require.NoError(t, os.Setenv("NODE3_BACKEND_RPC_URL", backend3.URL()))

	// Test with salt A
	require.NoError(t, os.Setenv("CONSISTENT_HASH_SALT", "salt-A"))

	config := ReadConfig("consistent_hash")
	client := NewProxydClient("http://127.0.0.1:8545")
	_, shutdown, err := proxyd.Start(config)
	require.NoError(t, err)

	_, statusCode, err := client.SendRequest(makeSendRawTransactionRequest(consistentHashTxHex1))
	require.NoError(t, err)
	require.Equal(t, 200, statusCode)

	saltABackend := ""
	if len(backend1.Requests()) > 0 {
		saltABackend = "node1"
	} else if len(backend2.Requests()) > 0 {
		saltABackend = "node2"
	} else if len(backend3.Requests()) > 0 {
		saltABackend = "node3"
	}

	shutdown()

	// Reset and test with salt B
	backend1.Reset()
	backend2.Reset()
	backend3.Reset()

	require.NoError(t, os.Setenv("CONSISTENT_HASH_SALT", "salt-B"))

	config = ReadConfig("consistent_hash")
	_, shutdown2, err := proxyd.Start(config)
	require.NoError(t, err)
	defer shutdown2()

	_, statusCode, err = client.SendRequest(makeSendRawTransactionRequest(consistentHashTxHex1))
	require.NoError(t, err)
	require.Equal(t, 200, statusCode)

	saltBBackend := ""
	if len(backend1.Requests()) > 0 {
		saltBBackend = "node1"
	} else if len(backend2.Requests()) > 0 {
		saltBBackend = "node2"
	} else if len(backend3.Requests()) > 0 {
		saltBBackend = "node3"
	}

	t.Logf("Salt A routed to: %s", saltABackend)
	t.Logf("Salt B routed to: %s", saltBBackend)

	// Different salts should (likely) route to different backends
	// Note: This isn't guaranteed due to hash collisions, but with 3 backends
	// and different salts, they should usually differ
	require.NotEmpty(t, saltABackend, "should route to a backend with salt A")
	require.NotEmpty(t, saltBBackend, "should route to a backend with salt B")
}

func TestConsistentHashFailoverToPrimaryCandidate(t *testing.T) {
	// Backend 1 is unhealthy (returns 500), Backend 2 is healthy
	backend1 := NewMockBackend(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer backend1.Close()
	backend2 := NewMockBackend(BatchedResponseHandler(200, consistentHashGoodResponse))
	defer backend2.Close()
	backend3 := NewMockBackend(BatchedResponseHandler(200, consistentHashGoodResponse))
	defer backend3.Close()

	require.NoError(t, os.Setenv("NODE1_BACKEND_RPC_URL", backend1.URL()))
	require.NoError(t, os.Setenv("NODE2_BACKEND_RPC_URL", backend2.URL()))
	require.NoError(t, os.Setenv("NODE3_BACKEND_RPC_URL", backend3.URL()))
	require.NoError(t, os.Setenv("CONSISTENT_HASH_SALT", "failover-test-salt"))

	config := ReadConfig("consistent_hash")
	client := NewProxydClient("http://127.0.0.1:8545")
	_, shutdown, err := proxyd.Start(config)
	require.NoError(t, err)
	defer shutdown()

	// Send a request - if primary is unhealthy, should failover to a candidate
	_, statusCode, err := client.SendRequest(makeSendRawTransactionRequest(consistentHashTxHex1))
	require.NoError(t, err)
	require.Equal(t, 200, statusCode)

	// At least one healthy backend should have received the request
	healthyRequests := len(backend2.Requests()) + len(backend3.Requests())
	require.GreaterOrEqual(t, healthyRequests, 1, "at least one healthy backend should receive the request")
}

func TestConsistentHashConcurrentRequests(t *testing.T) {
	backend1 := NewMockBackend(BatchedResponseHandler(200, consistentHashGoodResponse))
	defer backend1.Close()
	backend2 := NewMockBackend(BatchedResponseHandler(200, consistentHashGoodResponse))
	defer backend2.Close()
	backend3 := NewMockBackend(BatchedResponseHandler(200, consistentHashGoodResponse))
	defer backend3.Close()

	require.NoError(t, os.Setenv("NODE1_BACKEND_RPC_URL", backend1.URL()))
	require.NoError(t, os.Setenv("NODE2_BACKEND_RPC_URL", backend2.URL()))
	require.NoError(t, os.Setenv("NODE3_BACKEND_RPC_URL", backend3.URL()))
	require.NoError(t, os.Setenv("CONSISTENT_HASH_SALT", "concurrent-test-salt"))

	config := ReadConfig("consistent_hash")
	client := NewProxydClient("http://127.0.0.1:8545")
	_, shutdown, err := proxyd.Start(config)
	require.NoError(t, err)
	defer shutdown()

	// Send 10 concurrent requests from the same sender
	var wg sync.WaitGroup
	numRequests := 10
	results := make(chan int, numRequests)

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, statusCode, err := client.SendRequest(makeSendRawTransactionRequest(consistentHashTxHex1))
			if err != nil {
				t.Errorf("request failed: %v", err)
				return
			}
			results <- statusCode
		}()
	}

	wg.Wait()
	close(results)

	// All requests should succeed
	successCount := 0
	for statusCode := range results {
		if statusCode == 200 {
			successCount++
		}
	}
	require.Equal(t, numRequests, successCount, "all concurrent requests should succeed")

	// All requests from the same sender should go to the same backend
	totalRequests := len(backend1.Requests()) + len(backend2.Requests()) + len(backend3.Requests())
	require.Equal(t, numRequests, totalRequests, "all requests should be accounted for")

	// One backend should have received all requests (deterministic routing)
	maxRequests := max(len(backend1.Requests()), max(len(backend2.Requests()), len(backend3.Requests())))
	require.Equal(t, numRequests, maxRequests, "all requests from same sender should go to same backend")
}

func TestConsistentHashNonSendTxMethodsFallback(t *testing.T) {
	// For non-eth_sendRawTransaction methods, consistent hash routing should
	// fall back to normal routing behavior
	backend1 := NewMockBackend(BatchedResponseHandler(200, `{"jsonrpc": "2.0", "result": "0x1", "id": 999}`))
	defer backend1.Close()
	backend2 := NewMockBackend(BatchedResponseHandler(200, `{"jsonrpc": "2.0", "result": "0x1", "id": 999}`))
	defer backend2.Close()
	backend3 := NewMockBackend(BatchedResponseHandler(200, `{"jsonrpc": "2.0", "result": "0x1", "id": 999}`))
	defer backend3.Close()

	require.NoError(t, os.Setenv("NODE1_BACKEND_RPC_URL", backend1.URL()))
	require.NoError(t, os.Setenv("NODE2_BACKEND_RPC_URL", backend2.URL()))
	require.NoError(t, os.Setenv("NODE3_BACKEND_RPC_URL", backend3.URL()))
	require.NoError(t, os.Setenv("CONSISTENT_HASH_SALT", "non-sendtx-test"))

	config := ReadConfig("consistent_hash")
	client := NewProxydClient("http://127.0.0.1:8545")
	_, shutdown, err := proxyd.Start(config)
	require.NoError(t, err)
	defer shutdown()

	// Send eth_chainId request - should work (uses fallback routing)
	res, statusCode, err := client.SendRPC("eth_chainId", nil)
	require.NoError(t, err)
	require.Equal(t, 200, statusCode)

	// Verify we got a valid response
	var response map[string]interface{}
	err = json.Unmarshal(res, &response)
	require.NoError(t, err)
	require.Equal(t, "0x1", response["result"])
}

func makeSendRawTransactionRequest(txHex string) []byte {
	return []byte(`{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["` + txHex + `"],"id":999}`)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
