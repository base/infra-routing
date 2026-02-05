package proxyd

import (
	"fmt"
	"math"
	"math/big"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
)

func createTestBackends(count int) []*Backend {
	backends := make([]*Backend, count)
	for i := 0; i < count; i++ {
		backends[i] = &Backend{Name: fmt.Sprintf("node%d", i)}
	}
	return backends
}

func TestConsistentHashDeterminism(t *testing.T) {
	backends := createTestBackends(3)
	router := NewConsistentHashRouter(backends, "test-salt", 3, 50)

	senderAddr := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc454e4438f44e")

	var primary string
	for i := 0; i < 100; i++ {
		candidates, err := router.RouteToBackends(senderAddr)
		require.NoError(t, err)
		require.Len(t, candidates, 3)

		if i == 0 {
			primary = candidates[0].Name
		} else {
			require.Equal(t, primary, candidates[0].Name, "Primary should be consistent across calls")
		}
	}
}

func TestConsistentHashCandidateOrdering(t *testing.T) {
	backends := createTestBackends(5)
	router := NewConsistentHashRouter(backends, "test-salt", 5, 50)

	senderAddr := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc454e4438f44e")

	candidates1, err := router.RouteToBackends(senderAddr)
	require.NoError(t, err)
	require.Len(t, candidates1, 5)

	candidates2, err := router.RouteToBackends(senderAddr)
	require.NoError(t, err)
	require.Len(t, candidates2, 5)

	for i := range candidates1 {
		require.Equal(t, candidates1[i].Name, candidates2[i].Name, "Candidate ordering should be stable")
	}
}

func TestConsistentHashSaltImpact(t *testing.T) {
	backends := createTestBackends(10)

	router1 := NewConsistentHashRouter(backends, "salt-alpha", 3, 50)
	router2 := NewConsistentHashRouter(backends, "salt-beta", 3, 50)

	differentPrimaryCount := 0
	for i := 0; i < 100; i++ {
		addr := common.BigToAddress(big.NewInt(int64(i)))

		candidates1, err := router1.RouteToBackends(addr)
		require.NoError(t, err)

		candidates2, err := router2.RouteToBackends(addr)
		require.NoError(t, err)

		if candidates1[0].Name != candidates2[0].Name {
			differentPrimaryCount++
		}
	}

	require.Greater(t, differentPrimaryCount, 50, "Different salts should produce different primaries for most addresses")
}

func TestConsistentHashDistribution(t *testing.T) {
	const (
		numBackends = 10
		numSenders  = 10000
	)

	backends := createTestBackends(numBackends)
	router := NewConsistentHashRouter(backends, "test-salt", 3, 50)

	distribution := make(map[string]int)
	for i := 0; i < numSenders; i++ {
		addr := common.BigToAddress(big.NewInt(int64(i)))
		candidates, err := router.RouteToBackends(addr)
		require.NoError(t, err)
		distribution[candidates[0].Name]++
	}

	require.Equal(t, numBackends, len(distribution), "All backends should receive some requests")

	expected := float64(numSenders) / float64(numBackends)
	for node, count := range distribution {
		deviation := math.Abs(float64(count)-expected) / expected
		require.Less(t, deviation, 0.50,
			"Node %s deviation %.2f%% exceeds 50%%", node, deviation*100)
	}
}

func TestConsistentHashConcurrency(t *testing.T) {
	backends := createTestBackends(5)
	router := NewConsistentHashRouter(backends, "test-salt", 3, 50)

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			addr := common.BigToAddress(big.NewInt(int64(idx)))
			_, err := router.RouteToBackends(addr)
			if err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Concurrent routing failed: %v", err)
	}
}

func TestConsistentHashBackendAddRemove(t *testing.T) {
	backends := createTestBackends(3)
	router := NewConsistentHashRouter(backends, "test-salt", 3, 50)

	require.Equal(t, 3, router.NumBackends())

	newBackend := &Backend{Name: "node3"}
	router.AddBackend(newBackend)
	require.Equal(t, 4, router.NumBackends())

	router.RemoveBackend("node0")
	require.Equal(t, 3, router.NumBackends())

	addr := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc454e4438f44e")
	candidates, err := router.RouteToBackends(addr)
	require.NoError(t, err)

	for _, c := range candidates {
		require.NotEqual(t, "node0", c.Name, "Removed backend should not be in candidates")
	}
}

func TestConsistentHashSaltUpdate(t *testing.T) {
	backends := createTestBackends(10)
	router := NewConsistentHashRouter(backends, "original-salt", 3, 50)

	addr := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc454e4438f44e")

	candidates1, err := router.RouteToBackends(addr)
	require.NoError(t, err)
	originalPrimary := candidates1[0].Name

	router.UpdateSalt("new-salt-12345")

	candidates2, err := router.RouteToBackends(addr)
	require.NoError(t, err)
	newPrimary := candidates2[0].Name

	require.NotEqual(t, originalPrimary, newPrimary, "Salt rotation should change routing")
}

func TestConsistentHashTimeout(t *testing.T) {
	backends := createTestBackends(3)
	router := NewConsistentHashRouter(backends, "test-salt", 3, 50)

	require.Equal(t, 50*1000*1000, int(router.GetCandidateTimeout().Nanoseconds()), "Timeout should be 50ms")
}

func TestConsistentHashNumCandidatesLessThanBackends(t *testing.T) {
	backends := createTestBackends(10)
	router := NewConsistentHashRouter(backends, "test-salt", 3, 50)

	addr := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc454e4438f44e")
	candidates, err := router.RouteToBackends(addr)
	require.NoError(t, err)
	require.Len(t, candidates, 3, "Should return exactly numCandidates backends")
}

func TestConsistentHashNumCandidatesMoreThanBackends(t *testing.T) {
	backends := createTestBackends(2)
	router := NewConsistentHashRouter(backends, "test-salt", 5, 50)

	addr := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc454e4438f44e")
	candidates, err := router.RouteToBackends(addr)
	require.NoError(t, err)
	require.Len(t, candidates, 2, "Should return all available backends when numCandidates > len(backends)")
}
